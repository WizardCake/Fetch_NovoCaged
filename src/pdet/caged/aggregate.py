"""Aggregation logic for Novo CAGED microdata."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from pdet.aggregation import (
    aggregate_chunk,
    apply_row_filters,
    build_aggregation_map,
    combine_aggregates,
    finalize_measures,
)
from pdet.caged.columns import (
    ADMISSION_MOVEMENT_CODES,
    ALL_COLUMNS,
    BUILTIN_MEASURES,
    DECIMAL_COLUMNS,
    DISMISSAL_MOVEMENT_CODES,
    INTEGER_COLUMNS,
    NUMERIC_COLUMNS,
    STRING_COLUMNS,
)
from pdet.caged.convert import coerce_normalized_types, normalize_chunk
from pdet.common import remove_tree

logger = logging.getLogger(__name__)

INPUT_FORMATS = ("auto", "parquet", "csv", "txt")


def add_metric_columns(
    frame: pd.DataFrame,
    measures: list[tuple[str, str | None, str]],
) -> pd.DataFrame:
    """Add internal ``_``-prefixed metric columns required by the requested measures."""
    requested = {measure[0] for measure in measures}
    needs_correction_sign = {"saldo", "estoque", "admitidos", "demitidos"}.intersection(requested)

    if needs_correction_sign:
        # EXC records cancel a previously reported movement, so their sign is inverted.
        is_exclusion = (frame["source_kind"].astype("string") == "EXC") | (
            frame["indicador_de_exclusao"].astype("string") == "1"
        )
        correction_sign = pd.Series(1, index=frame.index, dtype="int64")
        correction_sign.loc[is_exclusion.fillna(False)] = -1
    else:
        correction_sign = pd.Series(1, index=frame.index, dtype="int64")

    if "saldo" in requested or "estoque" in requested:
        frame["_saldo"] = pd.to_numeric(frame["saldo_movimentacao"], errors="coerce").fillna(0) * correction_sign

    if {"admitidos", "demitidos"}.intersection(requested):
        movement_type = frame["tipomovimentacao"].astype("string")
        if "admitidos" in requested:
            frame["_admitidos"] = movement_type.isin(ADMISSION_MOVEMENT_CODES).astype("int64") * correction_sign
        if "demitidos" in requested:
            frame["_demitidos"] = movement_type.isin(DISMISSAL_MOVEMENT_CODES).astype("int64") * correction_sign

    if "movimentacoes" in requested:
        frame["_movimentacoes"] = 1

    for operation, column, output_name in measures:
        if operation in BUILTIN_MEASURES:
            continue
        values = pd.to_numeric(frame[column], errors="coerce")
        if operation == "mean":
            frame[f"_{output_name}_sum"] = values.fillna(0)
            frame[f"_{output_name}_count"] = values.notna().astype("int64")
        else:
            frame[f"_{output_name}"] = values
    return frame


def _build_caged_aggregation_map(measures: list[tuple[str, str | None, str]]) -> dict[str, str]:
    """Build aggregation map that understands CAGED-specific builtins (saldo, admitidos, etc.)."""
    aggregations: dict[str, str] = {}
    for operation, _column, output_name in measures:
        if operation == "estoque":
            aggregations["_saldo"] = "sum"
        elif operation in ("saldo", "admitidos", "demitidos", "movimentacoes"):
            aggregations[f"_{operation}"] = "sum"
        elif operation == "mean":
            aggregations[f"_{output_name}_sum"] = "sum"
            aggregations[f"_{output_name}_count"] = "sum"
        elif operation in ("sum", "min", "max"):
            aggregations[f"_{output_name}"] = operation
    return aggregations


def add_stock_measure(
    frame: pd.DataFrame,
    dimensions: list[str],
    initial_stock_csv: Path | None,
    initial_stock_column: str,
) -> pd.DataFrame:
    """Compute cumulative stock from corrected *saldo* values, optionally using an external baseline."""
    if "competencia_mov" not in dimensions:
        raise ValueError("The estoque measure requires competencia_mov in --dimensions.")

    stock_dimensions = [dimension for dimension in dimensions if dimension != "competencia_mov"]
    frame = frame.sort_values(stock_dimensions + ["competencia_mov"] if stock_dimensions else ["competencia_mov"])

    if stock_dimensions:
        frame["estoque"] = frame.groupby(stock_dimensions, dropna=False)["_saldo"].cumsum()
    else:
        frame["estoque"] = frame["_saldo"].cumsum()

    if initial_stock_csv:
        initial = pd.read_csv(initial_stock_csv, dtype="string")
        missing = [column for column in stock_dimensions + [initial_stock_column] if column not in initial.columns]
        if missing:
            raise ValueError(f"Initial stock CSV is missing columns: {', '.join(missing)}")
        initial[initial_stock_column] = pd.to_numeric(initial[initial_stock_column], errors="coerce").fillna(0)
        if stock_dimensions:
            frame = frame.merge(initial[stock_dimensions + [initial_stock_column]], on=stock_dimensions, how="left")
            frame[initial_stock_column] = frame[initial_stock_column].fillna(0)
            frame["estoque"] = frame["estoque"] + frame[initial_stock_column]
            frame = frame.drop(columns=[initial_stock_column])
        else:
            baseline = initial[initial_stock_column].sum()
            frame["estoque"] = frame["estoque"] + baseline
    else:
        logger.warning(
            "estoque is cumulative saldo from the selected data because no initial stock CSV was provided."
        )

    return frame


def finalize_caged_measures(
    frame: pd.DataFrame,
    dimensions: list[str],
    measures: list[tuple[str, str | None, str]],
    initial_stock_csv: Path | None,
    initial_stock_column: str,
) -> pd.DataFrame:
    """Finalize CAGED output, computing estoque if requested and renaming internal columns."""
    if any(operation == "estoque" for operation, _column, _output_name in measures):
        frame = add_stock_measure(frame, dimensions, initial_stock_csv, initial_stock_column)

    requested_output: list[str] = []
    for operation, _column, output_name in measures:
        if operation == "saldo":
            frame["saldo"] = frame["_saldo"]
        elif operation == "admitidos":
            frame["admitidos"] = frame["_admitidos"]
        elif operation == "demitidos":
            frame["demitidos"] = frame["_demitidos"]
        elif operation == "movimentacoes":
            frame["movimentacoes"] = frame["_movimentacoes"]
        elif operation == "mean":
            denominator = frame[f"_{output_name}_count"].replace(0, pd.NA)
            frame[output_name] = frame[f"_{output_name}_sum"] / denominator
        elif operation in ("sum", "min", "max"):
            frame[output_name] = frame[f"_{output_name}"]
        requested_output.append(output_name)

    output = frame[dimensions + requested_output].copy()
    return output.sort_values(dimensions) if dimensions else output


def discover_input_files(data_dir: Path, input_format: str) -> tuple[str, list[Path]]:
    """Auto-discover converted or extracted CAGED input files."""
    candidates = {
        "parquet": sorted((data_dir / "parquet").glob("**/*.parquet")),
        "csv": sorted((data_dir / "csv").glob("**/*.csv.gz")),
        "txt": sorted((data_dir / "extracted").glob("**/CAGED*.txt")),
    }
    if input_format == "auto":
        for candidate_format in ("parquet", "csv", "txt"):
            if candidates[candidate_format]:
                return candidate_format, candidates[candidate_format]
        raise FileNotFoundError("No converted or extracted Novo CAGED files found under data/")
    if not candidates[input_format]:
        raise FileNotFoundError(f"No {input_format} files found under {data_dir}")
    return input_format, candidates[input_format]


def iter_normalized_frames(data_dir: Path, input_format: str, chunksize: int):
    """Yield normalized DataFrames from any supported input format."""
    resolved_format, files = discover_input_files(data_dir, input_format)
    logger.info("Reading %d %s file(s)", len(files), resolved_format)

    for path in files:
        if resolved_format == "parquet":
            yield pd.read_parquet(path)
        elif resolved_format == "csv":
            reader = pd.read_csv(path, dtype="string", chunksize=chunksize, low_memory=False)
            for chunk in reader:
                yield chunk
        else:
            match = re.fullmatch(r"CAGED(MOV|FOR|EXC)(\d{6})\.txt", path.name, flags=re.I)
            if not match:
                continue
            source_kind, source_competencia = match.group(1).upper(), match.group(2)
            reader = pd.read_csv(
                path,
                sep=";",
                encoding="utf-8",
                dtype="string",
                chunksize=chunksize,
                low_memory=False,
            )
            for chunk in reader:
                yield normalize_chunk(chunk, source_competencia, source_kind)


def aggregate_data(
    data_dir: Path,
    input_format: str,
    dimensions: list[str],
    measures: list[tuple[str, str | None, str]],
    filters: dict[str, set[str]],
    start_mov: str | None,
    end_mov: str | None,
    chunksize: int,
    initial_stock_csv: Path | None,
    initial_stock_column: str,
) -> pd.DataFrame:
    """End-to-end aggregation pipeline for Novo CAGED."""
    aggregates: list[pd.DataFrame] = []
    agg_map = _build_caged_aggregation_map(measures)

    for frame in iter_normalized_frames(data_dir, input_format, chunksize):
        frame = coerce_normalized_types(frame)
        frame = apply_row_filters(frame, filters, start_mov, end_mov, "competencia_mov")
        if frame.empty:
            continue
        frame = add_metric_columns(frame, measures)
        aggregates.append(aggregate_chunk(frame, dimensions, measures, agg_map))
        if len(aggregates) > 20:
            aggregates = [combine_aggregates(aggregates, dimensions, measures, agg_map)]

    combined = combine_aggregates(aggregates, dimensions, measures, agg_map)
    return finalize_caged_measures(combined, dimensions, measures, initial_stock_csv, initial_stock_column)
