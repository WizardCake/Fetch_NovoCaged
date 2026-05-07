"""Aggregation logic for RAIS annual microdata."""

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
from pdet.rais.columns import BUILTIN_MEASURES, CODE_COLUMNS, DECIMAL_COLUMNS, INTEGER_COLUMNS, STANDARD_COLUMNS
from pdet.rais.convert import coerce_known_types, infer_source_from_path, normalize_chunk
from pdet.rais.extract import iter_microdata_text_files
from pdet.common import detect_delimiter

logger = logging.getLogger(__name__)

INPUT_FORMATS = ("auto", "parquet", "csv", "txt")
OUTPUT_FORMATS = ("parquet", "csv")


def active_3112_mask(frame: pd.DataFrame) -> pd.Series:
    """Return a boolean mask for rows that should count in the annual RAIS stock."""
    if "vinculo_ativo_31_12" in frame.columns:
        values = frame["vinculo_ativo_31_12"].astype("string").str.strip().str.upper()
        return values.isin({"1", "S", "SIM", "TRUE", "T"})
    if "mes_desligamento" in frame.columns:
        values = frame["mes_desligamento"].astype("string").str.strip()
        return values.isna() | values.isin({"", "0", "00", "99"})
    return frame.index.to_series().notna()


def month_present_mask(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return frame.index.to_series().notna() & False
    values = frame[column].astype("string").str.strip()
    return values.notna() & ~values.isin({"", "0", "00", "99"})


def add_metric_columns(frame: pd.DataFrame, measures: list[tuple[str, str | None, str]]) -> pd.DataFrame:
    requested = {measure[0] for measure in measures}
    if "vinculos" in requested:
        frame["_vinculos"] = 1
    if "estoque_3112" in requested:
        frame["_estoque_3112"] = active_3112_mask(frame).astype("int64")
    if "admitidos_ano" in requested:
        frame["_admitidos_ano"] = month_present_mask(frame, "mes_admissao").astype("int64")
    if "desligados_ano" in requested:
        frame["_desligados_ano"] = month_present_mask(frame, "mes_desligamento").astype("int64")

    for operation, column, output_name in measures:
        if operation in BUILTIN_MEASURES:
            continue
        if column not in frame.columns:
            raise ValueError(f"Measure column not found after RAIS normalization: {column}")
        values = pd.to_numeric(frame[column], errors="coerce")
        if operation == "mean":
            frame[f"_{output_name}_sum"] = values.fillna(0)
            frame[f"_{output_name}_count"] = values.notna().astype("int64")
        else:
            frame[f"_{output_name}"] = values
    return frame


def _build_rais_aggregation_map(measures: list[tuple[str, str | None, str]]) -> dict[str, str]:
    aggregations: dict[str, str] = {}
    for operation, _column, output_name in measures:
        if operation in BUILTIN_MEASURES:
            aggregations[f"_{operation}"] = "sum"
        elif operation == "mean":
            aggregations[f"_{output_name}_sum"] = "sum"
            aggregations[f"_{output_name}_count"] = "sum"
        elif operation in ("sum", "min", "max"):
            aggregations[f"_{output_name}"] = operation
    return aggregations


def finalize_rais_measures(frame: pd.DataFrame, dimensions: list[str], measures: list[tuple[str, str | None, str]]) -> pd.DataFrame:
    requested_output: list[str] = []
    for operation, _column, output_name in measures:
        if operation in BUILTIN_MEASURES:
            frame[output_name] = frame[f"_{operation}"]
        elif operation == "mean":
            denominator = frame[f"_{output_name}_count"].replace(0, pd.NA)
            frame[output_name] = frame[f"_{output_name}_sum"] / denominator
        elif operation in ("sum", "min", "max"):
            frame[output_name] = frame[f"_{output_name}"]
        requested_output.append(output_name)

    output = frame[dimensions + requested_output].copy()
    return output.sort_values(dimensions) if dimensions else output


def discover_input_files(data_dir: Path, input_format: str, subset_name: str) -> tuple[str, list[Path]]:
    subset = f"subset={subset_name}"
    candidates = {
        "parquet": sorted((data_dir / "parquet" / subset).glob("**/*.parquet")),
        "csv": sorted((data_dir / "csv" / subset).glob("**/*.csv.gz")),
        "txt": iter_microdata_text_files(data_dir / "extracted"),
    }
    if input_format == "auto":
        for candidate_format in ("parquet", "csv", "txt"):
            if candidates[candidate_format]:
                return candidate_format, candidates[candidate_format]
        raise FileNotFoundError(
            f"No converted or extracted RAIS files found under {data_dir}. "
            "If you only downloaded .7z files, run `python rais.py extract` and then `python rais.py convert` first."
        )
    if not candidates[input_format]:
        raise FileNotFoundError(f"No {input_format} files found under {data_dir}")
    return input_format, candidates[input_format]


def iter_normalized_frames(
    data_dir: Path,
    input_format: str,
    subset_name: str,
    chunksize: int,
    encoding: str,
):
    resolved_format, files = discover_input_files(data_dir, input_format, subset_name)
    logger.info("Reading %d %s file(s)", len(files), resolved_format)

    for path in files:
        if resolved_format == "parquet":
            yield pd.read_parquet(path)
        elif resolved_format == "csv":
            reader = pd.read_csv(path, dtype="string", chunksize=chunksize, low_memory=False)
            for chunk in reader:
                yield chunk
        else:
            source_year, source_kind, source_archive = infer_source_from_path(path)
            delimiter = detect_delimiter(path, encoding)
            reader = pd.read_csv(
                path,
                sep=delimiter,
                encoding=encoding,
                dtype="string",
                chunksize=chunksize,
                low_memory=False,
            )
            for chunk in reader:
                yield normalize_chunk(chunk, source_year, source_kind, source_archive)


def aggregate_data(
    data_dir: Path,
    input_format: str,
    subset_name: str,
    dimensions: list[str],
    measures: list[tuple[str, str | None, str]],
    filters: dict[str, set[str]],
    start_year: str | None,
    end_year: str | None,
    chunksize: int,
    encoding: str,
) -> pd.DataFrame:
    aggregates: list[pd.DataFrame] = []
    agg_map = _build_rais_aggregation_map(measures)

    for frame in iter_normalized_frames(data_dir, input_format, subset_name, chunksize, encoding):
        frame = coerce_known_types(frame)
        frame = apply_row_filters(frame, filters, start_year, end_year, "ano")
        if frame.empty:
            continue
        frame = add_metric_columns(frame, measures)
        aggregates.append(aggregate_chunk(frame, dimensions, measures, agg_map))
        if len(aggregates) > 20:
            aggregates = [combine_aggregates(aggregates, dimensions, measures, agg_map)]

    combined = combine_aggregates(aggregates, dimensions, measures, agg_map)
    return finalize_rais_measures(combined, dimensions, measures)
