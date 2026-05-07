"""TXT-to-Parquet/CSV conversion and column normalization for RAIS."""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd

from pdet.common import (
    detect_delimiter,
    make_unique_columns,
    normalize_code_series,
    normalize_uf_series,
    remove_tree,
    sanitize_column_name,
)
from pdet.rais.columns import (
    CANONICAL_RENAMES,
    CODE_COLUMNS,
    DECIMAL_COLUMNS,
    INTEGER_COLUMNS,
    STANDARD_COLUMNS,
)
from pdet.rais.extract import iter_microdata_text_files
from pdet.rais.ftp import archive_kind

logger = logging.getLogger(__name__)


def canonical_column(value: str) -> str:
    """Sanitize and map a raw column name to its canonical RAIS identifier."""
    sanitized = sanitize_column_name(value)
    return CANONICAL_RENAMES.get(sanitized, sanitized)


def normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate, sanitize, and rename RAIS columns to canonical names."""
    sanitized = make_unique_columns(sanitize_column_name(column) for column in frame.columns)
    frame = frame.copy()
    frame.columns = sanitized
    rename_map = {column: CANONICAL_RENAMES[column] for column in sanitized if column in CANONICAL_RENAMES}
    if rename_map:
        frame = frame.rename(columns=rename_map)
    frame.columns = make_unique_columns(frame.columns)
    return frame


def add_source_and_derived_columns(
    frame: pd.DataFrame,
    source_year: str,
    source_kind: str,
    source_archive: str,
) -> pd.DataFrame:
    frame["source_year"] = source_year
    frame["source_kind"] = source_kind
    frame["source_archive"] = source_archive
    if "ano" not in frame.columns:
        frame["ano"] = source_year

    if "municipio" in frame.columns:
        frame["municipio"] = normalize_code_series(frame["municipio"], 6)
        derived_uf = frame["municipio"].str.slice(0, 2)
        if "uf" not in frame.columns:
            frame["uf"] = derived_uf
        else:
            uf = normalize_uf_series(frame["uf"])
            missing = uf.isna() | (uf == "")
            uf.loc[missing] = derived_uf.loc[missing]
            frame["uf"] = normalize_uf_series(uf)
    elif "uf" in frame.columns:
        frame["uf"] = normalize_uf_series(frame["uf"])

    if "municipio_trabalho" in frame.columns:
        frame["municipio_trabalho"] = normalize_code_series(frame["municipio_trabalho"], 6)
    return frame


def coerce_known_types(frame: pd.DataFrame) -> pd.DataFrame:
    """Coerce known RAIS columns to their target types."""
    for column in CODE_COLUMNS:
        if column in frame.columns:
            if column == "uf":
                frame[column] = normalize_uf_series(frame[column])
            elif column in {"municipio", "municipio_trabalho"}:
                frame[column] = normalize_code_series(frame[column], 6)
            elif column in {"mes_admissao", "mes_desligamento"}:
                frame[column] = normalize_code_series(frame[column], 2)
            else:
                frame[column] = normalize_code_series(frame[column])

    for column in INTEGER_COLUMNS:
        if column in frame.columns:
            values = frame[column].astype("string").str.replace(",", ".", regex=False)
            frame[column] = pd.to_numeric(values, errors="coerce").astype("Int64")

    for column in DECIMAL_COLUMNS:
        if column in frame.columns:
            values = frame[column].astype("string").str.replace(",", ".", regex=False)
            frame[column] = pd.to_numeric(values, errors="coerce").astype("Float64")

    return frame


def order_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Reorder columns so standard ones appear first."""
    ordered = [column for column in STANDARD_COLUMNS if column in frame.columns]
    ordered.extend(column for column in frame.columns if column not in ordered)
    return frame[ordered]


def infer_source_from_path(txt_path: Path) -> tuple[str, str, str]:
    """Infer RAIS year, kind, and archive name from an extracted text file path."""
    year = next((part.name for part in txt_path.parents if re.fullmatch(r"\d{4}", part.name)), None)
    if not year:
        raise ValueError(f"Could not infer RAIS year from path: {txt_path}")
    kind = archive_kind(txt_path.name) or archive_kind(txt_path.parent.name)
    if not kind:
        upper = txt_path.name.upper()
        if "VINC" in upper:
            kind = "VINC"
        elif "ESTAB" in upper:
            kind = "ESTAB"
        else:
            kind = "RAIS"
    source_archive = txt_path.parent.name if txt_path.parent.name != year else txt_path.stem
    return year, kind, source_archive


def normalize_chunk(
    frame: pd.DataFrame,
    source_year: str,
    source_kind: str,
    source_archive: str,
) -> pd.DataFrame:
    frame = normalize_columns(frame)
    frame = add_source_and_derived_columns(frame, source_year, source_kind, source_archive)
    frame = coerce_known_types(frame)
    return order_columns(frame)


def convert_txt(
    txt_path: Path,
    output_dir: Path,
    output_format: str,
    chunksize: int,
    overwrite: bool,
    filters: dict[str, set[str]],
    encoding: str,
) -> list[Path]:
    """Convert a single extracted RAIS TXT file to partitioned Parquet or gzipped CSV."""
    source_year, source_kind, source_archive = infer_source_from_path(txt_path)
    target_dir = (
        output_dir
        / f"source_year={source_year}"
        / f"source_kind={source_kind}"
        / f"source_archive={source_archive}"
    )
    existing = sorted(target_dir.glob("part-*.parquet" if output_format == "parquet" else "part-*.csv.gz"))
    if existing and not overwrite:
        for path in existing:
            logger.info("exists: %s", path)
        return existing
    if target_dir.exists() and overwrite:
        remove_tree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    outputs: list[Path] = []
    delimiter = detect_delimiter(txt_path, encoding)
    reader = pd.read_csv(
        txt_path,
        sep=delimiter,
        encoding=encoding,
        dtype="string",
        chunksize=chunksize,
        low_memory=False,
    )

    for index, chunk in enumerate(reader):
        normalized = normalize_chunk(chunk, source_year, source_kind, source_archive)
        # Note: RAIS convert applies filters at conversion time (subset extraction)
        # This import is here to avoid circular imports with aggregate module
        from pdet.aggregation import apply_row_filters
        normalized = apply_row_filters(normalized, filters, None, None, "ano")
        if normalized.empty:
            continue

        if output_format == "parquet":
            output_path = target_dir / f"part-{index:05d}.parquet"
            normalized.to_parquet(output_path, index=False)
        else:
            output_path = target_dir / f"part-{index:05d}.csv.gz"
            normalized.to_csv(output_path, index=False, compression="gzip")
        logger.info("written: %s", output_path)
        outputs.append(output_path)
    if not outputs:
        logger.info("no rows matched filters for: %s", txt_path)
    return outputs
