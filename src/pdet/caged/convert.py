"""TXT-to-Parquet/CSV conversion for Novo CAGED."""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd

from pdet.common import remove_tree
from pdet.caged.columns import ALL_COLUMNS, COLUMN_RENAMES, DECIMAL_COLUMNS, INTEGER_COLUMNS, STRING_COLUMNS

logger = logging.getLogger(__name__)


def normalize_chunk(
    frame: pd.DataFrame,
    source_competencia: str,
    source_kind: str,
) -> pd.DataFrame:
    """Rename columns, add source metadata, and coerce types for a raw CAGED chunk."""
    frame = frame.rename(columns=COLUMN_RENAMES)
    frame["source_competencia"] = source_competencia
    frame["source_kind"] = source_kind

    for column in ALL_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA

    for column in STRING_COLUMNS:
        frame[column] = frame[column].astype("string")
    for column in INTEGER_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
    for column in DECIMAL_COLUMNS:
        values = frame[column].astype("string").str.replace(",", ".", regex=False)
        frame[column] = pd.to_numeric(values, errors="coerce").astype("Float64")

    return frame[ALL_COLUMNS]


def coerce_normalized_types(frame: pd.DataFrame) -> pd.DataFrame:
    """Re-apply type coercion to a frame that may have lost types (e.g. read from CSV)."""
    for column in ALL_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA
    for column in STRING_COLUMNS:
        frame[column] = frame[column].astype("string")
    for column in INTEGER_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
    for column in DECIMAL_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Float64")
    return frame[ALL_COLUMNS]


def convert_txt(
    txt_path: Path,
    output_dir: Path,
    output_format: str,
    chunksize: int,
    overwrite: bool,
) -> list[Path]:
    """Convert a single extracted CAGED TXT file to partitioned Parquet or gzipped CSV."""
    match = re.fullmatch(r"CAGED(MOV|FOR|EXC)(\d{6})\.txt", txt_path.name, flags=re.I)
    if not match:
        raise ValueError(f"Unexpected Novo CAGED TXT name: {txt_path.name}")
    source_kind, source_competencia = match.group(1).upper(), match.group(2)

    target_dir = output_dir / f"source_competencia={source_competencia}" / f"source_kind={source_kind}"
    if target_dir.exists() and overwrite:
        remove_tree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    outputs: list[Path] = []
    reader = pd.read_csv(
        txt_path,
        sep=";",
        encoding="utf-8",
        dtype="string",
        chunksize=chunksize,
        low_memory=False,
    )

    for index, chunk in enumerate(reader):
        normalized = normalize_chunk(chunk, source_competencia, source_kind)
        if output_format == "parquet":
            output_path = target_dir / f"part-{index:05d}.parquet"
            normalized.to_parquet(output_path, index=False)
        else:
            output_path = target_dir / f"part-{index:05d}.csv.gz"
            normalized.to_csv(output_path, index=False, compression="gzip")
        logger.info("written: %s", output_path)
        outputs.append(output_path)
    return outputs
