"""Archive extraction helpers for RAIS."""

from __future__ import annotations

import logging
from pathlib import Path

from pdet.common import find_files, remove_tree, run_tar_extract

logger = logging.getLogger(__name__)

TEXT_SUFFIXES = {".txt", ".csv", ".comt"}


def is_microdata_text_path(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in TEXT_SUFFIXES


def iter_microdata_text_files(root: Path) -> list[Path]:
    return find_files(root, is_microdata_text_path)


def extract_archive(archive_path: Path, data_dir: Path, overwrite: bool) -> list[Path]:
    """Extract a RAIS ``.7z`` archive to ``data/rais/extracted/{year}/{stem}/``."""
    year = archive_path.parent.name
    destination_dir = data_dir / "extracted" / year / archive_path.stem

    if destination_dir.exists() and overwrite:
        remove_tree(destination_dir)
    destination_dir.mkdir(parents=True, exist_ok=True)

    existing = iter_microdata_text_files(destination_dir)
    if existing and not overwrite:
        for path in existing:
            logger.info("exists: %s", path)
        return existing

    run_tar_extract(archive_path, destination_dir)

    extracted = iter_microdata_text_files(destination_dir)
    if not extracted:
        raise RuntimeError(f"No TXT file found after extracting {archive_path}")
    for path in extracted:
        logger.info("extracted: %s", path)
    return extracted
