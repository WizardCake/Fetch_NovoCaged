"""Archive extraction helpers for Novo CAGED."""

from __future__ import annotations

import logging
from pathlib import Path

from pdet.common import remove_tree, run_tar_extract

logger = logging.getLogger(__name__)


def extract_archive(archive_path: Path, data_dir: Path, overwrite: bool) -> list[Path]:
    """Extract a ``.7z`` archive to ``data/extracted/{competencia}/``."""
    competencia = archive_path.parent.name
    destination_dir = data_dir / "extracted" / competencia
    destination_dir.mkdir(parents=True, exist_ok=True)

    expected_txt = destination_dir / f"{archive_path.stem}.txt"
    if expected_txt.exists() and not overwrite:
        logger.info("exists: %s", expected_txt)
        return [expected_txt]

    run_tar_extract(archive_path, destination_dir)
    extracted = sorted(destination_dir.glob(f"{archive_path.stem}*.txt"))
    if not extracted:
        raise RuntimeError(f"No TXT file found after extracting {archive_path}")
    for path in extracted:
        logger.info("extracted: %s", path)
    return extracted
