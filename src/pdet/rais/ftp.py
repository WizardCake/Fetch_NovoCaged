"""FTP listing and download helpers for RAIS."""

from __future__ import annotations

import argparse
import logging
import re
from dataclasses import dataclass
from ftplib import FTP
from pathlib import Path

from pdet.common import connect_ftp, ftp_nlst, ftp_download, select_period_range
from pdet.common import UF_ACRONYM_TO_CODE, UF_CODE_TO_ACRONYM
from pdet.rais.columns import UF_CODE_TO_GROUP_TOKEN

logger = logging.getLogger(__name__)

BASE_DIR = "/pdet/microdados/RAIS"
ARCHIVE_KINDS = ("VINC", "ESTAB")


@dataclass(frozen=True)
class RaisRemoteArchive:
    """Represents a single RAIS ``.7z`` archive on the remote FTP server."""

    year: str
    kind: str
    filename: str

    @property
    def remote_dir(self) -> str:
        return f"{BASE_DIR}/{self.year}"

    @property
    def stem(self) -> str:
        return Path(self.filename).stem


def available_years(ftp: FTP) -> list[str]:
    """Return sorted consolidated year folders (``YYYY`` only; ignore ``YYYY Parcial``)."""
    years = [item for item in ftp_nlst(ftp, BASE_DIR) if re.fullmatch(r"\d{4}", item)]
    return sorted(years)


def parse_year(value: str) -> str:
    """Validate that *value* is a four-digit consolidated year string."""
    value = str(value).strip()
    if not re.fullmatch(r"\d{4}", value):
        raise argparse.ArgumentTypeError("Use a consolidated RAIS year folder, exactly YYYY. Do not use Parcial.")
    return value


def parse_uf(value: str | None) -> str | None:
    """Normalize a UF argument to a two-digit IBGE code, or ``None`` for "all"."""
    if value is None:
        return None
    value = str(value).strip().upper()
    if value in {"", "ALL", "TODAS", "TODOS"}:
        return None
    if value in UF_ACRONYM_TO_CODE:
        return UF_ACRONYM_TO_CODE[value]
    if re.fullmatch(r"\d{2}", value) and value in UF_CODE_TO_ACRONYM:
        return value
    raise argparse.ArgumentTypeError("Use a two-digit IBGE UF code, UF acronym, or 'all'.")


def archive_kind(filename: str) -> str | None:
    """Guess RAIS archive kind from filename."""
    upper = filename.upper()
    if upper.startswith("RAIS_VINC") or "_VINC_" in upper:
        return "VINC"
    if upper.startswith("RAIS_ESTAB") or "_ESTAB_" in upper:
        return "ESTAB"
    return None


def archive_matches_uf(filename: str, kind: str, uf: str | None) -> bool:
    """Check whether a RAIS archive filename covers the requested UF."""
    if uf is None or kind == "ESTAB":
        return True
    acronym = UF_CODE_TO_ACRONYM[uf]
    stem = Path(filename).stem.upper()
    tokens = {token for token in re.split(r"[^A-Z0-9]+", stem) if token}
    if acronym in tokens:
        return True
    group_token = UF_CODE_TO_GROUP_TOKEN.get(uf)
    if group_token and group_token in tokens:
        return True
    if group_token == "MG_ES_RJ" and {"MG", "ES", "RJ"}.issubset(tokens):
        return True
    return False


def list_archives(ftp: FTP, year: str, kinds: set[str], uf: str | None) -> list[RaisRemoteArchive]:
    """List RAIS archives for a given year, filtered by kind and UF."""
    if not re.fullmatch(r"\d{4}", year):
        raise ValueError(f"Invalid consolidated RAIS year: {year}")
    archives = []
    for filename in ftp_nlst(ftp, f"{BASE_DIR}/{year}"):
        if not filename.upper().endswith(".7Z"):
            continue
        kind = archive_kind(filename)
        if kind is None or kind not in kinds:
            continue
        if not archive_matches_uf(filename, kind, uf):
            continue
        archives.append(RaisRemoteArchive(year, kind, filename))
    return sorted(archives, key=lambda item: item.filename)


def download_archive(ftp: FTP, archive: RaisRemoteArchive, data_dir: Path, overwrite: bool) -> Path:
    """Download a single RAIS archive to ``data/rais/raw/{year}/``."""
    destination_dir = data_dir / "raw" / archive.year
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination = destination_dir / archive.filename
    if destination.exists() and not overwrite:
        logger.info("exists: %s", destination)
        return destination

    cwd = ftp.pwd()
    try:
        ftp.cwd(archive.remote_dir)
        ftp_download(ftp, archive.filename, destination)
    finally:
        ftp.cwd(cwd)
    return destination
