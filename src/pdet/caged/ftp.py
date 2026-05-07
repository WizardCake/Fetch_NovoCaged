"""FTP listing and download helpers for Novo CAGED."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from ftplib import FTP
from pathlib import Path

from pdet.common import connect_ftp, ftp_nlst, ftp_download, select_period_range
from pdet.caged.columns import ALL_COLUMNS

logger = logging.getLogger(__name__)

BASE_DIR = "/pdet/microdados/NOVO CAGED"
ARCHIVE_KINDS = ("MOV", "FOR", "EXC")


@dataclass(frozen=True)
class RemoteArchive:
    """Represents a single Novo CAGED ``.7z`` archive on the remote FTP server."""

    competencia: str
    kind: str
    filename: str

    @property
    def year(self) -> str:
        return self.competencia[:4]

    @property
    def remote_dir(self) -> str:
        return f"{BASE_DIR}/{self.year}/{self.competencia}"

    @property
    def stem(self) -> str:
        return Path(self.filename).stem


def available_months(ftp: FTP) -> list[str]:
    """Return a sorted list of all ``AAAAMM`` month folders available on the FTP."""
    years = [item for item in ftp_nlst(ftp, BASE_DIR) if re.fullmatch(r"\d{4}", item)]
    months: list[str] = []
    for year in sorted(years):
        for item in ftp_nlst(ftp, f"{BASE_DIR}/{year}"):
            if re.fullmatch(rf"{year}\d{{2}}", item):
                months.append(item)
    return sorted(months)


def list_archives(ftp: FTP, competencia: str) -> list[RemoteArchive]:
    """List all ``CAGED{MOV,FOR,EXC}{AAAAMM}.7z`` archives for a given competence month."""
    remote_dir = f"{BASE_DIR}/{competencia[:4]}/{competencia}"
    archives = []
    for filename in ftp_nlst(ftp, remote_dir):
        match = re.fullmatch(r"CAGED(MOV|FOR|EXC)(\d{6})\.7z", filename, flags=re.I)
        if not match:
            continue
        kind, file_competencia = match.group(1).upper(), match.group(2)
        if file_competencia == competencia:
            archives.append(RemoteArchive(competencia, kind, filename))
    return sorted(archives, key=lambda item: item.filename)


def download_archive(ftp: FTP, archive: RemoteArchive, data_dir: Path, overwrite: bool) -> Path:
    """Download a single archive to ``data/raw/{competencia}/``."""
    destination_dir = data_dir / "raw" / archive.competencia
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
