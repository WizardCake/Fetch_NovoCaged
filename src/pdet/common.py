"""Shared helpers for MTE/PDET FTP microdata scripts."""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
import time
import unicodedata
from ftplib import FTP, error_temp
from pathlib import Path
from typing import Callable, Iterable

logger = logging.getLogger(__name__)

FTP_HOST = "ftp.mtps.gov.br"
FTP_CONTROL_ENCODING = "latin-1"

UF_CODE_TO_ACRONYM = {
    "11": "RO",
    "12": "AC",
    "13": "AM",
    "14": "RR",
    "15": "PA",
    "16": "AP",
    "17": "TO",
    "21": "MA",
    "22": "PI",
    "23": "CE",
    "24": "RN",
    "25": "PB",
    "26": "PE",
    "27": "AL",
    "28": "SE",
    "29": "BA",
    "31": "MG",
    "32": "ES",
    "33": "RJ",
    "35": "SP",
    "41": "PR",
    "42": "SC",
    "43": "RS",
    "50": "MS",
    "51": "MT",
    "52": "GO",
    "53": "DF",
}
UF_ACRONYM_TO_CODE = {value: key for key, value in UF_CODE_TO_ACRONYM.items()}


class FTPContextManager:
    """Wraps ftplib.FTP so that ``with`` blocks close the connection on exit."""

    def __init__(self, ftp: FTP) -> None:
        self.ftp = ftp

    def __enter__(self) -> FTP:
        return self.ftp

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            self.ftp.quit()
        except Exception:
            logger.debug("FTP quit() failed, forcing close().")
            try:
                self.ftp.close()
            except Exception:
                pass


def connect_ftp(timeout: int = 60, host: str = FTP_HOST, encoding: str = FTP_CONTROL_ENCODING) -> FTPContextManager:
    """Open an anonymous FTP connection to the MTE server."""
    logger.debug("Connecting to %s (timeout=%d, encoding=%s)", host, timeout, encoding)
    ftp = FTP(host, timeout=timeout)
    ftp.encoding = encoding
    ftp.login()
    return FTPContextManager(ftp)


def ftp_nlst(ftp: FTP, path: str) -> list[str]:
    """List directory contents while preserving the original working directory."""
    cwd = ftp.pwd()
    try:
        ftp.cwd(path)
        return ftp.nlst()
    finally:
        ftp.cwd(cwd)


def ftp_download(
    ftp: FTP,
    filename: str,
    dest_path: Path,
    max_retries: int = 3,
    backoff: int = 5,
) -> None:
    """Download a single file from the current FTP directory with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            with dest_path.open("wb") as handle:
                ftp.retrbinary(f"RETR {filename}", handle.write)
            logger.info("Downloaded %s", dest_path)
            return
        except (TimeoutError, ConnectionError, error_temp) as exc:
            if attempt == max_retries:
                logger.error("Failed to download %s after %d attempts: %s", filename, max_retries, exc)
                raise RuntimeError(f"FTP download failed for {filename}: {exc}") from exc
            logger.warning("FTP download attempt %d/%d failed for %s: %s", attempt, max_retries, filename, exc)
            time.sleep(backoff * attempt)


def select_period_range(periods: Iterable[str], start: str | None, end: str | None) -> list[str]:
    """Filter a sorted list of period strings by an optional inclusive range."""
    selected = []
    for period in periods:
        if start and period < start:
            continue
        if end and period > end:
            continue
        selected.append(period)
    return selected


def run_tar_extract(archive_path: Path, destination_dir: Path) -> None:
    """Extract a ``.7z`` archive using the system ``tar`` command."""
    tar = shutil.which("tar")
    if not tar:
        raise RuntimeError("Could not find 'tar'. Install 7-Zip or bsdtar and put it on PATH.")
    destination_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run([tar, "-xf", str(archive_path), "-C", str(destination_dir)], check=True)


def remove_tree(path: Path) -> None:
    """Recursively delete a directory tree if it exists."""
    if path.exists():
        shutil.rmtree(path)


def find_files(root: Path, predicate: Callable[[Path], bool]) -> list[Path]:
    """Recursively collect files under *root* that satisfy *predicate*."""
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*") if path.is_file() and predicate(path))


def repair_mojibake(value: str) -> str:
    """Attempt to fix double-encoded UTF-8 strings (common in Brazilian government files)."""
    if "\u00c3" not in value and "\u00c2" not in value:
        return value
    try:
        return value.encode("latin-1").decode("utf-8")
    except UnicodeError:
        return value


def sanitize_column_name(value: object) -> str:
    """Convert an arbitrary column header to a snake_case ASCII identifier."""
    text = repair_mojibake(str(value)).strip().lower()
    text = text.replace("31/12", "31_12")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "column"


def make_unique_columns(columns: Iterable[str]) -> list[str]:
    """Deduplicate column names by appending ``_1``, ``_2``, etc."""
    seen: dict[str, int] = {}
    unique = []
    for column in columns:
        count = seen.get(column, 0)
        seen[column] = count + 1
        unique.append(column if count == 0 else f"{column}_{count}")
    return unique


def normalize_code_series(series, width: int | None = None):
    """Strip whitespace, nullify empty strings, remove ``.0`` suffix, and optionally zero-pad numeric codes."""
    import pandas as pd

    values = series.astype("string").str.strip()
    values = values.replace({"": pd.NA, "nan": pd.NA, "NaN": pd.NA, "None": pd.NA})
    values = values.str.replace(r"\.0$", "", regex=True)
    if width is not None:
        mask = values.notna() & values.str.fullmatch(r"\d+")
        values.loc[mask] = values.loc[mask].str.zfill(width)
    return values


def normalize_uf_series(series):
    """Convert UF column to a two-digit IBGE code, handling acronyms and numeric codes."""
    values = normalize_code_series(series)
    upper = values.str.upper()
    mapped = upper.map(UF_ACRONYM_TO_CODE)
    values = values.where(mapped.isna(), mapped)
    mask = values.notna() & values.str.fullmatch(r"\d+")
    values.loc[mask] = values.loc[mask].str.zfill(2)
    return values


def coerce_numeric_columns(frame, columns: Iterable[str], fill_value=None):
    """Coerce a list of columns to numeric, replacing commas with dots."""
    import pandas as pd

    for column in columns:
        if column not in frame.columns:
            continue
        values = frame[column].astype("string").str.replace(",", ".", regex=False)
        frame[column] = pd.to_numeric(values, errors="coerce")
        if fill_value is not None:
            frame[column] = frame[column].fillna(fill_value)
    return frame


def detect_delimiter(path: Path, encoding: str, delimiters: tuple[str, ...] = (";", ",", "\t")) -> str:
    """Heuristically detect the most frequent delimiter on the first line of a text file."""
    with path.open("r", encoding=encoding, errors="replace", newline="") as handle:
        header = handle.readline()
    counts = {delimiter: header.count(delimiter) for delimiter in delimiters}
    delimiter = max(counts, key=counts.get)
    return delimiter if counts[delimiter] else delimiters[0]


def require_columns(columns: Iterable[str], required: Iterable[str], label: str) -> None:
    """Raise *ValueError* if any of *required* columns is missing from *columns*."""
    missing = sorted(set(required).difference(columns))
    if missing:
        raise ValueError(f"{label} is missing columns: {', '.join(missing)}")
