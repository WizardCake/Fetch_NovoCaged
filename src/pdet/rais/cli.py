"""RAIS CLI entry point."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pdet.aggregation import parse_filters, parse_measures
from pdet.rais.aggregate import aggregate_data
from pdet.rais.columns import BUILTIN_MEASURES, NUMERIC_COLUMNS, STANDARD_COLUMNS
from pdet.rais.convert import convert_txt, run_convert
from pdet.rais.extract import extract_archive, iter_microdata_text_files
from pdet.rais.ftp import (
    ARCHIVE_KINDS,
    available_years,
    download_archive,
    list_archives,
    parse_uf,
    parse_year,
)
from pdet.common import connect_ftp, select_period_range

logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR = Path("data") / "rais"
DEFAULT_YEAR = "2024"
DEFAULT_UF = "33"
DEFAULT_SUBSET = "uf_33"
DEFAULT_OUTPUT = DEFAULT_DATA_DIR / "exports" / "rj_municipios_2024.csv"
INPUT_FORMATS = ("auto", "parquet", "csv", "txt")
OUTPUT_FORMATS = ("parquet", "csv")
DEFAULT_TEXT_ENCODING = "latin-1"


def _default_rj_filters(filters: list[str], all_ufs: bool) -> list[str]:
    if filters or all_ufs:
        return filters
    return [f"uf={DEFAULT_UF}"]


def _subset_name_from_filters(filters: dict[str, set[str]]) -> str:
    if not filters:
        return "all"
    parts = []
    for column in sorted(filters):
        values = "_".join(sorted(filters[column]))
        parts.append(f"{column}_{values}")
    subset = "_".join(parts)
    import re
    subset = re.sub(r"[^A-Za-z0-9_=-]+", "_", subset)
    subset = re.sub(r"_+", "_", subset).strip("_")
    return subset or "filtered"


def _parse_kinds(values: list[str]) -> set[str]:
    kinds = {value.upper() for value in values}
    invalid = kinds.difference(ARCHIVE_KINDS)
    if invalid:
        raise argparse.ArgumentTypeError(f"Invalid kinds: {', '.join(sorted(invalid))}")
    return kinds


def _parse_dimensions(values: list[str]) -> list[str]:
    from pdet.rais.convert import canonical_column
    return [canonical_column(value) for value in values]


def _filtered_raw_archives(data_dir: Path, start_year: str | None, end_year: str | None) -> list[Path]:
    archives = sorted((data_dir / "raw").glob("*/*.7z"))
    selected = []
    for archive in archives:
        year = archive.parent.name
        if start_year and year < start_year:
            continue
        if end_year and year > end_year:
            continue
        selected.append(archive)
    return selected


def _filtered_txt_files(data_dir: Path, start_year: str | None, end_year: str | None) -> list[Path]:
    from pdet.rais.convert import infer_source_from_path
    txt_files = iter_microdata_text_files(data_dir / "extracted")
    selected = []
    for txt_path in txt_files:
        source_year, _source_kind, _source_archive = infer_source_from_path(txt_path)
        if start_year and source_year < start_year:
            continue
        if end_year and source_year > end_year:
            continue
        selected.append(txt_path)
    return selected


def cmd_list(args: argparse.Namespace) -> None:
    kinds = _parse_kinds(args.kinds)
    uf = parse_uf(args.uf)
    with connect_ftp(args.timeout) as ftp:
        years = select_period_range(available_years(ftp), args.start_year, args.end_year)
        for year in years:
            archives = list_archives(ftp, year, kinds, uf)
            names = ", ".join(archive.filename for archive in archives) or "no archives"
            print(f"{year}: {names}")


def cmd_download(args: argparse.Namespace) -> None:
    kinds = _parse_kinds(args.kinds)
    uf = parse_uf(args.uf)
    with connect_ftp(args.timeout) as ftp:
        years = select_period_range(available_years(ftp), args.start_year, args.end_year)
        for year in years:
            for archive in list_archives(ftp, year, kinds, uf):
                download_archive(ftp, archive, args.data_dir, args.overwrite)


def cmd_extract(args: argparse.Namespace) -> None:
    archives = _filtered_raw_archives(args.data_dir, args.start_year, args.end_year)
    if not archives:
        raise FileNotFoundError(
            f"No raw RAIS .7z archives found under {args.data_dir / 'raw'} for "
            f"{args.start_year}-{args.end_year}. Run `python rais.py download` first."
        )
    for archive in archives:
        extract_archive(archive, args.data_dir, args.overwrite)


def cmd_convert(args: argparse.Namespace) -> None:
    filter_values = _default_rj_filters(args.filter, args.all_ufs)
    filters = parse_filters(filter_values, STANDARD_COLUMNS)
    run_convert(
        data_dir=args.data_dir,
        output_format=args.output_format,
        chunksize=args.chunksize,
        overwrite=args.overwrite,
        filters=filters,
        encoding=args.encoding,
        cleanup=args.cleanup,
    )


def cmd_aggregate(args: argparse.Namespace) -> None:
    dimensions = _parse_dimensions(args.dimensions)
    measures = parse_measures(args.measures, BUILTIN_MEASURES, NUMERIC_COLUMNS)
    filter_values = _default_rj_filters(args.filter, args.all_ufs)
    filters = parse_filters(filter_values, STANDARD_COLUMNS)

    output = aggregate_data(
        data_dir=args.data_dir,
        input_format=args.input_format,
        subset_name=args.subset_name,
        dimensions=dimensions,
        measures=measures,
        filters=filters,
        start_year=args.start_year,
        end_year=args.end_year,
        chunksize=args.chunksize,
        encoding=args.encoding,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(args.output, index=False, encoding="utf-8")
    logger.info("written: %s", args.output)


def cmd_rj_2024(args: argparse.Namespace) -> None:
    """Run the validated default workflow: consolidated RAIS 2024, RJ only."""
    year = DEFAULT_YEAR
    uf = DEFAULT_UF
    filters = {"uf": {uf}}
    archive_paths: list[Path] = []
    txt_files: list[Path] = []

    with connect_ftp(args.timeout) as ftp:
        archives = list_archives(ftp, year, {"VINC"}, uf)
        if not archives:
            raise FileNotFoundError(f"No consolidated RAIS VINC archive found for UF {uf} in {year}.")
        for archive in archives:
            archive_paths.append(download_archive(ftp, archive, args.data_dir, args.overwrite))

    run_convert(
        data_dir=args.data_dir,
        output_format=args.output_format,
        chunksize=args.chunksize,
        overwrite=args.overwrite,
        filters=filters,
        encoding=args.encoding,
        cleanup=False,
    )

    dimensions = ["ano", "municipio"]
    measures = parse_measures(
        [
            "estoque_3112",
            "vinculos",
            "admitidos_ano",
            "desligados_ano",
            "mean:remuneracao_media_nominal",
        ],
        BUILTIN_MEASURES,
        NUMERIC_COLUMNS,
    )
    output = aggregate_data(
        data_dir=args.data_dir,
        input_format="auto",
        subset_name=DEFAULT_SUBSET,
        dimensions=dimensions,
        measures=measures,
        filters=filters,
        start_year=year,
        end_year=year,
        chunksize=args.chunksize,
        encoding=args.encoding,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(args.output, index=False, encoding="utf-8")
    logger.info("written: %s", args.output)


def _add_year_range_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start-year", type=parse_year, default=DEFAULT_YEAR, help="First consolidated RAIS year.")
    parser.add_argument("--end-year", type=parse_year, default=DEFAULT_YEAR, help="Last consolidated RAIS year.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download and wrangle annual RAIS microdata from the public MTE FTP.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--timeout", type=int, default=60)

    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List available consolidated annual RAIS archives.")
    _add_year_range_arguments(list_parser)
    list_parser.add_argument("--kinds", nargs="+", default=["VINC"], help="Any of VINC ESTAB.")
    list_parser.add_argument("--uf", default=DEFAULT_UF, help="UF code/acronym to select VINC archive, or 'all'.")
    list_parser.set_defaults(func=cmd_list)

    download_parser = subparsers.add_parser("download", help="Download consolidated annual RAIS .7z archives.")
    _add_year_range_arguments(download_parser)
    download_parser.add_argument("--kinds", nargs="+", default=["VINC"], help="Any of VINC ESTAB.")
    download_parser.add_argument("--uf", default=DEFAULT_UF, help="UF code/acronym to select VINC archive, or 'all'.")
    download_parser.add_argument("--overwrite", action="store_true")
    download_parser.set_defaults(func=cmd_download)

    extract_parser = subparsers.add_parser("extract", help="Extract downloaded RAIS .7z archives into TXT files.")
    _add_year_range_arguments(extract_parser)
    extract_parser.add_argument("--overwrite", action="store_true")
    extract_parser.set_defaults(func=cmd_extract)

    convert_parser = subparsers.add_parser("convert", help="Convert extracted RAIS TXT files to parquet or gzipped CSV.")
    _add_year_range_arguments(convert_parser)
    convert_parser.add_argument("--output-format", choices=OUTPUT_FORMATS, default="parquet")
    convert_parser.add_argument("--subset-name", help="Output subset name. Defaults to a name derived from filters.")
    convert_parser.add_argument("--filter", action="append", default=[], help="Filter rows with column=value1,value2.")
    convert_parser.add_argument("--all-ufs", action="store_true", help="Disable the default uf=33 filter.")
    convert_parser.add_argument("--encoding", default=DEFAULT_TEXT_ENCODING)
    convert_parser.add_argument("--chunksize", type=int, default=500_000)
    convert_parser.add_argument("--overwrite", action="store_true")
    convert_parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove extracted TXT files after successful conversion to save disk space.",
    )
    convert_parser.set_defaults(func=cmd_convert)

    aggregate_parser = subparsers.add_parser("aggregate", help="Aggregate standardized RAIS files to CSV.")
    _add_year_range_arguments(aggregate_parser)
    aggregate_parser.add_argument(
        "--input-format",
        choices=INPUT_FORMATS,
        default="auto",
        help="Read data/rais/parquet, data/rais/csv, or data/rais/extracted TXT.",
    )
    aggregate_parser.add_argument("--subset-name", default=DEFAULT_SUBSET)
    aggregate_parser.add_argument("--dimensions", nargs="+", default=["ano", "municipio"])
    aggregate_parser.add_argument(
        "--measures",
        nargs="+",
        default=[
            "estoque_3112",
            "vinculos",
            "admitidos_ano",
            "desligados_ano",
            "mean:remuneracao_media_nominal",
        ],
    )
    aggregate_parser.add_argument("--filter", action="append", default=[], help="Filter rows with column=value1,value2.")
    aggregate_parser.add_argument("--all-ufs", action="store_true", help="Disable the default uf=33 filter.")
    aggregate_parser.add_argument("--encoding", default=DEFAULT_TEXT_ENCODING)
    aggregate_parser.add_argument("--chunksize", type=int, default=500_000)
    aggregate_parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    aggregate_parser.set_defaults(func=cmd_aggregate)

    rj_parser = subparsers.add_parser(
        "rj-2024",
        help="Default pipeline: consolidated RAIS 2024, RJ municipalities, VINC archive only.",
    )
    rj_parser.add_argument("--output-format", choices=OUTPUT_FORMATS, default="parquet")
    rj_parser.add_argument("--encoding", default=DEFAULT_TEXT_ENCODING)
    rj_parser.add_argument("--chunksize", type=int, default=500_000)
    rj_parser.add_argument("--overwrite", action="store_true")
    rj_parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    rj_parser.set_defaults(func=cmd_rj_2024)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        args.func(args)
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        parser.exit(1, f"error: {exc}\n")


if __name__ == "__main__":
    main()
