"""Novo CAGED CLI entry point."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pdet.aggregation import parse_filters, parse_measures, validate_dimensions
from pdet.caged.aggregate import aggregate_data
from pdet.caged.columns import ALL_COLUMNS, BUILTIN_MEASURES, NUMERIC_COLUMNS
from pdet.caged.convert import convert_txt, run_convert
from pdet.caged.extract import extract_archive
from pdet.caged.ftp import ARCHIVE_KINDS, available_months, download_archive, list_archives
from pdet.common import connect_ftp, select_period_range

logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR = Path("data")
INPUT_FORMATS = ("auto", "parquet", "csv", "txt")


def _parse_kinds(values: list[str]) -> set[str]:
    kinds = {value.upper() for value in values}
    invalid = kinds.difference(ARCHIVE_KINDS)
    if invalid:
        raise argparse.ArgumentTypeError(f"Invalid kinds: {', '.join(sorted(invalid))}")
    return kinds


def cmd_list(args: argparse.Namespace) -> None:
    with connect_ftp(args.timeout) as ftp:
        months = select_period_range(available_months(ftp), args.start, args.end)
        for month in months:
            archives = list_archives(ftp, month)
            names = ", ".join(archive.filename for archive in archives) or "no archives"
            print(f"{month}: {names}")


def cmd_download(args: argparse.Namespace) -> None:
    selected_kinds = _parse_kinds(args.kinds)
    with connect_ftp(args.timeout) as ftp:
        months = select_period_range(available_months(ftp), args.start, args.end)
        for month in months:
            archives = [item for item in list_archives(ftp, month) if item.kind in selected_kinds]
            for archive in archives:
                download_archive(ftp, archive, args.data_dir, args.overwrite)


def cmd_extract(args: argparse.Namespace) -> None:
    archives = sorted((args.data_dir / "raw").glob("**/*.7z"))
    for archive in archives:
        extract_archive(archive, args.data_dir, args.overwrite)


def cmd_convert(args: argparse.Namespace) -> None:
    run_convert(
        data_dir=args.data_dir,
        output_format=args.output_format,
        chunksize=args.chunksize,
        overwrite=args.overwrite,
        cleanup=args.cleanup,
    )


def cmd_aggregate(args: argparse.Namespace) -> None:
    dimensions = validate_dimensions(args.dimensions, ALL_COLUMNS)
    measures = parse_measures(args.measures, BUILTIN_MEASURES, NUMERIC_COLUMNS)
    filters = parse_filters(args.filter, ALL_COLUMNS)

    output = aggregate_data(
        data_dir=args.data_dir,
        input_format=args.input_format,
        dimensions=dimensions,
        measures=measures,
        filters=filters,
        start_mov=args.start_mov,
        end_mov=args.end_mov,
        chunksize=args.chunksize,
        initial_stock_csv=args.initial_stock_csv,
        initial_stock_column=args.initial_stock_column,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(args.output, index=False, encoding="utf-8")
    logger.info("written: %s", args.output)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download and wrangle Novo CAGED microdata from the public MTE FTP.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--timeout", type=int, default=60)

    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List available monthly archives.")
    list_parser.add_argument("--start", help="First competencia, AAAAMM.")
    list_parser.add_argument("--end", help="Last competencia, AAAAMM.")
    list_parser.set_defaults(func=cmd_list)

    download_parser = subparsers.add_parser("download", help="Download monthly .7z archives.")
    download_parser.add_argument("--start", required=True, help="First competencia, AAAAMM.")
    download_parser.add_argument("--end", required=True, help="Last competencia, AAAAMM.")
    download_parser.add_argument("--kinds", nargs="+", default=list(ARCHIVE_KINDS), help="Any of MOV FOR EXC.")
    download_parser.add_argument("--overwrite", action="store_true")
    download_parser.set_defaults(func=cmd_download)

    extract_parser = subparsers.add_parser("extract", help="Extract downloaded .7z archives into TXT files.")
    extract_parser.add_argument("--overwrite", action="store_true")
    extract_parser.set_defaults(func=cmd_extract)

    convert_parser = subparsers.add_parser("convert", help="Convert extracted TXT files to parquet or gzipped CSV.")
    convert_parser.add_argument("--output-format", choices=("parquet", "csv"), default="parquet")
    convert_parser.add_argument("--chunksize", type=int, default=500_000)
    convert_parser.add_argument("--overwrite", action="store_true")
    convert_parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove extracted TXT files after successful conversion to save disk space.",
    )
    convert_parser.set_defaults(func=cmd_convert)

    aggregate_parser = subparsers.add_parser("aggregate", help="Aggregate standardized Novo CAGED files to CSV.")
    aggregate_parser.add_argument(
        "--input-format",
        choices=INPUT_FORMATS,
        default="auto",
        help="Read data/parquet, data/csv, or data/extracted TXT. Auto prefers parquet, then csv, then txt.",
    )
    aggregate_parser.add_argument(
        "--dimensions",
        nargs="+",
        required=True,
        help="Standard columns to group by, such as competencia_mov municipio.",
    )
    aggregate_parser.add_argument(
        "--measures",
        nargs="+",
        required=True,
        help=(
            "Built-ins: saldo admitidos demitidos estoque movimentacoes. "
            "Generic numeric measures: sum:salario mean:idade min:salario max:salario."
        ),
    )
    aggregate_parser.add_argument(
        "--filter",
        action="append",
        default=[],
        help="Filter rows with column=value1,value2. Repeat for multiple columns. Example: --filter uf=33.",
    )
    aggregate_parser.add_argument("--start-mov", help="First competencia_mov to include, AAAAMM.")
    aggregate_parser.add_argument("--end-mov", help="Last competencia_mov to include, AAAAMM.")
    aggregate_parser.add_argument("--chunksize", type=int, default=500_000)
    aggregate_parser.add_argument(
        "--initial-stock-csv",
        type=Path,
        help=(
            "Optional CSV with the dimensions except competencia_mov plus an initial stock column. "
            "Without it, estoque is cumulative saldo from the selected data."
        ),
    )
    aggregate_parser.add_argument("--initial-stock-column", default="estoque_inicial")
    aggregate_parser.add_argument("--output", type=Path, required=True, help="CSV path to write.")
    aggregate_parser.set_defaults(func=cmd_aggregate)

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
