"""Integration CLI entry point."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pdet.integra.core import DEFAULT_CAGED_CSV, DEFAULT_KEYS, DEFAULT_OUTPUT, DEFAULT_RAIS_CSV, integrate

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Join RAIS annual stock with Novo CAGED monthly flows and calculate monthly stock evolution."
    )
    parser.add_argument("--rais", type=Path, default=DEFAULT_RAIS_CSV, help="RAIS municipal annual CSV.")
    parser.add_argument("--caged", type=Path, default=DEFAULT_CAGED_CSV, help="Novo CAGED municipal monthly CSV.")
    parser.add_argument("--keys", nargs="+", default=DEFAULT_KEYS, help="Join dimensions, default: municipio.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Integrated monthly CSV output.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        output = integrate(args.rais, args.caged, args.keys)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        output.to_csv(args.output, index=False, encoding="utf-8")
        logger.info("written: %s", args.output)
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        parser.exit(1, f"error: {exc}\n")


if __name__ == "__main__":
    main()
