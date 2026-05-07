"""Shared aggregation logic for CAGED and RAIS pipelines."""

from __future__ import annotations

import argparse
import logging
from typing import Sequence

import pandas as pd

logger = logging.getLogger(__name__)

GENERIC_AGGREGATIONS = ("sum", "mean", "min", "max")


def parse_filters(values: list[str], valid_columns: Sequence[str]) -> dict[str, set[str]]:
    """Parse CLI filter expressions like ``uf=33,35`` into a mapping of column names to allowed values."""
    filters: dict[str, set[str]] = {}
    for value in values:
        if "=" not in value:
            raise argparse.ArgumentTypeError(f"Invalid filter '{value}'. Use column=value1,value2.")
        column, raw_values = value.split("=", 1)
        column = column.strip()
        if column not in valid_columns:
            raise argparse.ArgumentTypeError(f"Invalid filter column '{column}'.")
        selected = {item.strip() for item in raw_values.split(",") if item.strip()}
        if not selected:
            raise argparse.ArgumentTypeError(f"Filter '{value}' has no values.")
        filters.setdefault(column, set()).update(selected)
    return filters


def parse_measures(
    values: list[str],
    builtin_measures: tuple[str, ...],
    numeric_columns: Sequence[str],
) -> list[tuple[str, str | None, str]]:
    """Parse measure expressions into ``(operation, column, output_name)`` tuples.

    Built-in measures have ``column=None``.
    Generic measures follow the pattern ``sum:salario`` or ``mean:idade``.
    """
    measures: list[tuple[str, str | None, str]] = []
    output_names: set[str] = set()
    for value in values:
        value = value.strip()
        if value in builtin_measures:
            parsed = (value, None, value)
        elif ":" in value:
            operation, column = value.split(":", 1)
            operation = operation.strip()
            column = column.strip()
            if operation not in GENERIC_AGGREGATIONS:
                raise argparse.ArgumentTypeError(
                    f"Invalid aggregation '{operation}'. Use one of: {', '.join(GENERIC_AGGREGATIONS)}."
                )
            if column not in numeric_columns:
                raise argparse.ArgumentTypeError(
                    f"Invalid numeric measure column '{column}'. Use one of: {', '.join(numeric_columns)}."
                )
            parsed = (operation, column, f"{operation}_{column}")
        else:
            raise argparse.ArgumentTypeError(
                f"Invalid measure '{value}'. Use built-ins {', '.join(builtin_measures)} "
                "or generic measures like sum:salario and mean:idade."
            )
        if parsed[2] in output_names:
            continue
        output_names.add(parsed[2])
        measures.append(parsed)
    return measures


def validate_dimensions(values: list[str], valid_columns: Sequence[str]) -> list[str]:
    """Raise if any requested dimension is not in the known column list."""
    invalid = [value for value in values if value not in valid_columns]
    if invalid:
        raise argparse.ArgumentTypeError(f"Invalid dimensions: {', '.join(invalid)}")
    return values


def apply_row_filters(
    frame: pd.DataFrame,
    filters: dict[str, set[str]],
    start_value: str | None,
    end_value: str | None,
    period_column: str,
) -> pd.DataFrame:
    """Filter *frame* by dimension values and an optional inclusive period range."""
    mask: pd.Series | None = None
    for column, allowed in filters.items():
        if column not in frame.columns:
            raise ValueError(f"Filter column not found: {column}")
        current = frame[column].astype("string").isin(allowed)
        mask = current if mask is None else mask & current

    if start_value:
        current = frame[period_column].astype("string") >= start_value
        mask = current if mask is None else mask & current
    if end_value:
        current = frame[period_column].astype("string") <= end_value
        mask = current if mask is None else mask & current

    if mask is None:
        return frame
    return frame.loc[mask].copy()


def aggregate_chunk(
    frame: pd.DataFrame,
    dimensions: list[str],
    measures: list[tuple[str, str | None, str]],
    aggregation_map: dict[str, str],
) -> pd.DataFrame:
    """Aggregate a single DataFrame chunk using the provided column-to-agg mapping.

    *aggregation_map* maps internal column names (prefixed with ``_``) to aggregation
    functions (e.g. ``"sum"``, ``"min"``).
    """
    if not aggregation_map:
        raise ValueError("No aggregations requested.")

    missing_dimensions = [dimension for dimension in dimensions if dimension not in frame.columns]
    if missing_dimensions:
        raise ValueError(f"Dimension columns not found: {', '.join(missing_dimensions)}")

    if dimensions:
        return frame.groupby(dimensions, dropna=False).agg(aggregation_map).reset_index()
    return frame.agg(aggregation_map).to_frame().T


def combine_aggregates(
    aggregates: list[pd.DataFrame],
    dimensions: list[str],
    measures: list[tuple[str, str | None, str]],
    aggregation_map: dict[str, str],
) -> pd.DataFrame:
    """Combine a list of partially-aggregated DataFrames into one final result."""
    if not aggregates:
        return pd.DataFrame(columns=dimensions + [measure[2] for measure in measures])

    combined = pd.concat(aggregates, ignore_index=True)

    if dimensions:
        return combined.groupby(dimensions, dropna=False).agg(aggregation_map).reset_index()
    return combined.agg(aggregation_map).to_frame().T


def finalize_measures(
    frame: pd.DataFrame,
    dimensions: list[str],
    measures: list[tuple[str, str | None, str]],
) -> pd.DataFrame:
    """Derive final output columns from internal ``_``-prefixed intermediate columns."""
    requested_output: list[str] = []
    for operation, _column, output_name in measures:
        internal = f"_{operation}"
        if operation == "mean":
            denominator = frame[f"_{output_name}_count"].replace(0, pd.NA)
            frame[output_name] = frame[f"_{output_name}_sum"] / denominator
        elif operation in ("sum", "min", "max"):
            frame[output_name] = frame[f"_{output_name}"]
        elif internal in frame.columns:
            frame[output_name] = frame[internal]
        else:
            raise ValueError(f"Internal column '{internal}' not found for measure '{output_name}'")
        requested_output.append(output_name)

    output = frame[dimensions + requested_output].copy()
    return output.sort_values(dimensions) if dimensions else output


def build_aggregation_map(measures: list[tuple[str, str | None, str]]) -> dict[str, str]:
    """Construct a column→agg mapping from parsed measures.

    Returns a dict mapping internal column names (with ``_`` prefix) to aggregation
    function names (``"sum"``, ``"min"``, ``"max"``, etc.).
    """
    aggregations: dict[str, str] = {}
    for operation, _column, output_name in measures:
        if operation == "mean":
            aggregations[f"_{output_name}_sum"] = "sum"
            aggregations[f"_{output_name}_count"] = "sum"
        elif operation in ("sum", "min", "max"):
            aggregations[f"_{output_name}"] = operation
        else:
            aggregations[f"_{operation}"] = "sum"
    return aggregations
