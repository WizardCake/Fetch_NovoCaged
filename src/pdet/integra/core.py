"""Integrate annual RAIS stock with monthly Novo CAGED flows."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from pdet.common import coerce_numeric_columns, normalize_code_series, require_columns

logger = logging.getLogger(__name__)

DEFAULT_RAIS_CSV = Path("data") / "rais" / "exports" / "rj_municipios_2024.csv"
DEFAULT_CAGED_CSV = Path("data") / "exports" / "rj_municipios.csv"
DEFAULT_OUTPUT = Path("data") / "exports" / "rj_municipios_rais_caged.csv"
DEFAULT_KEYS = ["municipio"]


def read_rais(path: Path, keys: list[str]) -> pd.DataFrame:
    """Load and normalize a RAIS aggregate CSV."""
    frame = pd.read_csv(path, dtype="string")
    required = set(keys + ["ano", "estoque_3112"])
    require_columns(frame.columns, required, "RAIS CSV")

    for key in keys:
        frame[key] = normalize_code_series(frame[key], 6 if key == "municipio" else None)
    frame["ano_rais"] = pd.to_numeric(frame["ano"], errors="coerce").astype("Int64")
    frame["ano_caged"] = frame["ano_rais"] + 1

    rename_map = {
        "admitidos_ano": "admitidos_rais_ano",
        "desligados_ano": "desligados_rais_ano",
        "vinculos": "vinculos_rais_ano",
    }
    frame = frame.rename(columns={key: value for key, value in rename_map.items() if key in frame.columns})

    numeric_columns = [
        "estoque_3112",
        "admitidos_rais_ano",
        "desligados_rais_ano",
        "vinculos_rais_ano",
        "mean_remuneracao_media_nominal",
    ]
    frame = coerce_numeric_columns(frame, numeric_columns)

    duplicate_keys = frame.duplicated(keys + ["ano_rais"], keep=False)
    if duplicate_keys.any():
        sample = frame.loc[duplicate_keys, keys + ["ano_rais"]].head().to_dict("records")
        raise ValueError(f"RAIS CSV has duplicated key/year rows. Sample: {sample}")

    keep_columns = [
        *keys,
        "ano_rais",
        "ano_caged",
        "estoque_3112",
        "admitidos_rais_ano",
        "desligados_rais_ano",
        "vinculos_rais_ano",
        "mean_remuneracao_media_nominal",
    ]
    keep_columns = [column for column in keep_columns if column in frame.columns]
    return frame[keep_columns].copy()


def read_caged(path: Path, keys: list[str]) -> pd.DataFrame:
    """Load and normalize a Novo CAGED aggregate CSV."""
    frame = pd.read_csv(path, dtype="string")
    required = set(keys + ["competencia_mov", "saldo", "admitidos", "demitidos"])
    require_columns(frame.columns, required, "Novo CAGED CSV")

    for key in keys:
        frame[key] = normalize_code_series(frame[key], 6 if key == "municipio" else None)
    frame["competencia_mov"] = normalize_code_series(frame["competencia_mov"], 6)
    frame["ano_caged"] = frame["competencia_mov"].str.slice(0, 4)
    frame["mes_caged"] = frame["competencia_mov"].str.slice(4, 6)

    frame = frame.rename(
        columns={
            "admitidos": "admitidos_novo_caged",
            "demitidos": "demitidos_novo_caged",
        }
    )
    frame = coerce_numeric_columns(frame, ["saldo", "admitidos_novo_caged", "demitidos_novo_caged"], fill_value=0)

    grouped = (
        frame.groupby(keys + ["competencia_mov", "ano_caged", "mes_caged"], dropna=False, as_index=False)
        .agg(
            {
                "saldo": "sum",
                "admitidos_novo_caged": "sum",
                "demitidos_novo_caged": "sum",
            }
        )
        .copy()
    )
    grouped["ano_caged"] = grouped["ano_caged"].astype("string").astype("Int64")
    grouped["ano_rais"] = grouped["ano_caged"] - 1
    return grouped


def build_month_grid(rais: pd.DataFrame, caged: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Cross-join unique RAIS keys with all CAGED months that occur after the first RAIS baseline."""
    first_caged_year = int(rais["ano_rais"].min()) + 1
    months = caged.loc[caged["ano_caged"] >= first_caged_year, ["ano_caged", "competencia_mov", "mes_caged"]]
    months = months.drop_duplicates().sort_values("competencia_mov").reset_index(drop=True)
    if months.empty:
        raise ValueError("No Novo CAGED months occur after the first available RAIS base year.")

    key_grid = rais[keys].drop_duplicates().reset_index(drop=True)

    # Vectorised cross join via constant key
    key_grid = key_grid.copy()
    key_grid["_tmp_join_key"] = 1
    months = months.copy()
    months["_tmp_join_key"] = 1
    joined = key_grid.merge(months, on="_tmp_join_key", how="inner").drop(columns="_tmp_join_key")
    return joined


def add_stock_evolution(monthly: pd.DataFrame, rais: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Calculate monthly stock evolution using RAIS baselines, fully vectorised.

    Rules (preserved from original implementation):
    - Stock resets to RAIS ``estoque_3112`` at the first CAGED month of each year
      when a matching RAIS baseline exists.
    - If no RAIS exists for the previous year, stock continues from the previous
      month's ``estoque_fim`` (cumulative saldo).
    - ``estoque_fim = estoque_inicio + saldo``.
    """
    # Merge RAIS baseline into monthly grid on keys + ano_caged
    merged = monthly.merge(
        rais[keys + ["ano_caged", "estoque_3112"]],
        on=keys + ["ano_caged"],
        how="left",
    )
    merged = merged.sort_values(keys + ["competencia_mov"]).reset_index(drop=True)

    # Detect year changes within each key group
    merged["_prev_ano_caged"] = merged.groupby(keys)["ano_caged"].shift(1)
    merged["estoque_reiniciado_por_rais"] = (
        (merged["ano_caged"] != merged["_prev_ano_caged"].fillna(-1)) & merged["estoque_3112"].notna()
    )
    merged["estoque_reiniciado_por_rais"] = merged["estoque_reiniciado_por_rais"].fillna(False)

    # Track whether a reset has occurred at or before each row within the group
    merged["_has_had_reset"] = merged.groupby(keys)["estoque_reiniciado_por_rais"].cummax()

    # Discard months before the first usable RAIS baseline
    merged = merged[merged["_has_had_reset"]].copy()
    if merged.empty:
        raise ValueError("Could not calculate stock evolution from the RAIS and Novo CAGED inputs.")

    # Build reset groups: every True increments the group id within each key group
    merged["_reset_group"] = merged.groupby(keys)["estoque_reiniciado_por_rais"].cumsum()

    # Within each reset group, the baseline is the first (non-null) RAIS stock
    merged["_baseline"] = merged.groupby(keys + ["_reset_group"])["estoque_3112"].transform("first")

    # Cumulative saldo within each reset group
    merged["_saldo_acum"] = merged.groupby(keys + ["_reset_group"])["saldo"].cumsum()

    # Final stock metrics
    merged["estoque_fim"] = merged["_baseline"] + merged["_saldo_acum"]
    merged["estoque_inicio"] = merged["estoque_fim"] - merged["saldo"]
    merged["saldo_acumulado_desde_rais"] = merged["estoque_fim"] - merged["_baseline"]

    # Annual cumulative saldo (within each key + year)
    merged["saldo_acumulado_ano"] = merged.groupby(keys + ["ano_caged"], dropna=False)["saldo"].cumsum()

    # Clean up intermediate columns
    merged = merged.drop(
        columns=["_prev_ano_caged", "_has_had_reset", "_reset_group", "_baseline", "_saldo_acum"],
        errors="ignore",
    )
    return merged.sort_values(keys + ["competencia_mov"]).reset_index(drop=True)


def integrate(rais_csv: Path, caged_csv: Path, keys: list[str]) -> pd.DataFrame:
    """Build monthly stock evolution from RAIS baseline and CAGED monthly saldo."""
    rais = read_rais(rais_csv, keys)
    caged = read_caged(caged_csv, keys)
    grid = build_month_grid(rais, caged, keys)

    merged = grid.merge(
        caged[keys + ["competencia_mov", "saldo", "admitidos_novo_caged", "demitidos_novo_caged"]],
        on=keys + ["competencia_mov"],
        how="left",
    )
    merged[["saldo", "admitidos_novo_caged", "demitidos_novo_caged"]] = merged[
        ["saldo", "admitidos_novo_caged", "demitidos_novo_caged"]
    ].fillna(0)
    merged = add_stock_evolution(merged, rais, keys)

    # Bring in RAIS annual columns (they are merged via keys + ano_caged in add_stock_evolution)
    # but we need to merge the remaining RAIS columns not yet present
    rais_cols_to_merge = [column for column in rais.columns if column not in keys + ["ano_caged", "estoque_3112"]]
    if rais_cols_to_merge:
        merged = merged.merge(rais[keys + ["ano_caged"] + rais_cols_to_merge], on=keys + ["ano_caged"], how="left")

    preferred = [
        "competencia_mov",
        "ano_caged",
        "mes_caged",
        *keys,
        "ano_rais",
        "estoque_3112",
        "estoque_reiniciado_por_rais",
        "estoque_inicio",
        "saldo",
        "estoque_fim",
        "saldo_acumulado_ano",
        "saldo_acumulado_desde_rais",
        "admitidos_novo_caged",
        "demitidos_novo_caged",
        "admitidos_rais_ano",
        "desligados_rais_ano",
        "vinculos_rais_ano",
        "mean_remuneracao_media_nominal",
    ]
    output_columns = [column for column in preferred if column in merged.columns]
    output_columns.extend(column for column in merged.columns if column not in output_columns)
    return merged[output_columns]
