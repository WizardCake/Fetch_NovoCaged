"""Tests for RAIS + Novo CAGED integration."""

import pandas as pd
import pytest

from pdet.integra.core import read_rais, read_caged, build_month_grid, add_stock_evolution, integrate


class TestReadRais:
    def test_basic(self, tmp_path):
        csv = tmp_path / "rais.csv"
        csv.write_text(
            "ano,municipio,estoque_3112,admitidos_ano,desligados_ano\n"
            "2024,330455,1000,50,30\n",
            encoding="utf-8",
        )
        result = read_rais(csv, ["municipio"])
        assert result["ano_caged"].iloc[0] == 2025
        assert result["estoque_3112"].iloc[0] == 1000.0
        assert result["admitidos_rais_ano"].iloc[0] == 50.0

    def test_duplicate_keys_raises(self, tmp_path):
        csv = tmp_path / "rais.csv"
        csv.write_text(
            "ano,municipio,estoque_3112\n"
            "2024,330455,1000\n"
            "2024,330455,1100\n",
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="duplicated key/year"):
            read_rais(csv, ["municipio"])


class TestReadCaged:
    def test_basic(self, tmp_path):
        csv = tmp_path / "caged.csv"
        csv.write_text(
            "competencia_mov,municipio,saldo,admitidos,demitidos\n"
            "202501,330455,10,15,5\n"
            "202502,330455,-2,8,10\n",
            encoding="utf-8",
        )
        result = read_caged(csv, ["municipio"])
        assert result["ano_caged"].iloc[0] == 2025
        assert result["saldo"].iloc[0] == 10.0
        assert result["admitidos_novo_caged"].iloc[0] == 15.0

    def test_grouping(self, tmp_path):
        csv = tmp_path / "caged.csv"
        csv.write_text(
            "competencia_mov,municipio,saldo,admitidos,demitidos\n"
            "202501,330455,10,5,5\n"
            "202501,330455,5,3,2\n",
            encoding="utf-8",
        )
        result = read_caged(csv, ["municipio"])
        assert len(result) == 1
        assert result["saldo"].iloc[0] == 15.0


class TestBuildMonthGrid:
    def test_cross_join(self):
        rais = pd.DataFrame({
            "municipio": ["330455", "330170"],
            "ano_rais": [2024, 2024],
            "ano_caged": [2025, 2025],
            "estoque_3112": [1000, 500],
        })
        caged = pd.DataFrame({
            "competencia_mov": ["202501", "202502", "202503"],
            "ano_caged": [2025, 2025, 2025],
            "mes_caged": ["01", "02", "03"],
            "municipio": ["330455", "330455", "330455"],
            "saldo": [10, -5, 3],
        })
        grid = build_month_grid(rais, caged, ["municipio"])
        assert len(grid) == 6  # 2 municipios * 3 meses
        assert set(grid["municipio"]) == {"330455", "330170"}

    def test_no_months_after_baseline(self):
        rais = pd.DataFrame({
            "municipio": ["330455"],
            "ano_rais": [2025],
            "ano_caged": [2026],
            "estoque_3112": [1000],
        })
        caged = pd.DataFrame({
            "competencia_mov": ["202501"],
            "ano_caged": [2025],
            "mes_caged": ["01"],
            "municipio": ["330455"],
            "saldo": [10],
        })
        with pytest.raises(ValueError, match="No Novo CAGED months occur after"):
            build_month_grid(rais, caged, ["municipio"])


class TestAddStockEvolution:
    def test_cumulative_stock(self):
        monthly = pd.DataFrame({
            "municipio": ["330455", "330455", "330455"],
            "competencia_mov": ["202501", "202502", "202503"],
            "ano_caged": [2025, 2025, 2025],
            "mes_caged": ["01", "02", "03"],
            "saldo": [10, -5, 3],
            "admitidos_novo_caged": [15, 8, 10],
            "demitidos_novo_caged": [5, 13, 7],
        })
        rais = pd.DataFrame({
            "municipio": ["330455"],
            "ano_caged": [2025],
            "estoque_3112": [1000],
        })
        result = add_stock_evolution(monthly, rais, ["municipio"])
        assert list(result["estoque_inicio"]) == [1000, 1010, 1005]
        assert list(result["estoque_fim"]) == [1010, 1005, 1008]
        assert list(result["saldo_acumulado_ano"]) == [10, 5, 8]
        assert list(result["saldo_acumulado_desde_rais"]) == [10, 5, 8]

    def test_year_reset(self):
        monthly = pd.DataFrame({
            "municipio": ["330455", "330455", "330455", "330455"],
            "competencia_mov": ["202512", "202601", "202602", "202603"],
            "ano_caged": [2025, 2026, 2026, 2026],
            "mes_caged": ["12", "01", "02", "03"],
            "saldo": [10, 5, -3, 2],
            "admitidos_novo_caged": [0, 0, 0, 0],
            "demitidos_novo_caged": [0, 0, 0, 0],
        })
        rais = pd.DataFrame({
            "municipio": ["330455", "330455"],
            "ano_caged": [2025, 2026],
            "estoque_3112": [1000, 1100],
        })
        result = add_stock_evolution(monthly, rais, ["municipio"])
        # Dec 2025: estoque = 1000 + 10 = 1010
        # Jan 2026: reset to RAIS 2025 estoque = 1100, then +5 = 1105
        # Feb 2026: 1105 -3 = 1102
        assert list(result["estoque_fim"]) == [1010, 1105, 1102, 1104]
        # Verify reset flag: first month always resets if a matching RAIS baseline exists
        assert list(result["estoque_reiniciado_por_rais"]) == [True, True, False, False]

    def test_missing_baseline_skips_early_months(self):
        # RAIS only available for 2025, CAGED starts in 2024
        monthly = pd.DataFrame({
            "municipio": ["330455", "330455"],
            "competencia_mov": ["202401", "202402"],
            "ano_caged": [2024, 2024],
            "mes_caged": ["01", "02"],
            "saldo": [10, 5],
            "admitidos_novo_caged": [0, 0],
            "demitidos_novo_caged": [0, 0],
        })
        rais = pd.DataFrame({
            "municipio": ["330455"],
            "ano_caged": [2025],
            "estoque_3112": [1000],
        })
        with pytest.raises(ValueError, match="Could not calculate stock evolution"):
            add_stock_evolution(monthly, rais, ["municipio"])

    def test_continues_without_baseline(self):
        # RAIS 2024, CAGED 2025 + 2026 but no RAIS 2025
        monthly = pd.DataFrame({
            "municipio": ["330455", "330455", "330455", "330455"],
            "competencia_mov": ["202501", "202502", "202601", "202602"],
            "ano_caged": [2025, 2025, 2026, 2026],
            "mes_caged": ["01", "02", "01", "02"],
            "saldo": [10, 5, -3, 2],
            "admitidos_novo_caged": [0, 0, 0, 0],
            "demitidos_novo_caged": [0, 0, 0, 0],
        })
        rais = pd.DataFrame({
            "municipio": ["330455"],
            "ano_caged": [2025],
            "estoque_3112": [1000],
        })
        result = add_stock_evolution(monthly, rais, ["municipio"])
        # 2025: starts at 1000, ends at 1015
        # 2026: no RAIS 2025 baseline, continues from 1015
        assert list(result["estoque_fim"]) == [1010, 1015, 1012, 1014]
        # Only Jan 2025 should be a reset (first month with baseline)
        resets = list(result["estoque_reiniciado_por_rais"])
        assert resets[0] is True
        assert resets[1] is False
        assert resets[2] is False
        assert resets[3] is False


class TestIntegrate:
    def test_end_to_end(self, tmp_path):
        rais_csv = tmp_path / "rais.csv"
        rais_csv.write_text(
            "ano,municipio,estoque_3112,admitidos_ano,desligados_ano\n"
            "2024,330455,1000,50,30\n",
            encoding="utf-8",
        )
        caged_csv = tmp_path / "caged.csv"
        caged_csv.write_text(
            "competencia_mov,municipio,saldo,admitidos,demitidos\n"
            "202501,330455,10,15,5\n"
            "202502,330455,-5,8,13\n",
            encoding="utf-8",
        )
        result = integrate(rais_csv, caged_csv, ["municipio"])
        assert len(result) == 2
        assert list(result["estoque_fim"]) == [1010, 1005]
        assert list(result["estoque_reiniciado_por_rais"]) == [True, False]
        assert "admitidos_rais_ano" in result.columns
        assert "desligados_rais_ano" in result.columns
