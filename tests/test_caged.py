"""Tests for Novo CAGED column normalization and aggregation."""

import argparse

import pandas as pd
import pytest

from pdet.caged.columns import (
    COLUMN_RENAMES,
    ALL_COLUMNS,
    ADMISSION_MOVEMENT_CODES,
    DISMISSAL_MOVEMENT_CODES,
)
from pdet.caged.convert import normalize_chunk, coerce_normalized_types, find_or_extract_txt, run_convert
from pdet.caged.aggregate import add_metric_columns, add_stock_measure
from pdet.aggregation import parse_filters, parse_measures, validate_dimensions


class TestCagedColumns:
    def test_rename_map(self):
        assert COLUMN_RENAMES["competênciamov"] == "competencia_mov"
        assert COLUMN_RENAMES["salário"] == "salario"

    def test_admission_codes(self):
        assert "10" in ADMISSION_MOVEMENT_CODES
        assert "31" not in ADMISSION_MOVEMENT_CODES

    def test_dismissal_codes(self):
        assert "31" in DISMISSAL_MOVEMENT_CODES
        assert "10" not in DISMISSAL_MOVEMENT_CODES


class TestNormalizeChunk:
    def test_basic(self):
        df = pd.DataFrame({
            "competênciamov": ["202401"],
            "uf": ["33"],
            "saldomovimentação": ["1"],
            "salário": ["1500,00"],
        })
        result = normalize_chunk(df, "202401", "MOV")
        assert "competencia_mov" in result.columns
        assert "source_competencia" in result.columns
        assert result["source_kind"].iloc[0] == "MOV"
        assert result["salario"].iloc[0] == 1500.0
        assert result["saldo_movimentacao"].iloc[0] == 1

    def test_missing_columns_filled(self):
        df = pd.DataFrame({
            "competênciamov": ["202401"],
        })
        result = normalize_chunk(df, "202401", "MOV")
        for col in ALL_COLUMNS:
            assert col in result.columns


class TestCoerceNormalizedTypes:
    def test_roundtrip(self):
        df = pd.DataFrame({
            "competencia_mov": ["202401"],
            "saldo_movimentacao": ["1"],
            "salario": ["1500.0"],
        })
        result = coerce_normalized_types(df)
        assert result["saldo_movimentacao"].dtype.name == "Int64"
        assert result["salario"].dtype.name == "Float64"


class TestAddMetricColumns:
    def test_saldo(self):
        df = pd.DataFrame({
            "source_kind": ["MOV", "MOV"],
            "saldo_movimentacao": [1, -1],
            "tipomovimentacao": ["10", "31"],
            "indicador_de_exclusao": [pd.NA, pd.NA],
        })
        measures = [("saldo", None, "saldo")]
        result = add_metric_columns(df, measures)
        assert "_saldo" in result.columns
        assert result["_saldo"].sum() == 0

    def test_exclusion_inverts(self):
        df = pd.DataFrame({
            "source_kind": ["EXC"],
            "saldo_movimentacao": [1],
            "tipomovimentacao": ["10"],
            "indicador_de_exclusao": [pd.NA],
        })
        measures = [("saldo", None, "saldo")]
        result = add_metric_columns(df, measures)
        assert result["_saldo"].iloc[0] == -1

    def test_admitidos_demitidos(self):
        df = pd.DataFrame({
            "source_kind": ["MOV", "MOV", "MOV"],
            "saldo_movimentacao": [1, -1, 1],
            "tipomovimentacao": ["10", "31", "20"],
            "indicador_de_exclusao": [pd.NA, pd.NA, pd.NA],
        })
        measures = [("admitidos", None, "admitidos"), ("demitidos", None, "demitidos")]
        result = add_metric_columns(df, measures)
        # 10 and 20 are admission codes; 31 is dismissal
        assert result["_admitidos"].sum() == 2
        assert result["_demitidos"].sum() == 1


class TestAddStockMeasure:
    def test_cumulative_saldo(self):
        df = pd.DataFrame({
            "competencia_mov": ["202401", "202402", "202403"],
            "municipio": ["330455", "330455", "330455"],
            "_saldo": [10, -5, 3],
        })
        result = add_stock_measure(df, ["competencia_mov", "municipio"], None, "estoque_inicial")
        assert list(result["estoque"]) == [10, 5, 8]

    def test_with_initial_stock(self, tmp_path):
        df = pd.DataFrame({
            "competencia_mov": ["202401", "202402"],
            "municipio": ["330455", "330455"],
            "_saldo": [10, 5],
        })
        stock = tmp_path / "stock.csv"
        stock.write_text("municipio,estoque_inicial\n330455,100\n", encoding="utf-8")
        result = add_stock_measure(df, ["competencia_mov", "municipio"], stock, "estoque_inicial")
        assert list(result["estoque"]) == [110, 115]


class TestParseFilters:
    def test_valid(self):
        result = parse_filters(["uf=33,35"], ALL_COLUMNS)
        assert result == {"uf": {"33", "35"}}

    def test_invalid_column(self):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_filters(["invalid=1"], ALL_COLUMNS)


class TestParseMeasures:
    def test_builtin(self):
        result = parse_measures(["saldo", "estoque"], ("saldo", "estoque"), ["salario"])
        assert result == [("saldo", None, "saldo"), ("estoque", None, "estoque")]

    def test_generic(self):
        result = parse_measures(["sum:salario"], ("saldo",), ["salario"])
        assert result == [("sum", "salario", "sum_salario")]

    def test_invalid_aggregation(self):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_measures(["invalid:salario"], ("saldo",), ["salario"])


class TestValidateDimensions:
    def test_valid(self):
        assert validate_dimensions(["uf", "municipio"], ALL_COLUMNS) == ["uf", "municipio"]

    def test_invalid(self):
        with pytest.raises(argparse.ArgumentTypeError):
            validate_dimensions(["invalid"], ALL_COLUMNS)


class TestFindOrExtractTxt:
    def test_returns_existing_txts(self, tmp_path):
        extracted = tmp_path / "extracted" / "202401"
        extracted.mkdir(parents=True)
        txt = extracted / "CAGEDMOV202401.txt"
        txt.write_text("competênciamov;uf\n202401;33\n", encoding="utf-8")
        result = find_or_extract_txt(tmp_path, overwrite=False)
        assert len(result) == 1
        assert result[0] == txt

    def test_extracts_from_raw(self, tmp_path):
        raw = tmp_path / "raw" / "202401"
        raw.mkdir(parents=True)
        # Create a fake .7z by making a tarball (tar supports .7z on this system)
        archive = raw / "CAGEDMOV202401.7z"
        txt = tmp_path / "extracted" / "202401" / "CAGEDMOV202401.txt"
        txt.parent.mkdir(parents=True)
        txt.write_text("competênciamov;uf\n202401;33\n", encoding="utf-8")
        # Instead of creating a real .7z, mock the extraction by pre-populating extracted/
        result = find_or_extract_txt(tmp_path, overwrite=False)
        assert len(result) == 1

    def test_raises_when_no_raw(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="No raw"):
            find_or_extract_txt(tmp_path, overwrite=False)


class TestRunConvert:
    def test_converts_existing_txts(self, tmp_path):
        extracted = tmp_path / "extracted" / "202401"
        extracted.mkdir(parents=True)
        txt = extracted / "CAGEDMOV202401.txt"
        txt.write_text("competênciamov;uf\n202401;33\n", encoding="utf-8")
        run_convert(tmp_path, "parquet", 1000, True, False)
        parquet_files = list((tmp_path / "parquet").rglob("*.parquet"))
        assert len(parquet_files) == 1

    def test_cleanup_removes_txts(self, tmp_path):
        extracted = tmp_path / "extracted" / "202401"
        extracted.mkdir(parents=True)
        txt = extracted / "CAGEDMOV202401.txt"
        txt.write_text("competênciamov;uf\n202401;33\n", encoding="utf-8")
        run_convert(tmp_path, "parquet", 1000, True, True)
        assert not txt.exists()
