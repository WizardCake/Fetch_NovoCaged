"""Tests for RAIS column normalization and aggregation."""

import pandas as pd
import pytest

from pdet.rais.columns import CANONICAL_RENAMES, STANDARD_COLUMNS, BUILTIN_MEASURES
from pdet.rais.convert import (
    canonical_column,
    normalize_columns,
    add_source_and_derived_columns,
    coerce_known_types,
    find_or_extract_txt,
    run_convert,
)
from pdet.rais.aggregate import active_3112_mask, month_present_mask, add_metric_columns
from pdet.aggregation import parse_filters, parse_measures


class TestRaisColumns:
    def test_canonical_rename(self):
        assert CANONICAL_RENAMES["vl_remun_media_nom"] == "remuneracao_media_nominal"
        assert CANONICAL_RENAMES["municipio_codigo"] == "municipio"

    def test_standard_columns_present(self):
        assert "ano" in STANDARD_COLUMNS
        assert "uf" in STANDARD_COLUMNS
        assert "remuneracao_media_nominal" in STANDARD_COLUMNS


class TestCanonicalColumn:
    def test_variants(self):
        assert canonical_column("vl_remun_media_nom") == "remuneracao_media_nominal"
        assert canonical_column("  VL_REMUN_MEDIA_NOM  ") == "remuneracao_media_nominal"


class TestNormalizeColumns:
    def test_rename_and_dedup(self):
        df = pd.DataFrame({
            "Ano": ["2024"],
            "Município": ["330455"],
            "vl_remun_media_nom": ["3000,00"],
        })
        result = normalize_columns(df)
        assert "ano" in result.columns
        assert "municipio" in result.columns
        assert "remuneracao_media_nominal" in result.columns


class TestAddSourceAndDerivedColumns:
    def test_uf_from_municipio(self):
        df = pd.DataFrame({"municipio": ["330455"]})
        result = add_source_and_derived_columns(df, "2024", "VINC", "test.7z")
        assert result["uf"].iloc[0] == "33"
        assert result["source_year"].iloc[0] == "2024"

    def test_existing_uf_preserved(self):
        df = pd.DataFrame({"municipio": ["330455"], "uf": ["35"]})
        result = add_source_and_derived_columns(df, "2024", "VINC", "test.7z")
        assert result["uf"].iloc[0] == "35"


class TestCoerceKnownTypes:
    def test_numeric_coercion(self):
        df = pd.DataFrame({
            "idade": ["25", "30"],
            "remuneracao_media_nominal": ["3000,00", "2500,50"],
        })
        result = coerce_known_types(df)
        assert result["idade"].dtype.name == "Int64"
        assert result["remuneracao_media_nominal"].iloc[0] == 3000.0

    def test_code_padding(self):
        df = pd.DataFrame({
            "municipio": ["330455", "1234"],
            "mes_admissao": ["1", "12"],
        })
        result = coerce_known_types(df)
        assert result["municipio"].iloc[1] == "001234"
        assert result["mes_admissao"].iloc[0] == "01"


class TestActive3112Mask:
    def test_active_flag(self):
        df = pd.DataFrame({"vinculo_ativo_31_12": ["1", "0", "S", "N"]})
        mask = active_3112_mask(df)
        assert list(mask) == [True, False, True, False]

    def test_desligamento_fallback(self):
        df = pd.DataFrame({"mes_desligamento": ["", "00", "05", "99"]})
        mask = active_3112_mask(df)
        assert list(mask) == [True, True, False, True]


class TestMonthPresentMask:
    def test_presence(self):
        df = pd.DataFrame({"mes_admissao": ["01", "", "00", "05"]})
        mask = month_present_mask(df, "mes_admissao")
        assert list(mask) == [True, False, False, True]

    def test_missing_column(self):
        df = pd.DataFrame({"other": ["x"]})
        mask = month_present_mask(df, "mes_admissao")
        assert list(mask) == [False]


class TestAddMetricColumnsRais:
    def test_estoque_3112(self):
        df = pd.DataFrame({"vinculo_ativo_31_12": ["1", "0", "1"]})
        measures = [("estoque_3112", None, "estoque_3112")]
        result = add_metric_columns(df, measures)
        assert result["_estoque_3112"].sum() == 2

    def test_admitidos_ano(self):
        df = pd.DataFrame({"mes_admissao": ["01", "", "05"]})
        measures = [("admitidos_ano", None, "admitidos_ano")]
        result = add_metric_columns(df, measures)
        assert result["_admitidos_ano"].sum() == 2

    def test_mean_measure(self):
        df = pd.DataFrame({"remuneracao_media_nominal": ["3000", "4000", ""]})
        measures = [("mean", "remuneracao_media_nominal", "mean_remuneracao_media_nominal")]
        result = add_metric_columns(df, measures)
        assert "_mean_remuneracao_media_nominal_sum" in result.columns
        assert "_mean_remuneracao_media_nominal_count" in result.columns
        assert result["_mean_remuneracao_media_nominal_count"].sum() == 2


class TestFindOrExtractTxtRais:
    def test_returns_existing_txts(self, tmp_path):
        extracted = tmp_path / "extracted" / "2024" / "RAIS_VINC"
        extracted.mkdir(parents=True)
        txt = extracted / "RAIS_VINC.txt"
        txt.write_text("Ano;Municipio\n2024;330455\n", encoding="utf-8")
        result = find_or_extract_txt(tmp_path, overwrite=False)
        assert len(result) == 1
        assert result[0] == txt

    def test_raises_when_no_raw(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="No raw"):
            find_or_extract_txt(tmp_path, overwrite=False)


class TestRunConvertRais:
    def test_converts_existing_txts(self, tmp_path):
        extracted = tmp_path / "extracted" / "2024" / "RAIS_VINC"
        extracted.mkdir(parents=True)
        txt = extracted / "RAIS_VINC.txt"
        txt.write_text("Ano;Municipio\n2024;330455\n", encoding="utf-8")
        run_convert(tmp_path, "parquet", 1000, True, {}, "latin-1", False)
        parquet_files = list((tmp_path / "parquet").rglob("*.parquet"))
        assert len(parquet_files) == 1

    def test_cleanup_removes_txts(self, tmp_path):
        extracted = tmp_path / "extracted" / "2024" / "RAIS_VINC"
        extracted.mkdir(parents=True)
        txt = extracted / "RAIS_VINC.txt"
        txt.write_text("Ano;Municipio\n2024;330455\n", encoding="utf-8")
        run_convert(tmp_path, "parquet", 1000, True, {}, "latin-1", True)
        assert not txt.exists()
