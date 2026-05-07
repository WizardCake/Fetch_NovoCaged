"""Tests for pdet.common helpers."""

import pytest
from pdet.common import (
    sanitize_column_name,
    make_unique_columns,
    normalize_code_series,
    normalize_uf_series,
    repair_mojibake,
    detect_delimiter,
    select_period_range,
    UF_CODE_TO_ACRONYM,
)


class TestSanitizeColumnName:
    def test_accented_portuguese(self):
        assert sanitize_column_name("Município") == "municipio"
        assert sanitize_column_name("Competência Mov") == "competencia_mov"

    def test_mojibake(self):
        # Simulating double-encoded UTF-8
        assert sanitize_column_name("Munic\u00c3\u00adpio") == "municipio"

    def test_31_12(self):
        assert sanitize_column_name("Vínculo Ativo 31/12") == "vinculo_ativo_31_12"

    def test_empty_fallback(self):
        assert sanitize_column_name("!!!") == "column"


class TestMakeUniqueColumns:
    def test_duplicate_appends_counter(self):
        assert make_unique_columns(["a", "a", "a"]) == ["a", "a_1", "a_2"]

    def test_unique_unchanged(self):
        assert make_unique_columns(["a", "b", "c"]) == ["a", "b", "c"]


class TestNormalizeCodeSeries:
    def test_strip_and_zfill(self):
        import pandas as pd
        series = pd.Series([" 330455 ", "1234", "1234.0", ""])
        result = normalize_code_series(series, width=6)
        assert result.iloc[0] == "330455"
        assert result.iloc[1] == "001234"
        assert result.iloc[2] == "001234"
        assert pd.isna(result.iloc[3])


class TestNormalizeUFSeries:
    def test_acronym_to_code(self):
        import pandas as pd
        series = pd.Series(["RJ", "sp", " 33 "])
        result = normalize_uf_series(series)
        assert list(result) == ["33", "35", "33"]


class TestRepairMojibake:
    def test_no_change(self):
        assert repair_mojibake("normal") == "normal"

    def test_double_encoded(self):
        # á encoded as UTF-8 then interpreted as latin-1
        double = "á".encode("utf-8").decode("latin-1")
        assert repair_mojibake(double) == "á"


class TestDetectDelimiter:
    def test_semicolon(self, tmp_path):
        path = tmp_path / "test.csv"
        path.write_text("a;b;c\n", encoding="utf-8")
        assert detect_delimiter(path, "utf-8") == ";"

    def test_comma(self, tmp_path):
        path = tmp_path / "test.csv"
        path.write_text("a,b,c\n", encoding="utf-8")
        assert detect_delimiter(path, "utf-8") == ","


class TestSelectPeriodRange:
    def test_filter_range(self):
        periods = ["202301", "202302", "202303", "202304"]
        assert select_period_range(periods, "202302", "202303") == ["202302", "202303"]

    def test_none_bounds(self):
        periods = ["202301", "202302"]
        assert select_period_range(periods, None, None) == periods
