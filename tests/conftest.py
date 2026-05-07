"""Shared test fixtures."""

import pytest


@pytest.fixture
def sample_caged_df():
    import pandas as pd
    return pd.DataFrame({
        "competênciamov": ["202401", "202401", "202402"],
        "uf": ["33", "33", "33"],
        "município": ["330455", "330455", "330455"],
        "saldomovimentação": ["1", "-1", "1"],
        "tipomovimentação": ["10", "31", "20"],
        "salário": ["1500,00", "2000,00", "1800,50"],
    })


@pytest.fixture
def sample_rais_df():
    import pandas as pd
    return pd.DataFrame({
        "Ano": ["2024", "2024"],
        "uf": ["33", "33"],
        "municipio": ["330455", "330455"],
        "vinculo_ativo_31_12": ["1", "1"],
        "vl_remun_media_nom": ["3000,00", "2500,00"],
    })
