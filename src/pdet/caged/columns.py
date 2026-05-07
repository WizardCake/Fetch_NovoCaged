"""Column definitions and normalization helpers for Novo CAGED."""

from __future__ import annotations

COLUMN_RENAMES = {
    "competênciamov": "competencia_mov",
    "região": "regiao",
    "município": "municipio",
    "seção": "secao",
    "saldomovimentação": "saldo_movimentacao",
    "cbo2002ocupação": "cbo2002_ocupacao",
    "graudeinstrução": "grau_de_instrucao",
    "raçacor": "raca_cor",
    "tipomovimentação": "tipomovimentacao",
    "tipodedeficiência": "tipodedeficiencia",
    "salário": "salario",
    "origemdainformação": "origem_da_informacao",
    "competênciadec": "competencia_dec",
    "competênciaexc": "competencia_exc",
    "indicadordeexclusão": "indicador_de_exclusao",
    "indicadordeforadoprazo": "indicador_de_fora_do_prazo",
    "unidadesaláriocódigo": "unidade_salario_codigo",
    "valorsaláriofixo": "valor_salario_fixo",
}

ALL_COLUMNS = [
    "competencia_mov",
    "regiao",
    "uf",
    "municipio",
    "secao",
    "subclasse",
    "saldo_movimentacao",
    "cbo2002_ocupacao",
    "categoria",
    "grau_de_instrucao",
    "idade",
    "horascontratuais",
    "raca_cor",
    "sexo",
    "tipoempregador",
    "tipoestabelecimento",
    "tipomovimentacao",
    "tipodedeficiencia",
    "indtrabintermitente",
    "indtrabparcial",
    "salario",
    "tamestabjan",
    "indicadoraprendiz",
    "origem_da_informacao",
    "competencia_dec",
    "competencia_exc",
    "indicador_de_exclusao",
    "indicador_de_fora_do_prazo",
    "unidade_salario_codigo",
    "valor_salario_fixo",
    "source_competencia",
    "source_kind",
]

STRING_COLUMNS = [
    "competencia_mov",
    "regiao",
    "uf",
    "municipio",
    "secao",
    "subclasse",
    "cbo2002_ocupacao",
    "categoria",
    "grau_de_instrucao",
    "raca_cor",
    "sexo",
    "tipoempregador",
    "tipoestabelecimento",
    "tipomovimentacao",
    "tipodedeficiencia",
    "indtrabintermitente",
    "indtrabparcial",
    "tamestabjan",
    "indicadoraprendiz",
    "origem_da_informacao",
    "competencia_dec",
    "competencia_exc",
    "indicador_de_exclusao",
    "indicador_de_fora_do_prazo",
    "unidade_salario_codigo",
    "source_competencia",
    "source_kind",
]

INTEGER_COLUMNS = ["saldo_movimentacao", "idade"]
DECIMAL_COLUMNS = ["horascontratuais", "salario", "valor_salario_fixo"]
NUMERIC_COLUMNS = INTEGER_COLUMNS + DECIMAL_COLUMNS

ADMISSION_MOVEMENT_CODES = {"10", "20", "25", "35", "70", "97"}
DISMISSAL_MOVEMENT_CODES = {"31", "32", "33", "40", "43", "45", "50", "60", "80", "90", "98"}

BUILTIN_MEASURES = ("saldo", "admitidos", "demitidos", "estoque", "movimentacoes")
