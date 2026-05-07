# Novo CAGED and RAIS Scraper and Wrangling Notes

This project downloads and standardizes the public Novo CAGED microdata from:

`ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/`

The official MTE page says the microdata are TXT files using `;` as delimiter and UTF-8 encoding for Novo CAGED. The FTP folder also provides the layout workbook used below.

## Installation

Install the package in editable mode (creates the `novo-caged`, `rais`, and `integra-rais-caged` CLI entry points):

```powershell
python -m pip install -e ".[dev]"
```

Run tests with:

```powershell
python -m pytest
```

## Project Structure

The code is organised as an installable Python package under `src/pdet/`:

- `src/pdet/common.py`: shared FTP helpers, column sanitisation, code normalisation, retry logic, and logging.
- `src/pdet/aggregation.py`: reusable aggregation pipeline (parse filters/measures, chunk aggregation, combine, and finalise).
- `src/pdet/caged/`: Novo CAGED modules — `ftp.py`, `extract.py`, `convert.py`, `aggregate.py`, `cli.py`.
- `src/pdet/rais/`: RAIS modules — `ftp.py`, `extract.py`, `convert.py`, `aggregate.py`, `cli.py`.
- `src/pdet/integra/`: RAIS + CAGED integration modules — `core.py`, `cli.py`.
- `novo_caged.py`, `rais.py`, `integra_rais_caged.py`: backward-compatible wrappers that call the new package.

Legacy `pdet_common.py` has been refactored into `src/pdet/common.py` with full type hints, `logging` instead of `print()`, and FTP retry with exponential backoff.

## Source Structure

Top-level files inspected:

- `Leia-me.txt`
- `Layout Não-identificado Novo Caged Movimentação.xlsx`
- `Sobre o Novo Caged.pdf`
- `Comunicado - Grupamento de Atividades Econômicas.pdf`
- Year folders: `2020` through `2026`

Each monthly folder is named `AAAAMM`, for example `2026/202603/`.

Archive types:

- `CAGEDMOVAAAAMM.7z`: movements declared within the deadline for declaration competence `AAAAMM`.
- `CAGEDFORAAAAMM.7z`: late declarations with declaration competence `AAAAMM`.
- `CAGEDEXCAAAAMM.7z`: exclusions with exclusion declaration competence `AAAAMM`.

Early months may not contain all three archive types. For example, `202001` currently has only `CAGEDMOV202001.7z`.

## Commands

All examples below use the backward-compatible wrapper files (`python novo_caged.py …`).
If you installed the package with `pip install -e .`, you can also call the entry points directly:
`novo-caged`, `rais`, and `integra-rais-caged`.

List available months and files:

```powershell
python novo_caged.py list --start 202603 --end 202603
```

Download one release month:

```powershell
python novo_caged.py download --start 202603 --end 202603 --kinds MOV FOR EXC
```

Extract downloaded `.7z` archives:

```powershell
python novo_caged.py extract
```

Convert extracted TXT to partitioned Parquet:

```powershell
python novo_caged.py convert --output-format parquet
```

If Parquet dependencies are not installed, convert to compressed CSV:

```powershell
python novo_caged.py convert --output-format csv
```

Aggregate standardized data to a final CSV:

```powershell
python novo_caged.py aggregate `
  --dimensions competencia_mov municipio `
  --measures saldo estoque admitidos demitidos `
  --filter uf=33 `
  --output data\exports\rj_municipios.csv
```

The aggregation command accepts any standardized column in `--dimensions`, and any number of filters with `--filter coluna=valor1,valor2`.

## RAIS Annual Flow

The RAIS scraper is in `rais.py` and uses the annual consolidated FTP folders from:

`ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/`

Consolidated folders are exactly `AAAA`. Folders such as `2023 Parcial` and `2024 Parcial` are intentionally ignored by the code and should not be used.

Default RJ 2024 pipeline:

```powershell
python rais.py rj-2024
```

This downloads only the consolidated 2024 VINC archive that covers RJ (`RAIS_VINC_PUB_MG_ES_RJ.7z`), extracts it, converts a filtered `uf=33` subset, and writes:

```text
data\rais\exports\rj_municipios_2024.csv
```

Equivalent step-by-step commands:

```powershell
python rais.py list
python rais.py download
python rais.py extract
python rais.py convert
python rais.py aggregate
```

Useful options:

- `--uf all` on `list` or `download` disables the default RJ archive selection.
- `--all-ufs` on `convert` or `aggregate` disables the default `uf=33` row filter.
- `--start-year` and `--end-year` default to `2024` and only accept consolidated `AAAA` folders.
- `--encoding` defaults to `latin-1`, which matches the historical RAIS TXT files.

RAIS aggregation is stock-oriented, not monthly movement-oriented. Built-in measures are:

- `estoque_3112`: links active on 31/12, using `vinculo_ativo_31_12` when present.
- `vinculos`: all rows in the annual VINC file after filters.
- `admitidos_ano`: rows with admission month in the year.
- `desligados_ano`: rows with dismissal month in the year.

Generic numeric measures follow the CAGED CLI style, for example:

```powershell
python rais.py aggregate `
  --dimensions ano municipio sexo `
  --measures estoque_3112 mean:remuneracao_media_nominal `
  --output data\rais\exports\rj_municipios_sexo_2024.csv
```

## RAIS + Novo CAGED Integrated Stock

Use `integra_rais_caged.py` after generating:

- RAIS annual municipal stock, for example `data\rais\exports\rj_municipios_2024.csv`.
- Novo CAGED monthly municipal flows, for example `data\exports\rj_municipios.csv`.

Command:

```powershell
python integra_rais_caged.py
```

The integrated file is written to:

```text
data\exports\rj_municipios_rais_caged.csv
```

Integration rule:

- RAIS stock for year `t` (`estoque_3112`) is used as the starting stock for the first available Novo CAGED month in year `t+1`.
- If Novo CAGED has a newer year but the matching RAIS year is not available yet, stock continues from the previous month's `estoque_fim`.
- When the newer RAIS is later added, the first month of the matching CAGED year is recalculated from that newer RAIS stock.
- For each month: `estoque_fim = estoque_inicio + saldo`.
- The next month's `estoque_inicio` is the previous month's `estoque_fim`.
- Months without a CAGED row for a municipality are kept in the grid with zero flow.

Column naming:

- RAIS `admitidos_ano` becomes `admitidos_rais_ano`.
- RAIS `desligados_ano` becomes `desligados_rais_ano`.
- Novo CAGED `admitidos` becomes `admitidos_novo_caged`.
- Novo CAGED `demitidos` becomes `demitidos_novo_caged`.

## Python Dependencies

The download and extract steps use only the Python standard library plus the system `tar` command. On this machine, `tar` is available and can extract `.7z`.

The convert step needs `pandas` and `pyarrow` (declared in `pyproject.toml`):

```powershell
python -m pip install -e "."
```

Use `pandas` only after extraction; do not load the `.7z` archives directly into memory.

## Layout

The movement layout workbook lists these columns:

| Raw column | Standard column | Notes |
| --- | --- | --- |
| `competênciamov` | `competencia_mov` | Movement competence, `AAAAMM`. Use this for the labor-market month. |
| `região` | `regiao` | IBGE region code. |
| `uf` | `uf` | IBGE state code. |
| `município` | `municipio` | Municipality code. |
| `seção` | `secao` | CNAE 2.0 section. |
| `subclasse` | `subclasse` | CNAE 2.0 subclass. |
| `saldomovimentação` | `saldo_movimentacao` | Signed movement impact, normally `1` or `-1`. |
| `categoria` | `categoria` | Worker category. |
| `cbo2002ocupação` | `cbo2002_ocupacao` | CBO 2002 occupation. |
| `graudeinstrução` | `grau_de_instrucao` | Education level. |
| `idade` | `idade` | Worker age. |
| `horascontratuais` | `horascontratuais` | Weekly contracted hours, decimal comma in TXT. |
| `raçacor` | `raca_cor` | Race/color code. |
| `sexo` | `sexo` | Sex code. |
| `tipoempregador` | `tipoempregador` | Employer type. |
| `tipoestabelecimento` | `tipoestabelecimento` | Establishment type. |
| `tipomovimentação` | `tipomovimentacao` | Movement type. |
| `tipodedeficiência` | `tipodedeficiencia` | Disability type. |
| `indtrabintermitente` | `indtrabintermitente` | Intermittent worker flag. |
| `indtrabparcial` | `indtrabparcial` | Part-time worker flag. |
| `salário` | `salario` | Monthly declared salary, decimal comma in TXT. |
| `tamestabjan` | `tamestabjan` | Establishment employment-size band in January. |
| `indicadoraprendiz` | `indicadoraprendiz` | Apprentice flag. |
| `origemdainformação` | `origem_da_informacao` | Data origin. |
| `competênciadec` | `competencia_dec` | Declaration competence. |
| `competênciaexc` | `competencia_exc` | Exclusion competence; present in `EXC`. |
| `indicadordeexclusão` | `indicador_de_exclusao` | Exclusion flag; present in `EXC`. |
| `indicadordeforadoprazo` | `indicador_de_fora_do_prazo` | Late-declaration flag. |
| `unidadesaláriocódigo` | `unidade_salario_codigo` | Salary payment unit. |
| `valorsaláriofixo` | `valor_salario_fixo` | Fixed salary amount, decimal comma in TXT. |

The script also adds:

- `source_competencia`: month of the downloaded archive folder.
- `source_kind`: `MOV`, `FOR`, or `EXC`.

## Wrangling Rules

Read TXT with:

```python
pd.read_csv(path, sep=";", encoding="utf-8", decimal=",", dtype="string")
```

Then convert only true measures to numeric:

- Integers: `saldo_movimentacao`, `idade`
- Decimals: `horascontratuais`, `salario`, `valor_salario_fixo`
- Keep codes as strings, including `uf`, `municipio`, `subclasse`, `cbo2002_ocupacao`, `categoria`, and every flag/dictionary field.

Unify the schemas:

- `MOV` and `FOR` do not have `competencia_exc` or `indicador_de_exclusao`; add them as nulls.
- `EXC` has those columns and should remain in the same fact table.
- Add `source_kind` before stacking the files so you can audit whether a row came from in-deadline, late, or exclusion files.

Aggregation:

- For net job balance, use `saldo_movimentacao` for `MOV`/`FOR` and invert it for `EXC`, because exclusions cancel the original event.
- `admitidos` and `demitidos` are adjusted counts: regular/late declarations add `1`; exclusions subtract `1` from the original admission or dismissal type.
- For a revised time series, group by `competencia_mov`, not by `source_competencia`.
- For publication/release auditing, group by `source_competencia` and `source_kind`.
- `estoque` is not directly available in the movement microdata. The script computes it as cumulative corrected `saldo` by `competencia_mov` within the selected dimensions. Pass `--initial-stock-csv` if you need absolute stock instead of accumulated variation from the first selected month.

Example:

```python
df["sinal_correcao"] = df["source_kind"].eq("EXC").map({True: -1, False: 1})
df["saldo_corrigido"] = df["saldo_movimentacao"] * df["sinal_correcao"]
saldo_por_mes_uf = df.groupby(["competencia_mov", "uf"], as_index=False)["saldo_corrigido"].sum()
```

CLI examples:

```powershell
# Rio de Janeiro municipalities, every available movement month in the local data
python novo_caged.py aggregate `
  --dimensions competencia_mov municipio `
  --measures saldo estoque admitidos demitidos `
  --filter uf=33 `
  --output data\exports\rj_municipios.csv

# Same idea, but split by CNAE section too
python novo_caged.py aggregate `
  --dimensions competencia_mov municipio secao `
  --measures saldo admitidos demitidos mean:salario `
  --filter uf=33 `
  --output data\exports\rj_municipios_secao.csv

# Filter more than one value
python novo_caged.py aggregate `
  --dimensions competencia_mov uf sexo `
  --measures saldo admitidos demitidos `
  --filter uf=33,35 `
  --output data\exports\rj_sp_por_sexo.csv
```

Use Parquet for the cleaned layer because monthly `MOV` files are large. A good local lake layout is:

```text
data/
  raw/AAAAMM/*.7z
  extracted/AAAAMM/*.txt
  parquet/source_competencia=AAAAMM/source_kind=MOV/*.parquet
```

## Data Quality Checks

Run these checks after conversion:

- Row count by `source_competencia` and `source_kind`.
- Null/unknown rates for key dimensions: `cbo2002_ocupacao`, `municipio`, `subclasse`, `salario`.
- `saldo_movimentacao` values should be signed integers.
- The latest official correction notice says files up to February 2023 were replaced, but movement counts were not changed; re-download old files instead of relying on stale local copies.

## End-to-End Example

Example for multiple RAIS base years and all available Novo CAGED months.

Novo CAGED:

```powershell
python novo_caged.py download --start 202001 --end 202612 --kinds MOV FOR EXC
python novo_caged.py extract
python novo_caged.py convert --output-format parquet
python novo_caged.py aggregate `
  --dimensions competencia_mov municipio `
  --measures saldo admitidos demitidos `
  --filter uf=33 `
  --start-mov 202001 `
  --end-mov 202612 `
  --output data\exports\caged_rj_municipios.csv
```

RAIS:

```powershell
python rais.py download --start-year 2019 --end-year 2026
python rais.py extract --start-year 2019 --end-year 2026
python rais.py convert --start-year 2019 --end-year 2026
python rais.py aggregate --start-year 2019 --end-year 2026 `
  --output data\rais\exports\rais_rj_municipios.csv
```

The RAIS commands use only consolidated `AAAA` folders. Folders such as `2024 Parcial` are ignored.

```text
data\rais\exports\rais_rj_municipios.csv
```

Integration:

```powershell
python integra_rais_caged.py `
  --rais data\rais\exports\rais_rj_municipios.csv `
  --caged data\exports\caged_rj_municipios.csv `
  --output data\exports\rj_municipios_complete.csv
```

The integrated output resets January stock from RAIS when the matching previous RAIS year exists. For newer Novo CAGED months without a matching RAIS year yet, it continues from the previous month's `estoque_fim`.
