# pdet-fetch

Pipeline em Python para baixar, extrair, converter, agregar e integrar microdados públicos do Novo CAGED e da RAIS a partir do FTP do MTE/PDET.

O projeto cobre três fluxos:

- Novo CAGED mensal: arquivos `CAGEDMOV`, `CAGEDFOR` e `CAGEDEXC`.
- RAIS anual consolidada: arquivos `VINC` e `ESTAB`, ignorando pastas parciais.
- Integração RAIS + Novo CAGED: estoque anual da RAIS como base e saldos mensais do Novo CAGED como evolução.

## Instalação

```powershell
python -m pip install -e ".[dev]"
python -m pytest
```

Dependências principais: `pandas` e `pyarrow`. A extração usa o comando `tar` disponível no sistema; no Windows, ele normalmente funciona com arquivos `.7z` quando o bsdtar/7-Zip está disponível no PATH.

Os comandos podem ser chamados pelos scripts de compatibilidade:

```powershell
python novo_caged.py --help
python rais.py --help
python integra_rais_caged.py --help
```

Após `pip install -e .`, também ficam disponíveis os entry points:

```powershell
novo-caged --help
rais --help
integra-rais-caged --help
```

## Estrutura

```text
src/pdet/
  common.py              # FTP, extração, normalização e utilitários comuns
  aggregation.py         # pipeline genérico de filtros, medidas e agregações
  caged/                 # Novo CAGED: FTP, extração, conversão, agregação e CLI
  rais/                  # RAIS: FTP, extração, conversão, agregação e CLI
  integra/               # integração de estoque RAIS com fluxo Novo CAGED
tests/                   # testes unitários
docs/                    # leia-me e layout oficial do Novo CAGED
NT/                      # PDFs técnicos da RAIS
```

Os dados gerados ficam fora do versionamento:

```text
data/
  raw/                   # .7z do Novo CAGED
  extracted/             # TXT extraído do Novo CAGED
  parquet/ ou csv/       # Novo CAGED convertido
  exports/               # agregados e integrações
  rais/
    raw/
    extracted/
    parquet/subset=.../
    csv/subset=.../
    exports/
```

## Novo CAGED

Fonte FTP: `/pdet/microdados/NOVO CAGED`.

Fluxo recomendado:

```powershell
python novo_caged.py list --start 202603 --end 202603
python novo_caged.py download --start 202603 --end 202603 --kinds MOV FOR EXC
python novo_caged.py convert --output-format parquet --cleanup
python novo_caged.py aggregate `
  --dimensions competencia_mov municipio `
  --measures saldo admitidos demitidos `
  --filter uf=33 `
  --output data\exports\caged_rj_municipios.csv
```

Observações:

- `convert` extrai automaticamente os `.7z` se não houver TXT em `data/extracted/`.
- `--cleanup` remove apenas os TXT extraídos; os `.7z` em `data/raw/` são preservados.
- `aggregate` lê automaticamente Parquet, depois CSV, depois TXT.
- Medidas nativas: `saldo`, `admitidos`, `demitidos`, `estoque`, `movimentacoes`.
- Medidas numéricas genéricas seguem `sum:coluna`, `mean:coluna`, `min:coluna` ou `max:coluna`.

Exemplo com estoque acumulado:

```powershell
python novo_caged.py aggregate `
  --dimensions competencia_mov municipio `
  --measures saldo estoque admitidos demitidos `
  --filter uf=33 `
  --output data\exports\caged_rj_municipios_estoque.csv
```

Sem `--initial-stock-csv`, `estoque` é o saldo acumulado a partir do primeiro mês selecionado, não o estoque absoluto.

## RAIS

Fonte FTP: `/pdet/microdados/RAIS`.

A CLI usa por padrão a RAIS consolidada de 2024, arquivo `VINC`, filtrada para RJ (`uf=33`):

```powershell
python rais.py rj-2024
```

Saída padrão:

```text
data\rais\exports\rj_municipios_2024.csv
```

Fluxo equivalente, passo a passo:

```powershell
python rais.py list --start-year 2024 --end-year 2024
python rais.py download --start-year 2024 --end-year 2024
python rais.py convert --start-year 2024 --end-year 2024 --cleanup
python rais.py aggregate --start-year 2024 --end-year 2024 `
  --output data\rais\exports\rais_rj_municipios.csv
```

Observações:

- Apenas pastas consolidadas `AAAA` são consideradas; pastas como `2024 Parcial` são ignoradas.
- `convert` também autoextrai quando só existem `.7z` em `data/rais/raw/`.
- O filtro padrão é `uf=33`, gravado no subset `uf_33`.
- Use `--all-ufs` para remover o filtro padrão.
- Ao criar um subset customizado, use o mesmo `--subset-name` no `convert` e no `aggregate`.
- A codificação padrão da RAIS é `latin-1`.

Medidas nativas da RAIS:

- `estoque_3112`: vínculos ativos em 31/12.
- `vinculos`: total de linhas após filtros.
- `admitidos_ano`: vínculos com mês de admissão preenchido.
- `desligados_ano`: vínculos com mês de desligamento preenchido.

Exemplo com dimensão adicional e média salarial:

```powershell
python rais.py aggregate `
  --dimensions ano municipio sexo `
  --measures estoque_3112 vinculos mean:remuneracao_media_nominal `
  --output data\rais\exports\rais_rj_municipios_sexo.csv
```

## Integração RAIS + Novo CAGED

Use a integração depois de gerar:

- um agregado anual da RAIS com `ano`, `municipio` e `estoque_3112`;
- um agregado mensal do Novo CAGED com `competencia_mov`, `municipio`, `saldo`, `admitidos` e `demitidos`.

```powershell
python integra_rais_caged.py `
  --rais data\rais\exports\rais_rj_municipios.csv `
  --caged data\exports\caged_rj_municipios.csv `
  --output data\exports\rj_municipios_complete.csv
```

Regra usada:

- RAIS do ano `t` vira base para o primeiro mês disponível do Novo CAGED no ano `t+1`.
- Se a RAIS mais recente ainda não existir, a série continua a partir do `estoque_fim` anterior.
- Para cada mês: `estoque_fim = estoque_inicio + saldo`.
- Meses sem linha do Novo CAGED para um município entram com fluxo zero.

## Regras de tratamento

- Colunas de código permanecem como texto (`uf`, `municipio`, `subclasse`, `cbo2002_ocupacao`, flags e dicionários).
- Colunas de medida são convertidas para numérico somente quando necessário.
- No Novo CAGED, registros `EXC` invertem o sinal do movimento para corrigir exclusões.
- Para séries revisadas do Novo CAGED, agregue por `competencia_mov`; `source_competencia` serve para auditoria da competência do arquivo baixado.
- A RAIS é anual e orientada a estoque; o Novo CAGED é mensal e orientado a movimento.

## Testes

```powershell
python -m pytest
```

Os testes cobrem normalização de colunas, filtros, medidas, conversão, agregação e integração RAIS + Novo CAGED.
