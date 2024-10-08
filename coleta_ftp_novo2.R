# Carregando as bibliotecas necessárias
library(tidyverse)
library(lubridate)
library(magrittr)
library(archive)
library(basedosdados)
library(writexl)
library(glue)

# Configurando o ID de faturamento para basedosdados
basedosdados::set_billing_id("secc-rj")

# Função para carregar dicionários e referências
carregar_referencias <- function() {
  dic_rais <- basedosdados::bdplyr("br_me_rais.dicionario") %>% 
    dplyr::filter(nome_coluna %in% c('sexo', 'raca_cor', 'grau_instrucao_apos_2005')) %>% 
    basedosdados::bd_collect()
  
  ibge_cnae_2 <- basedosdados::bdplyr("br_bd_diretorios_brasil.cnae_2") %>% 
    dplyr::select(secao, descricao_secao) %>% 
    basedosdados::bd_collect() %>% 
    distinct() %>% 
    dplyr::mutate(descricao_secao = stringr::str_replace_all(descricao_secao, "��gua", "Água"))
  
  list(dic_rais = dic_rais, ibge_cnae_2 = ibge_cnae_2)
}

# Função para baixar e extrair arquivos .7z do FTP
ftp_caged <- function(url) {
  temp_file <- tempfile(fileext = ".7z")
  temp_dir <- tempdir()
  
  # Baixando o arquivo .7z
  download.file(url, temp_file, mode = "wb")
  
  # Extraindo o conteúdo do .7z
  archive::archive_extract(temp_file, dir = temp_dir)
  
  # Identificando o arquivo extraído (assumindo que há apenas um arquivo)
  extracted_files <- archive::archive(temp_file)$path
  extracted_path <- file.path(temp_dir, extracted_files[1])
  
  # Lendo o arquivo delimitado (ajuste o delimitador se necessário)
  read_delim(extracted_path, delim = ";", locale = locale(encoding = "UTF-8"))
}

# Função para gerar URLs de FTP com base no tipo de arquivo
gerar_urls_ftp <- function(tipo,
                           inicio = paste0(year(Sys.Date())-2,'-01'),
                           fim = (Sys.Date()-months(2)) %>% format("%Y-%m")) {
  datas <- seq(ym(inicio), ym(fim), by = "months")
  
  tibble(data = datas) %>% 
    mutate(
      ano = year(data),
      mes = month(data),
      mes_competencia = sprintf("%02d", mes),
      ftp = case_when(
        tipo == "mov" ~ str_glue("ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/{ano}/{ano}{mes_competencia}/CAGEDMOV{ano}{mes_competencia}.7z"),
        tipo == "fora" ~ str_glue("ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/{ano}/{ano}{mes_competencia}/CAGEDFOR{ano}{mes_competencia}.7z"),
        tipo == "exc" ~ str_glue("ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/{ano}/{ano}{mes_competencia}/CAGEDEXC{ano}{mes_competencia}.7z"),
        TRUE ~ NA_character_
      )
    ) %>% 
    filter(!is.na(ftp))
}

# Função para processar os dados CAGED
processar_caged <- function(tipo, urls) {
  urls %>% 
    mutate(df = purrr::map(ftp, ~ {
      tryCatch({
        ftp_caged(.x) %>% 
          select(
            sigla_uf = uf,
            ano_mes = competênciamov,
            id_municipio = município,
            saldo_movimentacao = saldomovimentação,
            grau_instrucao = graudeinstrução,
            raça = raçacor,
            sexo,
            salario = valorsaláriofixo
          ) %>% 
          filter(sigla_uf == 33) %>% 
          mutate(
            salario = as.numeric(str_replace_all(salario, "[.,]", "")) / 100,
            movimentacao = if_else(saldo_movimentacao == 1, "Admitido", "Demitido")
          ) %>% 
          group_by(ano_mes, id_municipio, grau_instrucao, raça, sexo, movimentacao) %>% 
          summarise(
            saldo_movimentacao = sum(saldo_movimentacao, na.rm = TRUE),
            salario = sum(salario, na.rm = TRUE),
            .groups = "drop"
          )
      }, error = function(e) {
        message(str_glue("Erro ao processar {tipo} para { .x }: {e$message}"))
        NULL
      })
    }, .progress = TRUE)) %>% 
    select(df) %>% 
    unnest(df) %>% 
    replace_na(list(
      saldo_movimentacao = 0,
      salario = 0
    ))
}

# Função para completar as combinações faltantes
completar_combinacoes <- function(df, tipos = c("saldo_movimentacao", "salario")) {
  df %>% 
    complete(
      ano_mes,
      id_municipio, 
      grau_instrucao,
      raça,
      sexo,
      movimentacao,
      fill = setNames(as.list(rep(0, length(tipos))), tipos)
    )
}

# Carregando referências
referencias <- carregar_referencias()
dic_rais <- referencias$dic_rais
ibge_cnae_2 <- referencias$ibge_cnae_2

# Gerando URLs para cada tipo de dado
urls_mov <- gerar_urls_ftp("mov")
urls_fora <- gerar_urls_ftp("fora")
urls_exc <- gerar_urls_ftp("exc")

# Processando cada tipo de dado
caged_mov <- processar_caged("mov", urls_mov) %>% completar_combinacoes()
caged_fora <- processar_caged("fora", urls_fora) %>% 
  rename(
    saldo_for = saldo_movimentacao,
    salario_for = salario
  ) %>% 
  completar_combinacoes()
caged_exc <- processar_caged("exc", urls_exc) %>% 
  rename(
    saldo_exc = saldo_movimentacao,
    salario_exc = salario
  ) %>% 
  completar_combinacoes()

# Unindo os dados
caged <- caged_mov %>% 
  left_join(caged_fora, by = c("ano_mes", "id_municipio", "grau_instrucao", "raça", "sexo", "movimentacao")) %>% 
  left_join(caged_exc, by = c("ano_mes", "id_municipio", "grau_instrucao", "raça", "sexo", "movimentacao"),
            relationship = "many-to-many") %>% 
  replace_na(list(
    saldo_movimentacao = 0,
    saldo_for = 0,
    saldo_exc = 0,
    salario_for = 0,
    salario_exc = 0
  )) %>% 
  mutate(
    saldo = saldo_movimentacao + saldo_for - saldo_exc,
    salario = salario + salario_for - salario_exc
  ) %>% 
  select(-saldo_movimentacao, -saldo_for, -saldo_exc, -salario_for, -salario_exc)

# Finalizando os dados tratados
caged_trat_final <- caged %>% 
  filter(grau_instrucao != 80) %>% 
  mutate(
    ano = as.integer(str_sub(ano_mes, 1, 4)),
    mes = as.integer(str_sub(ano_mes, 5, 6))
  ) %>% 
  select(ano, mes, id_municipio, grau_instrucao, raça, sexo, movimentacao, saldo) %>%
  pivot_wider(names_from = movimentacao,
              values_from = saldo,
              values_fill = 0,
              values_fn = sum) %>%
  group_by(ano, mes, id_municipio, grau_instrucao, raça, sexo) %>% 
  summarise(
    admitido = sum(Admitido, na.rm = TRUE),
    demitido = sum(Demitido, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(saldo = admitido + demitido)

# Carregando e processando os dados da RAIS
ano_selecionado = year(Sys.Date()) - 3

rais <- basedosdados::bdplyr("br_me_rais.microdados_vinculos") %>%
  dplyr::filter(
    ano == ano_selecionado,
    sigla_uf == 'RJ',
    vinculo_ativo_3112 == '1'
  ) %>% 
  dplyr::select(ano, id_municipio, sexo, raca_cor, grau_instrucao_apos_2005) %>% 
  basedosdados::bd_collect() %>% 
  mutate(
    id_municipio = as.double(str_sub(id_municipio, 1, 6)),
    grau_instrucao_apos_2005 = as.double(grau_instrucao_apos_2005),
    grau_instrucao_apos_2005 = if_else(grau_instrucao_apos_2005 == -1, 99, grau_instrucao_apos_2005),
    sexo = case_when(
      sexo == '1' ~ 1,
      sexo == '2' ~ 3,
      sexo == '-1' ~ 9,
      TRUE ~ NA_real_
    ),
    raca_cor = case_when(
      raca_cor == '1' ~ 5,
      raca_cor == '2' ~ 1,
      raca_cor == '4' ~ 2,
      raca_cor == "6" ~ 4,
      raca_cor == '8' ~ 3,
      raca_cor == "9" ~ 9,
      raca_cor == "-1" ~ 6,
      TRUE ~ NA_real_
    ),
    estoque = n()
  ) %>%
  group_by(ano, id_municipio, sexo, raca_cor, grau_instrucao_apos_2005) %>% 
  summarise(estoque = n(), .groups = "drop")

# Integrando os dados da RAIS com os dados CAGED
caged_trat_estoque <- caged_trat_final %>% 
  ungroup() %>% 
  bind_rows(
    rais %>% 
      transmute(
        ano = 2021,
        mes = 12,
        admitido = 0,
        demitido = 0,
        id_municipio,
        grau_instrucao = grau_instrucao_apos_2005,
        raça = raca_cor,
        sexo,
        saldo = estoque
      )
  ) %>% 
  arrange(ano, mes) %>% 
  group_by(id_municipio, grau_instrucao, raça, sexo) %>% 
  mutate(
    estoque = cumsum(saldo),
    estoque_lag_12 = lag(estoque, 12)
  ) %>% 
  ungroup()

# Verificação da base final (exemplo)
verificacao <- caged_trat_estoque %>% 
  filter(
    ano == 2024, 
    mes == 8,
    id_municipio == 330010,
    grau_instrucao == 1,
    raça == 1,
    sexo == 1
  )

print(verificacao)

# Defina as variáveis globais
ano_inicial <- ano_selecionado
ano_final <- ano_selecionado + 3
data_hoje <- format(Sys.Date(), "%d-%m-%y")

# Use glue para criar o caminho do arquivo dinamicamente
caminho_arquivo <- glue::glue("C:\\Users\\deand\\Downloads\\NOVOCAGED_Estoque_{ano_inicial}_a_{ano_final}_acesso_{data_hoje}.xlsx")

# Filtrar e salvar o arquivo usando o caminho dinâmico
caged_trat_estoque %>% 
  filter(ano_selecionado >= ano_inicial) %>% 
  write_xlsx(path = caminho_arquivo)
