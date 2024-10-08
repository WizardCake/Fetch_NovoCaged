#' @title Carregar Dicionários e Referências
#' @description Função para carregar os dicionários e referências da base de dados.
#' @return Lista contendo o dicionário da RAIS e o diretório CNAE do IBGE.
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

#' @title Baixar e Extrair Arquivo CAGED
#' @description Baixa e extrai arquivos .7z do FTP, lê o arquivo delimitado e retorna um dataframe.
#' @param url URL do arquivo .7z a ser baixado.
#' @return Dataframe com o conteúdo extraído e processado.
ftp_caged <- function(url) {
  temp_file <- tempfile(fileext = ".7z")
  temp_dir <- tempdir()
  
  download.file(url, temp_file, mode = "wb")
  archive::archive_extract(temp_file, dir = temp_dir)
  
  extracted_files <- archive::archive(temp_file)$path
  extracted_path <- file.path(temp_dir, extracted_files[1])
  
  read_delim(extracted_path, delim = ";", locale = locale(encoding = "UTF-8"))
}

#' @title Gerar URLs de FTP para CAGED
#' @description Gera URLs de FTP para o download dos dados CAGED com base no tipo de arquivo.
#' @param tipo Tipo de dado (mov, fora, exc).
#' @param inicio Data de início no formato "YYYY-MM".
#' @param fim Data de fim no formato "YYYY-MM".
#' @return Tibble contendo as URLs geradas.
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
        tipo == "mov" ~ glue::glue("ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/{ano}/{ano}{mes_competencia}/CAGEDMOV{ano}{mes_competencia}.7z"),
        tipo == "fora" ~ glue::glue("ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/{ano}/{ano}{mes_competencia}/CAGEDFOR{ano}{mes_competencia}.7z"),
        tipo == "exc" ~ glue::glue("ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/{ano}/{ano}{mes_competencia}/CAGEDEXC{ano}{mes_competencia}.7z"),
        TRUE ~ NA_character_
      )
    ) %>% 
    filter(!is.na(ftp))
}

#' @title Processar Dados CAGED
#' @description Processa os dados CAGED a partir das URLs fornecidas.
#' @param tipo Tipo de dado (mov, fora, exc).
#' @param urls Tibble com as URLs para processamento.
#' @return Dataframe com os dados CAGED processados.
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
        message(glue::glue("Erro ao processar {tipo} para { .x }: {e$message}"))
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

#' @title Completar Combinações Faltantes
#' @description Completa combinações faltantes de colunas como ano, município, grau de instrução etc.
#' @param df Dataframe com os dados CAGED processados.
#' @param tipos Colunas a serem preenchidas com zero nas combinações faltantes.
#' @return Dataframe com as combinações completas.
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

# Código para integrar RAIS, gerar arquivos finais e salvar
# As funções acima são usadas para construir e processar os dados.

# Definir as variáveis globais
ano_inicial <- 2021
ano_final <- ano_inicial + 3
data_hoje <- format(Sys.Date(), "%d-%m-%Y")

# Criar o caminho do arquivo de saída
caminho_arquivo <- glue::glue("C:\\Users\\deand\\Downloads\\NOVOCAGED_Estoque_{ano_inicial}_a_{ano_final}_acesso_{data_hoje}.xlsx")

# Filtrar e salvar o arquivo de saída
caged_trat_estoque %>% 
  filter(ano >= ano_inicial) %>% 
  writexl::write_xlsx(path = caminho_arquivo)
