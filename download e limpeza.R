#Script de download e seleção inicial dos dados 
######################################################
#install.packages("tidyverse")
#install.packages("remotes") -> para baixar o microdatasus
#remotes::install_github("rfsaldanha/microdatasus")
#install.packages("geobr")
#######################################################
library(tidyverse)
library(microdatasus)
library(geobr)
#######################################################
if(!dir.exists("meses")) dir.create("meses") #caso diretorio já exista / já tenha sido feito o download, não repetir
#a pasta serve para armazenar os dados mensais
#Criação dos arquivos por mês

for (mes in 1:12) {
  arquivo_mensal <- paste0("meses/mes_", mes, ".rds") #paste0 para evitar espaços, essa linha cria os arquivos individuais em .rds
  if(file.exists(arquivo_mensal)) next #garantindo a possibilidade de rodar de novo sem repetir arquivos existentes
  message(paste("Processando mês:", mes)) 
  
  gc()
  
  #aqui o trycatch vai servir para evitar a interrupção do loop em anos incompletos
  #https://cnuge.github.io/post/trycatch/
  tryCatch({
    df_inicial_sia <- fetch_datasus( 
      year_start = 2025, month_start = mes,
      year_end = 2025, month_end = mes,
      uf = "CE",
      information_system = "SIA-PA"
    ) 
    
    if(is.null(df_inicial_sia)) stop("Download desse mes falhou! :(")
    
    #municipios <- "para analises menores" #lista municipios
    #tentei blindar contra erros de preenchimento
    
    df_filtrado_sia <- df_inicial_sia %>%
      filter(str_detect(PA_CIDPRI, "^F")) 
    
    #adicionar os municipios na pipeline para análises por regiao
      
    
    write_rds(df_filtrado_sia, arquivo_mensal) #salvando arquivo do mes
    
    rm(df_inicial_sia)
    rm(df_filtrado_sia)
    #removendo dfs da ram para salvar espaço
    
    gc()
    
  }, error = function(e) {
    
  })
  
}

arquivos <- list.files("meses", full.names = TRUE, pattern = "\\.rds")

df_final <- arquivos %>% map_dfr(read_rds) #o map vai juntar os arquivos mensais

message(paste("Total de registros unificados:", nrow(df_final)))
