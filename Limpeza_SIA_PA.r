#Script de download e seleção inicial dos dados 
######################################################
#caso não tenha os pacotes:
#install.packages("tidyverse")
#install.packages("remotes") -> para baixar o microdatasus
#remotes::install_github("rfsaldanha/microdatasus")
#install.packages("geobr")
#######################################################
library(tidyverse)
library(microdatasus)
library(geobr)
#######################################################
#Download da tabela pelo microdatasus
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
      filter(str_detect(PA_CIDPRI, "^F2")) 
    
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

##Download e processamento de tabelas auxiliares##

##CNES##

arquivo_cache_cnes <- "cnes_ce_2025_processado.rds"

if (!file.exists(arquivo_cache_cnes)) {
  
  cnes_inicial <- fetch_datasus(
    month_start = 1, month_end = 12, 
    year_start = 2025, year_end = 2025, 
    uf = "CE", information_system = "CNES-ST" 
    #o objetivo é pegar os nomes "fantasias" dos hospitais/clinicas
  )
  
  cnes_inicial$DISTRSAN <- NA  #essa coluna estava dando erro
  
  cnes_nomes <- cnes_inicial %>%
    process_cnes(information_system = "CNES-ST", nomes = TRUE, municipality_data = TRUE) %>%
    select(CNES, FANTASIA, TP_UNID)
  
  # CODUFMUN, munResNome, munResUf -> caso queira pegar por aqui tambem
  # optei por usar os dados do IBGE, como forma de fazer checagem se está tudo direitinho
  
  saveRDS(cnes_nomes, arquivo_cache_cnes)
  
  message("Dados do CNES baixados e processados!")
  
} else {
  cnes_nomes <- readRDS(arquivo_cache_cnes) 
  message("Dados do CNES carregados do cache local.")
}

rm(list = ls(pattern = "inicial")) 

gc()

#CBO usando a tabela do microdatasus
tabela_cbo <- tabCBO

##sigtap
arquivo_cache_sigtap <- "sigtap_cache.rds"

if (!file.exists(arquivo_cache_sigtap)) {
  tabela_sigtap <- fetch_sigtab() 
  saveRDS(tabela_sigtap, arquivo_cache_sigtap)
  message("Tabela SIGTAP baixada e salva no cache!")
  
} else {
  tabela_sigtap <- readRDS(arquivo_cache_sigtap)
  message("Tabela SIGTAP carregada do cache local!")
}

##### tabela IBGE #####
arquivo_cache_ibge <- "municipios_ibge_cache.rds"

if (!file.exists(arquivo_cache_ibge)) {
  municipios_1 <- read_municipality(code_muni = "all", year = 2024) %>% 
    sf::st_drop_geometry() %>% 
    select(code_muni, name_state, name_muni) %>% 
    mutate(codigo_ibge_sia = substr(code_muni, 1,6)) 
  
  municipios_2 <- municipios_1 %>%
    select(codigo_ibge_sia, name_muni, name_state)
  
  saveRDS(municipios_2, arquivo_cache_ibge)
  rm(municipios_1)
  message("Dados do IBGE baixados, processados e salvos no cache!")
  
} else {
  municipios_2 <- readRDS(arquivo_cache_ibge)
  message("Dados do IBGE carregados do cache local.")
}

##Unificação com as auxiliares##

df_sia_pa_ads <- df_final %>% # selecionadno tudo que será usado pela analise
  select(
    PA_CODUNI, PA_GESTAO, PA_UFMUN, PA_PROC_ID, PA_CBOCOD, 
    PA_CIDPRI, PA_CMP, PA_IDADE, PA_SEXO, PA_RACACOR, PA_MUNPCN, PA_TPFIN, PA_DOCORIG, PA_NIVCPL, PA_TPUPS
  )

rm(df_final)


#Organizando as tabelas auxiliares para o join

nomes_hospital_ref <- cnes_nomes %>%
  select(CNES, FANTASIA, TP_UNID) %>%
  distinct(CNES, .keep_all = TRUE)

###########se for usando dados do CNES-ST##############
#cidades_ref <- cnes_nomes %>%
# select(CODUFMUN, munResNome) %>%
#  distinct() 
################################################

rm(cnes_nomes)


##left joins para facilitar a leitura e deixar o script melhor para futuras analises em outros locais

df_sia_pa_ads_1 <- df_sia_pa_ads %>%
  left_join(nomes_hospital_ref, by = c("PA_CODUNI" = "CNES")) %>% 
  
  left_join(municipios_2, by = c("PA_GESTAO" = "codigo_ibge_sia")) %>%
  rename(MUNICIPIO_RESPONSAVEL = name_muni) %>%
  rename(UF_GESTAO = name_state) %>%
  
  left_join(municipios_2, by = c("PA_MUNPCN" = "codigo_ibge_sia"))%>%
  rename(MUNICIPIO_PACIENTE = name_muni) %>%
  rename(UF_PACIENTE = name_state) %>%
  
  left_join(municipios_2, by = c("PA_UFMUN" = "codigo_ibge_sia")) %>%
  rename(MUNICIPIO_CNES = name_muni) %>%
  rename(UF_CNES = name_state)

##########estava utilizando os dados do CNES-ST para as cidades, entretanto, ele só pegaria todos os códigos se 
########## você baixasse dados do pais todo, optei por usar o geobr, caso seja feita uma analise do pais todo
########## pode vir a compensar usar novamente, so mudando as tabelas e usando o distinct() para a criação do dicionario
#######################################################################################

rm(df_sia_pa_ads)
rm(municipios_2)
rm(nomes_hospital_ref)

gc()

########ainda formatando tudo#########

df_sia_pa_ads_1 <- df_sia_pa_ads_1 %>%
  mutate(
    tipo_de_documento = case_when(
      PA_DOCORIG == "B" ~ "BPA - Consolidado",
      PA_DOCORIG == "I" ~ "BPA - Individualizado",
      PA_DOCORIG == "P" ~ "APAC - Principal",
      PA_DOCORIG == "S" ~ "APAC - Secundário",
      TRUE ~ "Outros"
    )
  ) #fonte dos codigos BPA/APAC: documento da prefeitura de fortaleza

df_sia_pa_ads_1 <- df_sia_pa_ads_1 %>%
  mutate(
    sexo = case_when(
      PA_SEXO == "M" ~ "Masculino",
      PA_SEXO == "F" ~ "Feminino"
    )
  ) #seria muito interessante ter "intersexo" como opção!

#utilizando a tabela da sigtap
df_sia_pa_ads_1 <- df_sia_pa_ads_1 %>%
  left_join(tabela_sigtap, by = c("PA_PROC_ID" = "COD"))

rm(tabela_sigtap)

#CBOs 

df_sia_pa_ads_1 <- df_sia_pa_ads_1 %>%
  left_join(tabela_cbo, by = c("PA_CBOCOD" = "cod")) %>%
  rename(PROFISSIONAL = nome)

rm(tabela_cbo)


#raca/cor formatado, fonte sigtap

df_sia_pa_ads_1 <- df_sia_pa_ads_1 %>%
  mutate(
    raca_cor = case_when(
      PA_RACACOR == "01" ~ "Branca",
      PA_RACACOR == "02" ~ "Preta",
      PA_RACACOR == "03" ~ "Parda",
      PA_RACACOR == "04" ~ "Amarela",
      PA_RACACOR == "05" ~ "Indígena",
      PA_RACACOR == "99" ~ "Sem Informação",
      TRUE ~ "Não declarado"
    )
  )

##tipo de financiamento fonte: sigtap

df_sia_pa_ads_1 <- df_sia_pa_ads_1 %>%
  mutate(
    financiamento = case_when(
      PA_TPFIN == "01" ~ "Atenção Básica (PAB)",
      PA_TPFIN == "02" ~ "Assistência Farmacêutica",
      PA_TPFIN == "04" ~ "FAEC",
      PA_TPFIN == "05" ~ "Incentivo - MAC",
      PA_TPFIN == "06" ~ "Média e Alta Complexidade (MAC)",
      PA_TPFIN == "07" ~ "Vigilância em Saúde",
      PA_TPFIN == "08" ~ "Gestão do SUS",
      TRUE ~ "Outros/Não informado"
    )
  )

## conferindo tudo:
nrow(df_sia_pa_ads_1) #checagem de duplicatas!!

#ajustando idades como numéricas
df_sia_pa_ads_1 <- df_sia_pa_ads_1 %>%
  mutate(PA_IDADE = as.numeric(PA_IDADE))

#coluna invasão de -> para e handling de NAs + logica inter estados
df_sia_pa_ads_1 <- df_sia_pa_ads_1 %>%
  mutate(
    invasao_municipial = case_when(
      is.na(PA_MUNPCN) | is.na(PA_UFMUN) ~ "indeterminado",
      PA_MUNPCN != PA_UFMUN ~ "sim", #optei por usar as colunas não limpas como forma de cross-checking
      TRUE ~ "nao" #vou melhorar a logica ainda
    )
  )

df_sia_pa_ads_1 <-df_sia_pa_ads_1 %>%
  mutate(
    invasao_estadual = case_when(
      UF_PACIENTE != UF_CNES ~ "sim",
      TRUE ~ "não"
    )
  )

#criação de normalização dos CIDs para o modelo "Fxx" para mitigar problemas de preenchimento
df_sia_pa_ads_1 <- df_sia_pa_ads_1 %>%
  mutate(
    CID = str_sub(PA_CIDPRI, 1, 3) 
  )

#selecionar apenas as tabelas limpas

Tabela_SIA_PA <- df_sia_pa_ads_1 %>%
  select(FANTASIA, TP_UNID, MUNICIPIO_RESPONSAVEL, UF_GESTAO, MUNICIPIO_PACIENTE, UF_PACIENTE, MUNICIPIO_CNES,
         UF_CNES, tipo_de_documento, sexo, nome_proced, PROFISSIONAL, raca_cor, financiamento, invasao_municipial, invasao_estadual,
         CID)

rm(df_sia_pa_ads_1)
gc()

