#falta impletementar a logica que fiz no script de limpeza
arquivo_cache <- "cnes_ce_2025_processado.rds"

if (!file.exists(arquivo_cache)) {
  
  cnes_inicial <- fetch_datasus(
    month_start = 1, month_end = 12, 
    year_start = 2025, year_end = 2025, 
    uf = "CE", information_system = "CNES-ST" #o objetivo é pegar os nomes "fantasias" dos hospitais/clinicas
  )
  
  cnes_inicial$DISTRSAN <- NA  #essa coluna estava dando erro
  
  cnes_nomes <- cnes_inicial %>%
    process_cnes(information_system = "CNES-ST", nomes = TRUE, municipality_data = TRUE) %>%
    select(CNES, FANTASIA,TP_UNID)
  
  # CODUFMUN, munResNome, munResUf -> caso queira pegar por aqui tambem
  # optei por usar os dados do IBGE, como forma de fazer checagem se está tudo direitinho
  
  saveRDS(cnes_nomes, arquivo_cache)
  
  message("Dados baixados e processados")
  
} else {
  
  cnes_nomes <- readRDS(arquivo_cache)
  message("Dados carregados do cache local.")
}

rm(list = ls(pattern = "inicial")) 

gc()

##sigtap

tabela_sigtap <- fetch_sigtab()

#CBO usando a tabela do microdatasus

tabela_cbo <- tabCBO

##### tabela IBGE #####
#tabelas para cruzamento de municipios, o uso pelo IBGE é para garantir puxar pacientes de outros estados e evitar NAs
municipios_1 <- read_municipality(code_muni = "all", year = 2024)  %>% #ano mais recente
  sf::st_drop_geometry() %>% #removendo geometrias para mapas
  select(code_muni, name_state, name_muni) %>% #selecionando apenas codigo, cidade e estado
  mutate( #o SIA utiliza 6 numeros enquanto o IBGE, 7, então tirei  digito de verificação
    codigo_ibge_sia = substr(code_muni, 1,6)
  ) 

municipios_2 <- municipios_1 %>%
select(codigo_ibge_sia, name_muni, name_state)

rm(municipios_1)