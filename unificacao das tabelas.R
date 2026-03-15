df_sia_pa_ads <- df_final %>% # selecionadno tudo que sera usado pela analise
  select(
    PA_CODUNI, PA_GESTAO, PA_UFMUN, PA_PROC_ID, PA_CBOCOD, 
    PA_CIDPRI, PA_CMP, PA_IDADE, PA_SEXO, PA_RACACOR, PA_MUNPCN, PA_TPFIN, PA_DOCORIG, PA_NIVCPL, PA_TPUPS
  )

rm(df_final)


#Organizando as tabelas auxiliares para o join

nomes_hospital_ref <- cnes_nomes %>%
  select(CNES, FANTASIA, TP_UNID) %>%
  distinct(CNES, .keep_all = TRUE)

###########usando dados do CNES-ST##############
#cidades_ref <- cnes_nomes %>%
 # select(CODUFMUN, munResNome) %>%
#  distinct() -> pode vir a ser util depois
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
  ) #fonte: documento da prefeitura de fortaleza, no anexo do projeto no OSF

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

#criação de normalização dos CIDs + tabela de cids fonte: sigtap/cid10
df_sia_pa_ads_1 <- df_sia_pa_ads_1 %>%
  mutate(
      CID = str_sub(PA_CIDPRI, 1, 3) #extrai para o modelo "Fxx" para mitigar problemas de preenchimento
  )