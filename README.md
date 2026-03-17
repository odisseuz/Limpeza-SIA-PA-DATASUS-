# Pipeline de Limpeza: SIA (DATASUS)

> **Status:** 🚧 Work In Progress (WIP)
> SIA-PA Limpeza está ok, mas não de todos os dados do sistema!
> Próximo passo: Limpar os dados do SIA-PS.

Um script em R desenvolvido para realizar a limpeza e o processamento de dados da Produção Ambulatorial (PA) do Sistema de Informações Ambulatoriais (SIA/DATASUS). 

## Dependências e Agradecimentos
Este script não existiria sem os ombros de gigantes da comunidade R no Brasil e no mundo. Toda minha admiração aos pacotes abaixo:

* [tidyverse](https://www.tidyverse.org/) <3 - Pela gramática elegante e funcional de manipulação de dados.
* [microdatasus](https://github.com/rfsaldanha/microdatasus) - Um pacote incrível que democratizou e facilitou o acesso aos dados de saúde no Brasil!
* [geobr](https://github.com/ipeaGIT/geobr) - Outro pacote pelo qual tenho muita admiração, essencial para a espacialização dos dados.

---

## Visão Geral do Processamento
Por conta da RAM, ele opera sob uma lógica de ETL, baixando os arquivos do mês e filtrando logo em seguida, nos picos durantes os testes, atingiu 3 gb de ram, contra cerca de 8-9 sem essa lógica.
O foco da extração atual são os atendimentos no estado do Ceará (CE) no ano de 2025, filtrando especificamente diagnósticos primários que iniciam com **"F2"** (Esquizofrenia, transtornos esquizotípicos e transtornos delirantes).
Entretanto, é possível alterar o tipo de dado requerido dentro do script!

O pipeline unifica os microdados com tabelas auxiliares (CNES, SIGTAP, CBO e IBGE) para traduzir códigos em nomes legíveis e gerar indicativos de deslocamento de pacientes (invasão municipal/estadual).

## ⚙️ Etapas do Pipeline

### 1. Download e Filtragem Inicial (SIA-PA)
* Cria um diretório local chamado `meses/`.
* Itera sobre os 12 meses baixando os dados, filtrando pelo CID principal (`PA_CIDPRI` iniciando em "F2").
* Salva o resultado de cada mês em arquivos `.rds` (cache) para evitar refazer downloads em caso de falha, unificando tudo ao final.

### 2. Obtenção de Tabelas Auxiliares (Cache Local)
Verifica se as tabelas de domínio já existem localmente. Se não, realiza o download e processamento:
* **CNES-ST:** Busca o nome "Fantasia" e o tipo das unidades de saúde.
* **SIGTAP:** Traduz os códigos de procedimentos.
* **CBO:** Traduz os códigos de ocupação dos profissionais.
* **IBGE (`geobr`):** Extrai a relação de códigos de municípios e estados para padronizar as localidades.

### 3. Cruzamento de Dados e Limpeza 
* Realiza `left_join` para enriquecer a tabela principal com descrições legíveis.
* **Decodificação:** Traduz categorias padronizadas do SUS (Documento, Sexo, Raça/Cor, Financiamento).
* **Deslocamento (Invasão):** Calcula se o paciente foi atendido fora do seu município ou estado de residência.
* **Normalização:** Trunca o CID para os 3 primeiros caracteres, mitigando preenchimentos errôneos.

---

## Dicionário de Dados Final (`Tabela_SIA_PA`)

| Coluna | Descrição | Origem |
| :--- | :--- | :--- |
| **FANTASIA** | Nome fantasia do estabelecimento de saúde. | CNES |
| **TP_UNID** | Tipo de unidade de saúde. | CNES |
| **MUNICIPIO_RESPONSAVEL** | Município gestor do atendimento. | IBGE / geobr |
| **UF_GESTAO** | Estado gestor do atendimento. | IBGE / geobr |
| **MUNICIPIO_PACIENTE** | Município de residência do paciente. | IBGE / geobr |
| **UF_PACIENTE** | Estado de residência do paciente. | IBGE / geobr |
| **MUNICIPIO_CNES** | Município onde o estabelecimento está localizado. | IBGE / geobr |
| **UF_CNES** | Estado onde o estabelecimento está localizado. | IBGE / geobr |
| **tipo_de_documento** | Instrumento de registro (Ex: BPA, APAC). | SIA-PA |
| **sexo** | Sexo do paciente (Masculino/Feminino). | SIA-PA |
| **nome_proced** | Nome completo do procedimento realizado. | SIGTAP |
| **PROFISSIONAL** | Profissão do responsável pelo atendimento. | CBO |
| **raca_cor** | Raça/Cor autodeclarada do paciente. | SIA-PA |
| **financiamento** | Tipo de financiamento do procedimento no SUS. | SIA-PA |
| **invasao_municipial** | Indica atendimento fora do município (sim/não/indeterminado). | Calculado |
| **invasao_estadual** | Indica atendimento fora do estado (sim/não). | Calculado |
| **CID** | Código Internacional de Doenças (3 caracteres). | SIA-PA |

---


---
*Desenvolvido com muito café e MPB* ☕🎶
