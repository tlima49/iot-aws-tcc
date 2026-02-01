# Projeto IoT na AWS com Grafana Cloud

Este repositório contém os principais artefatos do projeto desenvolvido para IoT na nuvem AWS, incluindo funções Lambda, definições de tabelas no Glue Data Catalog e queries utilizadas em dashboards do Grafana Cloud.

---

## Estrutura do Repositório
/lambda_functions/ └── process_sensor_data.py        # Funções Lambda em Python
/glue_catalog/ └── sensor_data_table.json        # Definição da tabela Glue Data Catalog
/grafana_queries/ └── sensor_timeseries.sql         # Query para painel Time Series (Athena) └── ph_stat_panel.sql             # Query para painel Stat do pH (Athena)
/iot_core_queries/ └── iot_roles.sql                 # Queries das roles do IoT Core


---

## Conteúdo

### 1. Funções Lambda
- Implementadas em Python (`.py`).
- Responsáveis pelo processamento de dados recebidos dos sensores IoT.
- Cada função está documentada com comentários explicativos.

### 2. Glue Data Catalog
- Arquivo `.json` contendo a definição da tabela `sensor_data`.
- Inclui metadados como nome da tabela, database, colunas e tipos de dados.

### 3. Grafana Cloud Queries
- **`sensor_timeseries.sql`**: Query para painel *Time Series* exibindo dados temporais dos sensores.  
- **`ph_stat_panel.sql`**: Query para painel *Stat* exibindo o valor atual do pH.  

### 4. IoT Core Queries
- **`iot_roles.sql`**: Contém as queries das roles configuradas no AWS IoT Core para roteamento e filtragem de mensagens.

---

## Segurança
- Nenhuma credencial ou chave de acesso da AWS está incluída neste repositório.  
- Recomenda-se configurar variáveis de ambiente ou usar o AWS Secrets Manager para credenciais.  

---

## Como usar
1. Clone este repositório:
   ```bash

   git clone https://github.com/tlima49/iot-aws-tcc.git
