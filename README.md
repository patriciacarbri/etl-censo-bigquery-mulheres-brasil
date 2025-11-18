# ETL - Censo 2022: Alfabetização da População Feminina do Brasil

Este projeto implementa um pipeline ETL que:

1. Consulta o Censo 2022 no BigQuery 
2. Consulta no Sidra via biblioteca.
3. Agrega por UF, grupo de idade e raça/cor.
4. Consolida em um único dataset.
5. Salva os resultados em Parquet.
6. Gera automaticamente um dicionário de dados.


## Stack

- Python
- Pandas
- Google BigQuery
- Parquet/CSV

## Como rodar

Arquivo principal: src/pipeline_orchestrator.py

1. Crie e ative o ambiente virtual.
2. Instale as dependências:


```bash
pip install -r requirements.txt

