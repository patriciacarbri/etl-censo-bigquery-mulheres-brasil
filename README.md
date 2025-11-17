# ETL - Censo 2022: Alfabetização da População Feminina do Brasil

Este projeto implementa um pipeline ETL que:

1. Consulta o Censo 2022 no BigQuery 
2. Consulta no Sidra via biblioteca.
3. Agrega por UF, grupo de idade e raça/cor.
4. Consolida em um único dataset.
5. Salva os resultados em CSV e Parquet.
6. Gera automaticamente um dicionário de dados.

## Perguntas que este projeto ajuda a responder

- Qual o número total de mulheres por UF e faixa etária?
- Qual a distribuição racial da população feminina por UF?

## Stack

- Python
- Pandas
- Google BigQuery
- Parquet/CSV

## Como rodar

1. Crie e ative o ambiente virtual.
2. Instale as dependências:

```bash
pip install -r requirements.txt

