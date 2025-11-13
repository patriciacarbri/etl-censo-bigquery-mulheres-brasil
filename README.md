# ETL - Censo 2010 e 2022: População Feminina do Brasil

Este projeto implementa um pipeline ETL simples que:

1. Consulta o Censo 2010 e 2022 no BigQuery.
2. Filtra a população feminina.
3. Agrega por UF, grupo de idade e raça/cor.
4. Consolida os anos 2010 e 2022 em um único dataset.
5. Salva os resultados em CSV e Parquet.
6. Gera automaticamente um dicionário de dados.

## Perguntas que este projeto ajuda a responder

- Qual o número total de mulheres por UF e faixa etária?
- Como essa distribuição se compara entre 2010 e 2022?
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
