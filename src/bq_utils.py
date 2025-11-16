from pathlib import Path
from string import Template

import pandas as pd
from google.cloud import bigquery

from .config import GCP_PROJECT_ID, BQ_DATASET_2022


def get_bq_client() -> bigquery.Client:
    """Retorna um cliente BigQuery usando as credenciais padrão."""
    return bigquery.Client(project=GCP_PROJECT_ID)


def load_sql(path: Path, **params) -> str:
    """
    Lê um arquivo .sql e substitui placeholders no formato {{chave}}.
    Exemplo: {{project_id}}, {{dataset}}, etc.
    """
    text = path.read_text(encoding="utf-8")
    for key, value in params.items():
        text = text.replace(f"{{{{{key}}}}}", value)
    return text


def query_to_dataframe(sql_path: Path, ano: str) -> pd.DataFrame:
    """
    Executa uma query SQL parametrizada e retorna um DataFrame.

    Atualmente só há dataset configurado para 2022.
    """
    if ano != "2022":
        raise ValueError(f"Ano não suportado: {ano}")

    client = get_bq_client()

    sql = load_sql(
        sql_path,
        project_id=GCP_PROJECT_ID,
        dataset=BQ_DATASET_2022,
    )

    df = client.query(sql).to_dataframe()
    return df
