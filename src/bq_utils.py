from pathlib import Path
from string import Template

import pandas as pd
from google.cloud import bigquery

from .config import GCP_PROJECT_ID, BQ_DATASET_2010, BQ_DATASET_2022

def get_bq_client() -> bigquery.Client:
    """Retorna um cliente BigQuery usando as credenciais padrão."""
    return bigquery.Client(project=GCP_PROJECT_ID)

def load_sql(path: Path, **params) -> str:
    """
    Lê um arquivo .sql e substitui placeholders no formato {{chave}}.
    Exemplo: {{project_id}}, {{dataset_2010}}, etc.
    """
    text = path.read_text(encoding="utf-8")
    # substituição simples usando Template
    for key, value in params.items():
        text = text.replace(f"{{{{{key}}}}}", value)
    return text

def query_to_dataframe(sql_path: Path, ano: str) -> pd.DataFrame:
    client = get_bq_client()
    if ano == "2010":
        dataset = BQ_DATASET_2010
    elif ano == "2022":
        dataset = BQ_DATASET_2022
    else:
        raise ValueError(f"Ano não suportado: {ano}")

    sql = load_sql(
        sql_path,
        project_id=GCP_PROJECT_ID,
        dataset_2010=BQ_DATASET_2010,
        dataset_2022=BQ_DATASET_2022,
    )
    job = client.query(sql)
    df = job.to_dataframe()
    return df
