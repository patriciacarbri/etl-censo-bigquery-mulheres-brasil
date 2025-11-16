# src/bq_utils.py
import sys
from pathlib import Path
import argparse
import traceback
from typing import Any, Optional

import pandas as pd

# Import da config com fallback
try:
    from src.config import GCP_PROJECT_ID, BQ_DATASET_2022
except Exception:
    from config import GCP_PROJECT_ID, BQ_DATASET_2022

# BigQuery opcional
try:
    from google.cloud import bigquery
    _HAVE_BQ = True
except Exception:
    bigquery = None
    _HAVE_BQ = False


def get_bq_client() -> Any:
    """Cria cliente BigQuery com prints para testes."""
    print("[INFO] Criando cliente BigQuery...")

    if not _HAVE_BQ:
        raise RuntimeError("google-cloud-bigquery não instalado.")

    if not GCP_PROJECT_ID:
        raise RuntimeError("GCP_PROJECT_ID não encontrado em config.")

    client = bigquery.Client(project=GCP_PROJECT_ID)
    print("[INFO] Cliente criado. Projeto:", GCP_PROJECT_ID)
    return client


def load_sql(path: Path, **params) -> str:
    """Carrega SQL e substitui {{chaves}}."""
    path = Path(path)
    print(f"[INFO] Lendo SQL: {path}")

    if not path.exists():
        raise FileNotFoundError(f"SQL não encontrado: {path}")

    sql = path.read_text(encoding="utf-8")

    print("[INFO] Aplicando parâmetros:", params)
    for key, value in params.items():
        sql = sql.replace(f"{{{{{key}}}}}", str(value))

    return sql


def query_to_dataframe(
    sql_path: Path,
    ano: str = "2022",
    show_sql: bool = False,
    dry_run: bool = False,
    timeout: int = 300,
    poll_interval: int = 5,
    limit: Optional[int] = None,
) -> pd.DataFrame:

    print(f"[INFO] Iniciando query_to_dataframe ano={ano}")

    sql = load_sql(
        Path(sql_path),
        project_id=GCP_PROJECT_ID,
        dataset=BQ_DATASET_2022,
        ano=ano,
    )

    if limit:
        print(f"[INFO] Aplicando LIMIT {limit} para teste.")
        sql = sql.rstrip().rstrip(";") + f"\nLIMIT {limit}"

    if show_sql:
        print("\n------ SQL FINAL ------")
        print(sql)
        print("------------------------\n")

    if dry_run:
        print("[INFO] Dry-run ativo. Nenhuma execução será feita.")
        return pd.DataFrame({"sql": [sql]})

    client = get_bq_client()

    print("[INFO] Enviando query ao BigQuery...")
    try:
        job = client.query(sql)
        print(f"[INFO] Job enviado. job_id={job.job_id}")
    except Exception:
        print("[ERRO] Falha ao enviar job:")
        traceback.print_exc()
        raise

    # Polling simples
    import time
    start = time.time()

    while True:
        if job.done():
            print(f"[INFO] Job concluído. State={job.state}")
            break

        elapsed = int(time.time() - start)

        if elapsed > timeout:
            raise TimeoutError(f"Tempo excedido: {timeout}s")

        print(f"[INFO] Aguardando job... {elapsed}s (id={job.job_id})")
        time.sleep(poll_interval)

    print("[INFO] Convertendo resultado para DataFrame...")
    df = job.to_dataframe()

    print("[INFO] DataFrame carregado:", df.shape)
    print(df.head())

    return df


# ============================
# Execução direta no terminal
# ============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Executor simples de SQL no BigQuery.")
    parser.add_argument("--sql", "-s", required=True, help="Arquivo SQL.")
    parser.add_argument("--ano", "-a", default="2022")
    parser.add_argument("--show-sql", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--poll-interval", type=int, default=5)
    parser.add_argument("--limit", type=int, default=None)

    args = parser.parse_args()

    print("\n=== Execução BigQuery ===")

    try:
        df = query_to_dataframe(
            sql_path=args.sql,
            ano=args.ano,
            show_sql=args.show_sql,
            dry_run=args.dry_run,
            timeout=args.timeout,
            poll_interval=args.poll_interval,
            limit=args.limit,
        )
        print("\n[OK] Execução finalizada. Shape:", df.shape)
    except Exception as exc:
        print("\n[ERRO FATAL]:", type(exc).__name__, exc)
        sys.exit(1)
