# src/bq_utils.py
import sys
from pathlib import Path
import argparse
import traceback
from typing import Any, Optional
import pandas as pd

# BigQuery
try:
    from google.cloud import bigquery
    _HAVE_BQ = True
except Exception:
    bigquery = None
    _HAVE_BQ = False


# Config (fallback para execução direta)
try:
    from src.config import GCP_PROJECT_ID, BQ_DATASET_2022
except Exception:
    from config import GCP_PROJECT_ID, BQ_DATASET_2022


# ---------------------------------------------------------
# CLIENTE BIGQUERY
# ---------------------------------------------------------
def get_bq_client() -> Any:
    print("[INFO] Criando cliente BigQuery...")

    if not _HAVE_BQ:
        raise RuntimeError("google-cloud-bigquery não instalado.")

    if not GCP_PROJECT_ID:
        raise RuntimeError("GCP_PROJECT_ID não encontrado em config.")

    client = bigquery.Client(project=GCP_PROJECT_ID)
    print("[INFO] Cliente criado. Projeto:", GCP_PROJECT_ID)
    return client


# ---------------------------------------------------------
# CARREGAR SQL
# ---------------------------------------------------------
def load_sql(path: Path, **params) -> str:
    path = Path(path)
    print(f"[INFO] Lendo SQL: {path}")

    if not path.exists():
        raise FileNotFoundError(f"SQL não encontrado: {path}")

    sql = path.read_text(encoding="utf-8")
    print("[INFO] Aplicando parâmetros:", params)

    for key, value in params.items():
        sql = sql.replace(f"{{{{{key}}}}}", str(value))

    return sql


# ---------------------------------------------------------
# Salvar o resultado de query em CSV
# ---------------------------------------------------------
def query_to_csv(
    sql_path: Path,
    out_path: Path,
    ano: str = "2022",
    show_sql: bool = False,
    dry_run: bool = False,
    limit: Optional[int] = None,
    timeout: int = 300,
    poll_interval: int = 5,
    page_size: int = 5000,
):
    print(f"[INFO] Iniciando query_to_csv para ano={ano}")

    sql = load_sql(
        Path(sql_path),
        project_id=GCP_PROJECT_ID,
        dataset=BQ_DATASET_2022,
        ano=ano,
    )

    if limit:
        print(f"[INFO] Aplicando LIMIT {limit} (somente teste)")
        sql = sql.rstrip().rstrip(";") + f"\nLIMIT {limit}"

    if show_sql:
        print("\n------ SQL FINAL ------")
        print(sql)
        print("------------------------\n")

    if dry_run:
        print("[INFO] Dry-run: Não executando. Apenas SQL carregado.")
        print("SQL:\n", sql)
        return

    client = get_bq_client()

    print("[INFO] Enviando query ao BigQuery...")
    try:
        job = client.query(sql)
        print(f"[INFO] Job enviado: job_id={job.job_id}")
    except Exception:
        print("[ERRO] Falha ao enviar job:")
        traceback.print_exc()
        raise

    # Polling
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

    # Recuperar tabela destino
    print("[INFO] Obtendo tabela destino do job...")
    try:
        table = client.get_table(job.destination)
    except Exception:
        print("[ERRO] job.destination é None — BigQuery não materializou o resultado.")
        raise

    print(f"[INFO] Tabela destino: {table.full_table_id}")
    print(f"[INFO] Linhas estimadas: {table.num_rows}")

    # Criar CSV por streaming
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Gravando CSV em: {out_path}")

    writer = None
    total = 0

    rows_iter = client.list_rows(table, page_size=page_size)
    for page in rows_iter.pages:
        batch = list(page)
        if not batch:
            break

        # Para evitar montar DataFrame gigante na memória, processa em pedaços
        df_chunk = pd.DataFrame([dict(row) for row in batch])

        if total == 0:
            df_chunk.to_csv(out_path, index=False, mode="w")
        else:
            df_chunk.to_csv(out_path, index=False, mode="a", header=False)

        total += len(df_chunk)
        print(f"[INFO] Escrevendo... total={total} linhas")

    print(f"[INFO] Finalizado. Total de linhas gravadas: {total}")
    print(f"[INFO] Arquivo salvo: {out_path}")


# ---------------------------------------------------------
# EXECUÇÃO TESTE LOCAL
# ---------------------------------------------------------
# Para teste local rápido
# python src/bq_utils.py --sql sql/populacao_mulheres_2022.sql

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Executa SQL no BigQuery e salva resultado em CSV.")
    parser.add_argument("--sql", "-s", required=True, help="Arquivo SQL.")
    parser.add_argument("--out", "-o", default="data/raw/bq_2022.csv", help="CSV de saída.")
    parser.add_argument("--ano", "-a", default="2022")
    parser.add_argument("--show-sql", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--poll-interval", type=int, default=5)
    parser.add_argument("--page-size", type=int, default=5000)

    args = parser.parse_args()

    print("\n=== Execução BigQuery → CSV ===")

    try:
        query_to_csv(
            sql_path=args.sql,
            out_path=args.out,
            ano=args.ano,
            show_sql=args.show_sql,
            dry_run=args.dry_run,
            limit=args.limit,
            timeout=args.timeout,
            poll_interval=args.poll_interval,
            page_size=args.page_size,
        )
    except Exception as exc:
        print("\n[ERRO FATAL]:", type(exc).__name__, exc)
        sys.exit(1)
