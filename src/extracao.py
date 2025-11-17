from pathlib import Path
import argparse
from src.sidra_utils import sidra_raw_table
from src.bq_utils import query_to_csv


def run_sidra(out_path: Path):
    print("[SIDRA] Iniciando...")
    try:
        dados = sidra_raw_table(
            table_id="1383",
            period="2010",
            territorial_level="6",
            ibge_territorial_code="all",
            variable="all",
            geo="5565",
            use_sidrapy_if_available=True,
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(str(dados), encoding="utf-8")
        print(f"[SIDRA] OK → {out_path}")
    except Exception as e:
        print("[SIDRA] ERRO:", e)


def run_bigquery(sql_path: Path, out_path: Path, ano: str):
    print("[BQ] Iniciando...")
    try:
        query_to_csv(
            sql_path=sql_path,
            out_path=out_path,
            ano=ano,
            show_sql=False,
            dry_run=False,
            limit=None,
        )
        print(f"[BQ] OK → {out_path}")
    except Exception as e:
        print("[BQ] ERRO:", e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orquestra extração SIDRA + BigQuery.")

    parser.add_argument("--sidra-out", default="data/raw/sidra_2010.json")
    parser.add_argument("--bq-sql", default="sql/populacao_mulheres_2022.sql")
    parser.add_argument("--bq-out", default="data/raw/bq_2022.csv")
    parser.add_argument("--ano", default="2022")

    args = parser.parse_args()

    run_sidra(Path(args.sidra_out))
    run_bigquery(Path(args.bq_sql), Path(args.bq_out), args.ano)

    print("\n[FINALIZADO]")
