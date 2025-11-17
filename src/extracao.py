#!/usr/bin/env python3
# src/extracao.py
"""
Orquestrador simples e objetivo (pt-BR).
- Quando --all é usado, usa por padrão sql/populacao_mulheres_2022.sql para a etapa BQ, se existir.
- Mantém comportamento claro: --inep, --sidra, --bq.
Uso:
    python src/extracao.py --all
    python src/extracao.py --bq --sql sql/populacao_mulheres_2022.sql --out data/raw/bq.csv
"""

import argparse
import sys
from pathlib import Path

# tenta importar módulos locais (assume nomes em src/)
try:
    from src import inep_download as inep_mod
except Exception:
    try:
        import inep_download as inep_mod
    except Exception:
        inep_mod = None

try:
    from src import sidra_utils as sidra_mod
except Exception:
    try:
        import sidra_utils as sidra_mod
    except Exception:
        sidra_mod = None

try:
    from src import bq_utils as bq_mod
except Exception:
    try:
        import bq_utils as bq_mod
    except Exception:
        bq_mod = None

DEFAULT_SQL = Path("sql") / "populacao_mulheres_2022.sql"

def run_inep():
    if inep_mod is None:
        print("[ERRO] modulo inep_download não encontrado. Verifique src/inep_download.py")
        return 1
    print("[OK] INEP: iniciando extração (microdados)...")
    if hasattr(inep_mod, "main"):
        inep_mod.main()
        return 0
    else:
        # tenta executar arquivo como script
        script_path = Path("src") / "inep_download.py"
        if script_path.exists():
            import runpy
            runpy.run_path(str(script_path), run_name="__main__")
            return 0
        print("[ERRO] inep_download sem função main() e arquivo não encontrado.")
        return 2

def run_sidra():
    if sidra_mod is None:
        print("[ERRO] modulo sidra_utils não encontrado. Verifique src/sidra_utils.py")
        return 1
    print("[OK] SIDRA: iniciando extração (tabela exemplo)...")
    # chama função pública conhecida ou falha com mensagem clara
    if hasattr(sidra_mod, "baixar_sidra_1383"):
        sidra_mod.baixar_sidra_1383()
        return 0
    # tenta função alternativa com nome genérico
    if hasattr(sidra_mod, "main"):
        sidra_mod.main()
        return 0
    print("[ERRO] sidra_utils não expõe função esperada (baixar_sidra_1383 ou main).")
    return 2

def run_bq(sql_path: Path, out_path: Path, ano="2022", limit=None, dry_run=False):
    if bq_mod is None:
        print("[ERRO] modulo bq_utils não encontrado. Verifique src/bq_utils.py")
        return 1
    if not sql_path or not sql_path.exists():
        print(f"[ERRO] arquivo SQL não encontrado: {sql_path}")
        return 2
    print(f"[OK] BQ: executando query -> {out_path} (sql={sql_path})")
    try:
        # espera-se que bq_utils disponha de função query_to_csv(sql_path, out_path, ano, dry_run, limit)
        if hasattr(bq_mod, "query_to_csv"):
            bq_mod.query_to_csv(sql_path=sql_path, out_path=out_path, ano=ano, dry_run=dry_run, limit=limit)
            return 0
        else:
            print("[ERRO] bq_utils não expõe query_to_csv(sql_path, out_path, ...).")
            return 3
    except Exception as e:
        print("[ERRO] Falha ao executar bq_utils.query_to_csv():", e)
        return 4

def main(argv=None):
    p = argparse.ArgumentParser(description="Orquestrador simples de extração (INEP, SIDRA, BQ).")
    p.add_argument("--inep", action="store_true", help="Executa extração INEP (microdados).")
    p.add_argument("--sidra", action="store_true", help="Executa extração SIDRA (exemplo).")
    p.add_argument("--bq", action="store_true", help="Executa extração via BigQuery (usa --sql e --out ou default).")
    p.add_argument("--all", action="store_true", help="Executa todas as extrações: inep + sidra + bq (usa default SQL se necessário).")
    p.add_argument("--sql", help="Arquivo SQL para --bq (ex: sql/populacao_mulheres_2022.sql)")
    p.add_argument("--out", help="Arquivo de saída para --bq (ex: data/raw/bq_2022.csv)", default="data/raw/bq_2022.csv")
    p.add_argument("--ano", help="Ano para consultas (padrão 2022)", default="2022")
    p.add_argument("--limit", type=int, help="Limit para testes", default=None)
    p.add_argument("--dry-run", action="store_true", help="Se setado, não executa a query no BQ (apenas mostra SQL)")
    args = p.parse_args(argv)

    rc = 0

    # Se --all e não foi passado --sql explicitamente, tenta DEFAULT_SQL
    sql_path = Path(args.sql) if args.sql else None
    if args.all and sql_path is None and DEFAULT_SQL.exists():
        sql_path = DEFAULT_SQL
        print(f"[INFO] --all: usando SQL padrão: {sql_path}")

    # Executa as etapas conforme flags
    if args.inep or args.all:
        rc |= run_inep()
    if args.sidra or args.all:
        rc |= run_sidra()
    if args.bq or args.all:
        if sql_path is None:
            print("[ERRO] Para executar BQ é necessário informar --sql <arquivo.sql> ou garantir sql/populacao_mulheres_2022.sql existe.")
            return 5
        out_path = Path(args.out)
        rc |= run_bq(sql_path=sql_path, out_path=out_path, ano=args.ano, limit=args.limit, dry_run=args.dry_run)

    if not (args.inep or args.sidra or args.bq or args.all):
        print("Nada para fazer. Use --inep, --sidra, --bq ou --all. Exemplo: --all")
        return 6

    return rc

if __name__ == "__main__":
    sys.exit(main())
