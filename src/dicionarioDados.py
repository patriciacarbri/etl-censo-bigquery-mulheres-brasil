# src/etl_mulheres.py
from datetime import datetime
from pathlib import Path
import sys
import os

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

import pandas as pd

from src.config import TRUSTED_CSV_DIR, TRUSTED_PARQUET_DIR, OUTPUT_DICT_DIR, ROOT_DIR
from src.bq_utils import query_to_dataframe
from src.sidra_utils import extrair_sidra_populacao_2010, normalize_sidra_df

SQL_2022 = ROOT_DIR / "sql" / "populacao_mulheres_2022.sql"

def gerar_dicionario_dados(df: pd.DataFrame, caminho_csv: Path, caminho_md: Path) -> None:
    linhas = []
    for col in df.columns:
        serie = df[col]
        tipo = str(serie.dtype)
        n_nulos = int(serie.isna().sum())
        exemplo = serie.dropna().iloc[0] if serie.dropna().size > 0 else ""
        linhas.append({
            "coluna": col,
            "tipo": tipo,
            "n_nulos": n_nulos,
            "exemplo_valor": str(exemplo),
            "descricao": "",
        })
    dict_df = pd.DataFrame(linhas)
    dict_df.to_csv(caminho_csv, index=False, encoding="utf-8")
    linhas_md = ["| coluna | tipo | n_nulos | exemplo_valor | descricao |", "|--------|------|---------|---------------|-----------|"]
    for _, row in dict_df.iterrows():
        linhas_md.append(f"| {row['coluna']} | {row['tipo']} | {row['n_nulos']} | {row['exemplo_valor']} | {row['descricao']} |")
    caminho_md.write_text("\n".join(linhas_md), encoding="utf-8")

def normalize_bq_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # mapeamento para as colunas esperadas (ajuste conforme seu SQL)
    # Ex.: se SQL já retorna ano, sigla_uf, grupo_idade, raca_cor, total_mulheres então só garante tipos
    for col in ['ano', 'sigla_uf', 'grupo_idade', 'raca_cor', 'total_mulheres']:
        if col not in df.columns:
            df[col] = pd.NA
    # converter total_mulheres para int
    df['total_mulheres'] = pd.to_numeric(df['total_mulheres'], errors='coerce').fillna(0).astype(int)
    return df[['ano', 'sigla_uf', 'grupo_idade', 'raca_cor', 'total_mulheres']]

def etl_populacao_mulheres():
    # 1) SIDRA 2010
    print(">> Extraindo SIDRA 2010...")
    try:
        raw_2010 = extrair_sidra_populacao_2010(table_id="1134", periodo="2010")
        df_2010 = normalize_sidra_df(raw_2010)
        print(">> SIDRA 2010 extraído:", len(df_2010), "linhas")
    except Exception as e:
        print("!! Erro extraindo SIDRA 2010:", e)
        df_2010 = None

    # 2) BigQuery 2022
    print(">> Extraindo dados de 2022...")
    try:
        df_2022 = query_to_dataframe(SQL_2022, ano="2022")
        df_2022 = normalize_bq_df(df_2022)
        print(">> BigQuery 2022 extraído:", len(df_2022), "linhas")
    except Exception as e:
        print("!! Erro extraindo BigQuery 2022:", e)
        df_2022 = None

    if df_2010 is None and df_2022 is None:
        raise RuntimeError("Nenhuma fonte foi extraída com sucesso. Abortando.")

    dfs = [d for d in [df_2010, df_2022] if d is not None]
    df_all = pd.concat(dfs, ignore_index=True)

    # drop rows sem uf
    df_all = df_all[df_all['sigla_uf'].notna()]

    # calcula percentuais
    df_all['total_uf_ano'] = df_all.groupby(['ano', 'sigla_uf'])['total_mulheres'].transform('sum')
    df_all['perc_mulheres_uf'] = df_all.apply(
        lambda r: (r['total_mulheres'] / r['total_uf_ano']) if r['total_uf_ano'] and r['total_uf_ano'] > 0 else 0, axis=1
    )

    cols_ordem = ["ano", "sigla_uf", "grupo_idade", "raca_cor", "total_mulheres", "total_uf_ano", "perc_mulheres_uf"]
    df_all = df_all[cols_ordem]

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = TRUSTED_CSV_DIR / f"populacao_mulheres_2010_2022_{ts}.csv"
    parquet_path = TRUSTED_PARQUET_DIR / f"populacao_mulheres_2010_2022_{ts}.parquet"

    print(f">> Salvando CSV em {csv_path}")
    df_all.to_csv(csv_path, index=False, encoding="utf-8")
    print(f">> Salvando Parquet em {parquet_path}")
    df_all.to_parquet(parquet_path, index=False)

    dict_csv = OUTPUT_DICT_DIR / f"dicionario_populacao_mulheres_2010_2022_{ts}.csv"
    dict_md = OUTPUT_DICT_DIR / f"dicionario_populacao_mulheres_2010_2022_{ts}.md"
    print(">> Gerando data dictionary...")
    gerar_dicionario_dados(df_all, dict_csv, dict_md)

    print(">> ETL finalizado. Arquivos gerados:")
    print("   -", csv_path)
    print("   -", parquet_path)
    print("   -", dict_csv)
    print("   -", dict_md)

if __name__ == "__main__":
    etl_populacao_mulheres()
