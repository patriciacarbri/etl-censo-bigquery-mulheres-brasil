from datetime import datetime
from pathlib import Path

import pandas as pd

from .config import TRUSTED_CSV_DIR, TRUSTED_PARQUET_DIR, OUTPUT_DICT_DIR
from .bq_utils import query_to_dataframe
from .config import ROOT_DIR


#SQL_2010 = ROOT_DIR / "sql" / "populacao_mulheres_2010.sql"
SQL_2022 = ROOT_DIR / "sql" / "populacao_mulheres_2022.sql"


def gerar_dicionario_dados(df: pd.DataFrame, caminho_csv: Path, caminho_md: Path) -> None:
    """
    Gera um 'data dictionary' simples a partir de um DataFrame:
    - nome da coluna
    - tipo
    - nº de nulos
    - exemplo de valor
    - descrição (placeholder para você preencher depois)
    """
    linhas = []
    for col in df.columns:
        serie = df[col]
        tipo = str(serie.dtype)
        n_nulos = int(serie.isna().sum())
        exemplo = serie.dropna().iloc[0] if serie.dropna().size > 0 else ""
        linhas.append(
            {
                "coluna": col,
                "tipo": tipo,
                "n_nulos": n_nulos,
                "exemplo_valor": str(exemplo),
                "descricao": "",  # você preenche manualmente depois
            }
        )

    dict_df = pd.DataFrame(linhas)
    dict_df.to_csv(caminho_csv, index=False, encoding="utf-8")

    # Versão em Markdown
    linhas_md = ["| coluna | tipo | n_nulos | exemplo_valor | descricao |",
                 "|--------|------|---------|---------------|-----------|"]
    for _, row in dict_df.iterrows():
        linhas_md.append(
            f"| {row['coluna']} | {row['tipo']} | {row['n_nulos']} | {row['exemplo_valor']} | {row['descricao']} |"
        )
    caminho_md.write_text("\n".join(linhas_md), encoding="utf-8")


def etl_populacao_mulheres() -> None:
    # print(">> Extraindo dados de 2010...")
    # df_2010 = query_to_dataframe(SQL_2010, ano="2010")

    print(">> Extraindo dados de 2022...")
    df_2022 = query_to_dataframe(SQL_2022, ano="2022")

    # Checa colunas básicas
    expected_cols = {"ano", "sigla_uf", "grupo_idade", "raca_cor", "total_mulheres"}
    for nome, df in [("2010", df_2010), ("2022", df_2022)]:
        missing = expected_cols - set(df.columns)
        if missing:
            raise ValueError(f"DataFrame {nome} está faltando colunas: {missing}")

    print(">> Concatenando 2010 e 2022...")
    df_all = pd.concat([df_2010, df_2022], ignore_index=True)

    # Exemplo de transformação extra: calcula percentuais dentro de cada ano+UF
    print(">> Calculando percentuais dentro de cada ano/UF...")
    df_all["total_uf_ano"] = df_all.groupby(["ano", "sigla_uf"])["total_mulheres"].transform("sum")
    df_all["perc_mulheres_uf"] = df_all["total_mulheres"] / df_all["total_uf_ano"]

    # Ordena colunas
    cols_ordem = [
        "ano",
        "sigla_uf",
        "grupo_idade",
        "raca_cor",
        "total_mulheres",
        "total_uf_ano",
        "perc_mulheres_uf",
    ]
    df_all = df_all[cols_ordem]

    # Gera timestamp para versão dos arquivos
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_path = TRUSTED_CSV_DIR / f"populacao_mulheres_2010_2022_{ts}.csv"
    parquet_path = TRUSTED_PARQUET_DIR / f"populacao_mulheres_2010_2022_{ts}.parquet"

    print(f">> Salvando CSV em {csv_path}")
    df_all.to_csv(csv_path, index=False, encoding="utf-8")

    print(f">> Salvando Parquet em {parquet_path}")
    df_all.to_parquet(parquet_path, index=False)

    # Data dictionary
    dict_csv = OUTPUT_DICT_DIR / f"dicionario_populacao_mulheres_2010_2022_{ts}.csv"
    dict_md = OUTPUT_DICT_DIR / f"dicionario_populacao_mulheres_2010_2022_{ts}.md"

    print(">> Gerando data dictionary...")
    gerar_dicionario_dados(df_all, dict_csv, dict_md)

    print(">> ETL finalizado com sucesso!")


if __name__ == "__main__":
    etl_populacao_mulheres()
