import pandas as pd
import sidrapy
from pathlib import Path

OUT = Path("data/raw/alfabetizacao_2010.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

def baixar_sidra_1383():
    print("[INFO] Extraindo dados da Tabela 1383 via sidrapy…")

    raw = sidrapy.get_table(
        table_code="1383",
        territorial_level="6",   # municípios
        ibge_territorial_code="all",
        variable="1646",         # taxa de alfabetização
        period="2010"            # censo 2010
    )

    df = pd.DataFrame(raw)
    print("[INFO] Dados recebidos:", df.shape)

    # Renomear campos internos
    df = df.rename(columns={
        "D1C": "id_municipio",
        "D1N": "municipio",
        "D2N": "ano",
        "D3N": "variavel",
        "D4N": "sexo",
        "V":   "valor"
    })

    # Manter apenas as colunas úteis
    df = df[["id_municipio", "municipio", "ano", "sexo", "valor"]]

    # Corrige tipos
    df["id_municipio"] = df["id_municipio"].astype(str)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    df.to_csv(OUT, index=False, encoding="utf-8")
    print(f"[OK] CSV salvo em: {OUT} ({OUT.stat().st_size} bytes)")

if __name__ == "__main__":
    baixar_sidra_1383()
