import sidrapy
import pandas as pd
import json
import os

def fetch_sidra_table(table_code: str,
                      territorial_level: str = "1",
                      ibge_territorial_code: str = "all",
                      variable: str = "all",
                      period: str = "all",
                      classifications: dict = None) -> pd.DataFrame:
    """
    Busca dados da tabela SIDRA via sidrapy e retorna DataFrame bruto.
    """
    df = sidrapy.get_table(table_code=table_code,
                           territorial_level=territorial_level,
                           ibge_territorial_code=ibge_territorial_code,
                           variable=variable,
                           period=period,
                           classifications=classifications)
    return df

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpeza básica: ajustar cabeçalho, converter valores etc.
    """
    # Usa primeira linha como cabeçalho se necessário
    df.columns = df.iloc[0]
    df = df.drop(index=0).reset_index(drop=True)
    # Converter coluna "Valor" para numérico se existir
    if "Valor" in df.columns:
        df["Valor"] = pd.to_numeric(df["Valor"].str.replace(",", "."), errors="coerce")
    return df

def save_to_json(df: pd.DataFrame, output_path: str):
    """
    Salva DataFrame em JSON orientado a registros.
    """
    records = df.to_dict(orient="records")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

def main():
    table_code = "1134"  # a tabela que você pediu
    # Aqui ajuste conforme o nível territorial que você quer: Brasil, UF, Município etc
    territorial_level = "1"  # exemplo: 1 = Brasil
    ibge_territorial_code = "all"
    variable = "all"
    period = "all"
    classifications = None
    # Se souber classificações específicas da tabela 1134, ajuste abaixo, ex:
    # classifications = {"11278": "33460", "166": "3067,3327"}

    print(f"Buscando dados da tabela {table_code}")
    df_raw = fetch_sidra_table(table_code=table_code,
                                territorial_level=territorial_level,
                                ibge_territorial_code=ibge_territorial_code,
                                variable=variable,
                                period=period,
                                classifications=classifications)
    print("Dados brutos obtidos. Limpando…")
    df_clean = clean_dataframe(df_raw)
    output_file = f"sidra_table_{table_code}.json"
    print(f"Salvando arquivo {output_file}")
    save_to_json(df_clean, output_file)
    print("Concluído.")

if __name__ == "__main__":
    main()
