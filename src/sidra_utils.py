# src/sidra_utils.py
from pathlib import Path
import pandas as pd

try:
    import sidrapy as sidra
    _HAVE_SIDRA = True
except Exception:
    _HAVE_SIDRA = False

import requests
import time

def _sidra_get_table_with_requests(table_id: str, period: str = "2010", variables: str = None, geo: str = None) -> pd.DataFrame:
    """
    Fetch SIDRA table via requests (CSV format) as fallback.
    variables and geo can be used to narrow the query; keep simple for now.
    """
    base = "https://apisidra.ibge.gov.br/values"
    params = {
        "t": table_id,
        "v": variables or "all",
        "p": period,
        "c": "N",  # sem comentarios
        "formato": "json"
    }
    # If geo provided, add it (e.g., 'false' for Brasil, or 'all' etc.)
    if geo:
        params['g'] = geo
    resp = requests.get(base, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    # SIDRA returns a list of dicts where first row is header metadata; convert to DataFrame
    # The JSON format often is a list of dicts with keys corresponding to variables.
    # If structure is different, user should inspect resp.json() and adapt.
    try:
        df = pd.DataFrame(data)
    except Exception:
        # If not a clean table, return empty df
        df = pd.DataFrame()
    return df

def extrair_sidra_populacao_2010(table_id: str = "1134", periodo: str = "2010") -> pd.DataFrame:
    """
    Extrai dados do SIDRA para o Censo 2010 usando sidrapy (nova API).
    Caso sidrapy não esteja disponível, usa fallback via requests.
    """
    if _HAVE_SIDRA:
        print(f">> SIDRA: usando sidrapy para tabela {table_id} periodo {periodo}")

        df = sidra.get_table(
            table_code=table_id,
            territorial_level="1",            # 1 = Brasil; 2 = UF; 6 = município
            ibge_territorial_code="all",      # all = todos os códigos do nível
            variable="all",                   # todas as variáveis
            period=periodo                    # ex: "2010"
        )

        # sidrapy retorna LISTA de DICTS -> converter para DataFrame
        return pd.DataFrame(df)

    else:
        print(f">> SIDRA: sidrapy não disponível, usando requests para tabela {table_id} periodo {periodo}")
        time.sleep(0.5)
        return _sidra_get_table_with_requests(table_id, period=periodo)

def normalize_sidra_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza colunas do DataFrame SIDRA para o padrão:
    ano, sigla_uf, grupo_idade, raca_cor, total_mulheres
    Esse mapeamento depende do retorno da tabela sidra escolhida.
    Ajuste conforme o seu caso.
    """
    df = df.copy()
    # Tenta detectar colunas comuns e mapear
    col_map = {}

    # Possíveis colunas: 'V001' (valor), 'D1C' or 'Município' etc.
    # Exemplo: se existir 'UF' -> 'sigla_uf'
    for c in df.columns:
        cl = c.lower()
        if cl in ("uf", "unidade da federação", "coduf", "sigla_uf"):
            col_map[c] = "sigla_uf"
        if "idade" in cl or "faixa" in cl:
            col_map[c] = "grupo_idade"
        if "sexo" in cl:
            col_map[c] = "sexo"
        if "cor" in cl or "raça" in cl or "raca" in cl:
            col_map[c] = "raca_cor"
        # Valor: muitas tabelas SIDRA usam 'V1' ou 'V001' ou 'Valor'
        if cl.startswith("v") and cl.lstrip("v").isdigit():
            col_map[c] = "total"
        if cl in ("valor", "valor (pessoas)", "V001"):
            col_map[c] = "total"

    # Aplica o rename
    df = df.rename(columns=col_map)

    # Se não tiver coluna 'sexo' mas tiver nome 'Sexo' com valores 'Mulheres', etc.,
    # isso já mapeado acima. Filtra sexo feminino (ajuste se necessário)
    if "sexo" in df.columns:
        df = df[df["sexo"].astype(str).str.lower().str.contains("mulher|f", na=False)]

    # Adiciona ano
    if "Ano" in df.columns:
        df["ano"] = df["Ano"]
    elif "ano" not in df.columns:
        # se não houver, inferir do período (padrão 2010)
        df["ano"] = 2010

    # Garante colunas esperadas
    if "total" in df.columns:
        df["total_mulheres"] = pd.to_numeric(df["total"], errors="coerce").fillna(0).astype(int)
    elif "V001" in df.columns:
        df["total_mulheres"] = pd.to_numeric(df["V001"], errors="coerce").fillna(0).astype(int)
    else:
        # se não achou total, tenta todas as colunas numéricas
        nums = df.select_dtypes(include=["number"]).columns
        if len(nums) > 0:
            df["total_mulheres"] = df[nums[0]].fillna(0).astype(int)
        else:
            df["total_mulheres"] = 0

    # Garantir sigla_uf e grupo_idade e raca_cor
    for col in ["sigla_uf", "grupo_idade", "raca_cor"]:
        if col not in df.columns:
            df[col] = pd.NA

    return df[["ano", "sigla_uf", "grupo_idade", "raca_cor", "total_mulheres"]]
