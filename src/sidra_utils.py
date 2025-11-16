# src/sidra_utils.py
import json
import argparse
from typing import Any, Optional

try:
    import sidrapy as sidra  # type: ignore
    _HAVE_SIDRA = True
except Exception:
    _HAVE_SIDRA = False

import requests

RawSidra = Any


def _sidra_get_table_with_requests(
    table_id: str,
    period: str = "2010",
    variables: Optional[str] = None,
    geo: Optional[str] = None,
    timeout: int = 60,
) -> RawSidra:
    """
    Requisição direta à API SIDRA -> retorna JSON cru.
    """
    print("\n[INFO] Usando fallback via requests (API REST SIDRA)...")
    base = "https://apisidra.ibge.gov.br/values"
    params = {
        "t": table_id,
        "v": variables or "all",
        "p": period,
        "c": "N",
        "formato": "json",
    }
    if geo:
        params["g"] = geo

    print("[INFO] Endpoint:", base)
    print("[INFO] Parâmetros:", params)
    resp = requests.get(base, params=params, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    print("[INFO] Resposta recebida (tipo):", type(data).__name__)
    if isinstance(data, list):
        print("[INFO] Total itens:", len(data))
    return data


def sidra_raw_table(
    table_id: str = "1383",
    period: str = "2010",
    territorial_level: Optional[str] = "6",          # 6 = município
    ibge_territorial_code: Optional[str] = "all",    # todos os municípios
    variable: Optional[str] = "all",                 # todas as variáveis
    geo: Optional[str] = "5565",
    use_sidrapy_if_available: bool = True,
) -> RawSidra:
    """
    Retorna os dados BRUTOS da tabela SIDRA solicitada.
    Padrão ajustado para:
      - tabela 1383
      - ano 2010
      - nível territorial 6 (município)
      - todos os códigos IBGE
    Nenhuma transformação é feita nos dados.
    """
    print("\n=== INICIANDO EXTRAÇÃO SIDRA ===")
    print(f"[INFO] Tabela: {table_id}")
    print(f"[INFO] Período: {period}")
    print(f"[INFO] territorial_level: {territorial_level}")
    print(f"[INFO] ibge_territorial_code: {ibge_territorial_code}")
    print(f"[INFO] variable: {variable}")
    print(f"[INFO] geo (para API REST): {geo}")
    print(f"[INFO] sidrapy disponível: {_HAVE_SIDRA}")

    if use_sidrapy_if_available and _HAVE_SIDRA:
        print("[INFO] Usando sidrapy.get_table(...)")

        # Garantir defaults mínimos
        territorial_level = territorial_level or "6"
        ibge_territorial_code = ibge_territorial_code or "all"
        variable = variable or "all"

        print(
            "[INFO] Parâmetros passados para sidrapy:",
            {
                "table_code": table_id,
                "territorial_level": territorial_level,
                "ibge_territorial_code": ibge_territorial_code,
                "variable": variable,
                "period": period,
            },
        )

        # Aqui passamos TODOS os argumentos obrigatórios
        data = sidra.get_table(
            table_code=table_id,
            territorial_level=territorial_level,
            ibge_territorial_code=ibge_territorial_code,
            variable=variable,
            period=period,
        )
        print("[INFO] Dados retornados pelo sidrapy (lista/dict cru).")
        return data

    print("[INFO] sidrapy não disponível ou desabilitado — usando requests.")
    return _sidra_get_table_with_requests(
        table_id=table_id,
        period=period,
        variables=variable,
        geo=geo,
    )


def _print_sample(raw: RawSidra, sample: int = 3) -> None:
    """Imprime amostra legível do JSON cru retornado."""
    print("\n=== AMOSTRA DOS DADOS BRUTOS ===")
    if isinstance(raw, list):
        print(f"[INFO] Lista com {len(raw)} itens. Mostrando primeiros {sample}:")
        print(json.dumps(raw[:sample], indent=2, ensure_ascii=False))
    elif isinstance(raw, dict):
        print("[INFO] Dict recebido. Exibindo chaves top-level e conteúdo (parcial):")
        keys = list(raw.keys())
        print("Chaves:", keys)
        to_show = {k: raw[k] for k in keys[:3]}
        print(json.dumps(to_show, indent=2, ensure_ascii=False))
    else:
        print("[INFO] Tipo inesperado:", type(raw))
        print(raw)


# -------------------------------------------------------------------------
# EXECUÇÃO DIRETA PELO TERMINAL
# -------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Teste de extração bruta SIDRA (tabela 1383 por padrão)."
    )
    parser.add_argument("--table", "-t", default="1383", help="ID da tabela SIDRA (padrão 1383).")
    parser.add_argument("--period", "-p", default="2010", help="Período/ano (ex: 2010).")
    parser.add_argument(
        "--tl",
        "--territorial-level",
        dest="territorial_level",
        default="6",
        help="Nível territorial (1=Brasil, 2=UF, 6=município; padrão 6).",
    )
    parser.add_argument(
        "--code",
        "--ibge-code",
        dest="ibge_code",
        default="all",
        help="Código territorial IBGE (padrão 'all').",
    )
    parser.add_argument(
        "--geo",
        "-g",
        default="5565",
        help="Parâmetro 'g' da API REST (usado no fallback via requests).",
    )
    parser.add_argument(
        "--no-sidrapy",
        action="store_true",
        help="Forçar uso do fallback requests mesmo se sidrapy estiver disponível.",
    )
    parser.add_argument(
        "--sample",
        "-s",
        type=int,
        default=3,
        help="Quantos itens mostrar na amostra (padrão 3).",
    )
    args = parser.parse_args()

    print("\n=== TESTE RÁPIDO: EXTRAÇÃO BRUTA SIDRA ===")
    print("Observação: este script NÃO faz nenhuma transformação nos dados (raw JSON).")

    try:
        raw = sidra_raw_table(
            table_id=args.table,
            period=args.period,
            territorial_level=args.territorial_level,
            ibge_territorial_code=args.ibge_code,
            variable="all",
            geo=args.geo,
            use_sidrapy_if_available=not args.no_sidrapy,
        )
        _print_sample(raw, sample=args.sample)
    except Exception as exc:
        print("\n[ERRO] Falha durante a extração SIDRA:")
        print(type(exc).__name__, exc)
