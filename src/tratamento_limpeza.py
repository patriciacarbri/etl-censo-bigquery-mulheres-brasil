# src/tratamento_limpeza.py

import pandas as pd
import os

# --- Configuração de Caminhos ---
OUTPUT_DIR = 'data/trusted/parquet/'

def salvar_dataframe(df, nome_arquivo, output_dir=OUTPUT_DIR):
    """Salva o DataFrame no formato Parquet no diretório especificado."""
    if df is None:
        print(f"AVISO: O DataFrame para {nome_arquivo} está vazio e não será salvo.")
        return
        
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    caminho_saida = os.path.join(output_dir, nome_arquivo + '.parquet')
    df.to_parquet(caminho_saida, index=False)
    print(f"DataFrame '{nome_arquivo}' salvo em: {caminho_saida}")


## FUNÇÕES DE TRATAMENTO 
def tratar_big_query():
    """Importa o arquivo CSV, converte colunas de população para Int64."""
    print("\n--- Processando Big Query (df_bq) ---")
    caminho_dados = 'data/raw/bq_2022.csv'
    try:
        df = pd.read_csv(caminho_dados)
    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado em {caminho_dados}")
        return None
    
    colunas_para_converter = [
        'populacao_homens', 'populacao_mulheres', 'populacao_total'
    ]
    
    for coluna in colunas_para_converter:
        df[coluna] = df[coluna].astype('Int64')

    print("Conversão de tipos concluída.")
    return df

def tratar_sidra():
    """Importa o arquivo CSV e remove a linha de cabeçalho extra (índice 0)."""
    print("\n--- Processando Sidra (df_sidra) ---")
    caminho_dados = 'data/raw/alfabetizacao_2010.csv'
    try:
        df2 = pd.read_csv(caminho_dados)
    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado em {caminho_dados}")
        return None
    
    df2.drop(0, axis=0, inplace=True)
    df2.reset_index(drop=True, inplace=True)
    
    print(f"Linha de cabeçalho extra (índice 0) removida.")
    return df2

def tratar_inep():
    """Importa o arquivo Parquet, remove linhas com nulos e renomeia coluna."""
    print("\n--- Processando INEP (df_inep) ---")
    caminho_dados = 'data/raw/inep_MicrodadosEducaBasica2022.parquet'
    try:
        df3 = pd.read_parquet(caminho_dados)
    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado em {caminho_dados}. Verifique a instalação do 'pyarrow'.")
        return None
    
    linhas_originais = len(df3)
    df3_limpo = df3.dropna(how='any')
    linhas_removidas = linhas_originais - len(df3_limpo)
    
    df3_limpo = df3_limpo.rename(columns={'CO_MUNICIPIO': 'id_municipio'})
    
    print(f"Linhas removidas devido a Nulos: {linhas_removidas}")
    print("Coluna 'CO_MUNICIPIO' renomeada.")
    return df3_limpo


# ====================================================================
## FUNÇÃO DE ORQUESTRAÇÃO LOCAL
# ====================================================================

def run_tratamento_limpeza():
    """Orquestra o tratamento, limpeza e salvamento dos DataFrames."""
    
    # 1. Executa as funções de tratamento
    df_bq = tratar_big_query()
    df_sidra = tratar_sidra()
    df_inep = tratar_inep()

    print("\n" + "="*30)
    print("INICIANDO SALVAMENTO DOS ARQUIVOS LIMPOS")
    print("="*30)
    
    # 2. Salva cada DataFrame tratado
    salvar_dataframe(df_bq, 'bq_2022_trusted')
    salvar_dataframe(df_sidra, 'sidra_2010_trusted')
    salvar_dataframe(df_inep, 'inep_2022_trusted')


if __name__ == "__main__":
    run_tratamento_limpeza()