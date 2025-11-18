# src/dicionarioDados.py

import pandas as pd
import os
import glob
import numpy as np

# --- Configurações de Caminhos ---
# VARIÁVEIS GLOBAIS
INPUT_DIR = 'data/trusted/parquet/'
OUTPUT_DIR = 'data/trusted/dicionario/' 

def generate_data_dictionary(df, file_name):
    """
    Analisa um DataFrame e gera o conteúdo do dicionário de dados em formato Markdown.
    """
    
    data_dict = []
    for col in df.columns:
        data_type = str(df[col].dtype)
        n_nulos = df[col].isnull().sum()
        exemplo_series = df[col].dropna()
        
        exemplo_valor = "N/A (Todos nulos)"
        if not exemplo_series.empty:
            exemplo_valor = str(exemplo_series.iloc[0])
            if len(exemplo_valor) > 50:
                exemplo_valor = exemplo_valor[:47] + "..."
        
        descricao = "A ser preenchida."
        
        data_dict.append({
            'coluna': col, 'tipo': data_type, 'n_nulos': n_nulos, 
            'exemplo_valor': exemplo_valor, 'descricao': descricao
        })

    df_dict = pd.DataFrame(data_dict)
    
    markdown_content = f"# Dicionário de Dados: {file_name}\n\n"
    markdown_content += "Este dicionário descreve as colunas do arquivo Parquet tratado.\n\n"
    markdown_content += df_dict.to_markdown(index=False)
    
    return markdown_content

# ====================================================================
## FUNÇÃO DE ORQUESTRAÇÃO LOCAL
# ====================================================================

def run_dicionario_dados():
    """Busca arquivos Parquet e gera os dicionários de dados."""
    
    # 1. Cria o diretório de saída se ele não existir
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Diretório de saída '{OUTPUT_DIR}' criado.")

    # 2. Busca todos os arquivos Parquet no diretório de entrada
    parquet_files = glob.glob(os.path.join(INPUT_DIR, '*.parquet'))

    if not parquet_files:
        print(f"ERRO: Nenhum arquivo Parquet encontrado em {INPUT_DIR}")
    else:
        print(f"Encontrados {len(parquet_files)} arquivos para processamento.")
        
        for file_path in parquet_files:
            file_name = os.path.basename(file_path)
            output_file_name = file_name.replace('.parquet', '_dicionario.md')
            output_path = os.path.join(OUTPUT_DIR, output_file_name)
            
            print(f"\n--- Processando {file_name} ---")
            
            try:
                df = pd.read_parquet(file_path)
                markdown_output = generate_data_dictionary(df, file_name)
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(markdown_output)
                    
                print(f"Dicionário salvo com sucesso em: {output_path}")

            except Exception as e:
                print(f"FALHA ao processar {file_name}: {e}")

if __name__ == "__main__":
    run_dicionario_dados()