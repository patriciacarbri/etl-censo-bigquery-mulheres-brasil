import pandas as pd
import os
import glob
import numpy as np

# --- Configurações de Caminhos ---
# Diretório onde estão os arquivos Parquet tratados
INPUT_DIR = 'data/trusted/parquet/'
# Diretório onde os dicionários serão salvos
OUTPUT_DIR = 'data/trusted/dicionario/'

def generate_data_dictionary(df, file_name):
    """
    Analisa um DataFrame e gera o conteúdo do dicionário de dados em formato Markdown.
    """
    
    # 1. Estrutura do Dicionário
    data_dict = []
    
    # Processar cada coluna
    for col in df.columns:
        # Tipo de Dado (dtype)
        data_type = str(df[col].dtype)
        
        # Contagem de Nulos
        n_nulos = df[col].isnull().sum()
        
        # Exemplo de Valor (pega o primeiro valor não nulo)
        exemplo_series = df[col].dropna()
        if not exemplo_series.empty:
            # Pega o primeiro valor e converte para string
            exemplo_valor = str(exemplo_series.iloc[0])
            # Limita a 50 caracteres para manter a tabela limpa
            if len(exemplo_valor) > 50:
                exemplo_valor = exemplo_valor[:47] + "..."
        else:
            exemplo_valor = "N/A (Todos nulos)"
            
        # Descrição (Placeholder, deve ser preenchida manualmente depois)
        descricao = "A ser preenchida."
        
        data_dict.append({
            'coluna': col,
            'tipo': data_type,
            'n_nulos': n_nulos,
            'exemplo_valor': exemplo_valor,
            'descricao': descricao
        })

    # 2. Criar o DataFrame do Dicionário para fácil conversão
    df_dict = pd.DataFrame(data_dict)
    
    # 3. Gerar o conteúdo Markdown
    
    # Título
    markdown_content = f"# Dicionário de Dados: {file_name}\n\n"
    markdown_content += "Este dicionário descreve as colunas do arquivo Parquet tratado.\n\n"

    # Tabela Markdown
    # Converte o DataFrame do dicionário para a tabela Markdown
    markdown_content += df_dict.to_markdown(index=False)
    
    return markdown_content

# ====================================================================
## EXECUÇÃO DO SCRIPT
# ====================================================================

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
        # Extrai apenas o nome do arquivo (ex: 'bq_2022_trusted.parquet')
        file_name = os.path.basename(file_path)
        # Cria o nome do arquivo de saída (ex: 'bq_2022_trusted_dicionario.md')
        output_file_name = file_name.replace('.parquet', '_dicionario.md')
        output_path = os.path.join(OUTPUT_DIR, output_file_name)
        
        print(f"\n--- Processando {file_name} ---")
        
        try:
            # Lê o arquivo Parquet
            df = pd.read_parquet(file_path)
            
            # Gera o conteúdo do dicionário
            markdown_output = generate_data_dictionary(df, file_name)
            
            # Salva o arquivo Markdown
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(markdown_output)
                
            print(f"Dicionário salvo com sucesso em: {output_path}")

        except Exception as e:
            print(f"FALHA ao processar {file_name}: {e}")