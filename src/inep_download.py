import pandas as pd
import requests
import zipfile
import io
import os
import tempfile # Importa a biblioteca de arquivos temporários

# --- 1. Configuração ---

# URL para o arquivo ZIP
zip_url = "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2022.zip"

# Caminho para o arquivo CSV dentro do ZIP
# Há caracteres especiais no nome do arquivo (╞o = ã)
path_to_csv_in_zip = "Microdados do Censo Escolar da Educaç╞o Básica 2022/dados/microdados_ed_basica_2022.csv"

# Colunas a serem selecionadas
colunas_selecionadas = [
    'NU_ANO_CENSO', 'NO_REGIAO', 'CO_REGIAO', 'NO_UF', 'SG_UF', 'CO_UF',
    'NO_MUNICIPIO', 'CO_MUNICIPIO', 'QT_MAT_BAS', 'QT_MAT_INF',
    'QT_MAT_INF_CRE', 'QT_MAT_INF_PRE', 'QT_MAT_FUND', 'QT_MAT_FUND_AI',
    'QT_MAT_FUND_AF', 'QT_MAT_MED', 'QT_MAT_PROF', 'QT_MAT_PROF_TEC',
    'QT_MAT_EJA', 'QT_MAT_EJA_FUND', 'QT_MAT_EJA_MED', 'QT_MAT_ESP',
    'QT_MAT_ESP_CC', 'QT_MAT_ESP_CE', 'QT_MAT_BAS_FEM', 'QT_MAT_BAS_MASC',
    'QT_MAT_BAS_ND', 'QT_MAT_BAS_BRANCA', 'QT_MAT_BAS_PRETA',
    'QT_MAT_BAS_PARDA', 'QT_MAT_BAS_AMARELA', 'QT_MAT_BAS_INDIGENA'
]

# Caminho de saída
output_dir = "data/trusted"
output_filename = "inep_MicrodadosEducaBasica2022.parquet"
output_path = os.path.join(output_dir, output_filename)

# --- 2. Execução ---

print(f"Iniciando o processo...")
zip_file_object = None # Para podermos listar os arquivos no erro

# Usa um gerenciador de contexto para o arquivo temporário
# Ele será criado e excluído automaticamente
with tempfile.NamedTemporaryFile() as temp_zip_file:
    try:
        # --- Etapa 1: Baixar para arquivo temporário ---
        print(f"Baixando dados de {zip_url}...")
        
        with requests.get(zip_url, stream=True) as response:
            # Lança um erro se o download falhar (ex: 404)
            response.raise_for_status()
            
            # Salva o arquivo em partes (chunks) no arquivo temporário
            # Isso é robusto para arquivos grandes
            for chunk in response.iter_content(chunk_size=8192):
                temp_zip_file.write(chunk)
        
        print("Download concluído. Arquivo salvo temporariamente em disco.")

        # --- Etapa 2: Abrir o ZIP a partir do arquivo em disco ---
        print("Abrindo o arquivo ZIP...")
        zip_file_object = zipfile.ZipFile(temp_zip_file.name)

        # --- Etapas 3 e 4: Ler CSV para DataFrame ---
        print(f"Lendo o arquivo {path_to_csv_in_zip} de dentro do ZIP...")
        
        with zip_file_object.open(path_to_csv_in_zip) as csv_file:
            df = pd.read_csv(
                csv_file,
                sep=';',
                encoding='latin1'
            )
        
        print(f"DataFrame carregado com {df.shape[0]} linhas e {df.shape[1]} colunas.")

        # --- Etapa 5: Selecionar colunas ---
        print(f"Selecionando as {len(colunas_selecionadas)} colunas especificadas...")
        
        # (O resto do script é o mesmo)
        cols_faltando = [col for col in colunas_selecionadas if col not in df.columns]
        if cols_faltando:
            print(f"Atenção: As seguintes colunas não foram encontradas: {cols_faltando}")
            colunas_existentes = [col for col in colunas_selecionadas if col in df.columns]
            df_filtrado = df[colunas_existentes].copy()
        else:
            df_filtrado = df[colunas_selecionadas].copy()

        print(f"DataFrame filtrado. Novas dimensões: {df_filtrado.shape}")

        # --- Etapa 6: Salvar em Parquet ---
        print(f"Preparando para salvar em {output_path}...")
        os.makedirs(output_dir, exist_ok=True)
        df_filtrado.to_parquet(output_path, engine='pyarrow', index=False)
        
        print("---")
        print("Processo concluído com sucesso!")
        print(f"Arquivo salvo em: {output_path}")

    except requests.exceptions.RequestException as e:
        print(f"Erro no download (Rede/Conexão): {e}")
        print("A conexão pode ter caído. Tente novamente.")
    
    except zipfile.BadZipFile:
        print("Erro: O arquivo baixado é um ZIP inválido ou corrompido.")
        print("Isso geralmente acontece por um download incompleto (erro de rede).")
    
    except KeyError as e:
        print(f"Erro: O arquivo CSV não foi encontrado dentro do ZIP. Verifique o caminho: {e}")
        print("O download parece ter funcionado, mas o caminho do arquivo CSV está incorreto.")
        
        # --- AJUDA PARA DEBUG ---
        # Se o caminho estiver errado, isso listará todos os arquivos dentro do ZIP
        if zip_file_object:
            print("\n--- Arquivos encontrados no ZIP ---")
            for f_name in zip_file_object.namelist():
                print(f_name)
            print("------------------------------------")
            print(f"Verifique se o caminho '{path_to_csv_in_zip}' está correto.")
        
    except pd.errors.ParserError as e:
        print(f"Erro ao ler o CSV: {e}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")