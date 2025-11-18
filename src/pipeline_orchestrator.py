# src/pipeline_orchestrator.py

import extracao
import tratamento_limpeza
import dicionarioDados
import sys

def run_pipeline():
    """
    Orquestra a execução sequencial das etapas de ETL (Extração, Transformação e Documentação).
    """
    print("==============================================")
    print("🚀 INICIANDO PIPELINE DE DADOS")
    print("==============================================")
    
    # 1. ETAPA DE EXTRAÇÃO
    try:
        print("\n--- 1. Extraindo Dados (extracao.py) ---")
        # Chama a função principal 'main' do extracao.py, passando o argumento '--all'
        # O argumento 'argv' é usado para simular a chamada via terminal: python extracao.py --all
        extracao.main(argv=['--all']) 
        print("✅ Extração concluída.")
    except Exception as e:
        print(f"❌ Erro na extração. Interrompendo pipeline. Detalhe: {e}")
        # Retorna um código de erro para o sistema operacional
        sys.exit(1) 
    
    # 2. ETAPA DE TRATAMENTO E LIMPEZA
    try:
        print("\n--- 2. Tratando e Limpando Dados (tratamento_limpeza.py) ---")
        # Chama a função encapsulada
        tratamento_limpeza.run_tratamento_limpeza()
        print("✅ Tratamento e Limpeza concluídos.")
    except Exception as e:
        print(f"❌ Erro no tratamento. Interrompendo pipeline. Detalhe: {e}")
        sys.exit(1)
        
    # 3. ETAPA DE GERAÇÃO DO DICIONÁRIO DE DADOS
    try:
        print("\n--- 3. Gerando Dicionário de Dados (dicionarioDados.py) ---")
        # Chama a função encapsulada
        dicionarioDados.run_dicionario_dados()
        print("✅ Geração do Dicionário de Dados concluída.")
    except Exception as e:
        print(f"❌ Erro na geração do dicionário. Detalhe: {e}")
        # Permite continuar se for um erro de documentação, mas registra
        
    print("\n==============================================")
    print("✨ PIPELINE DE DADOS CONCLUÍDO COM SUCESSO!")
    print("==============================================")


if __name__ == "__main__":
    run_pipeline()