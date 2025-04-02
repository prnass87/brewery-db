# Importa Path para manipulação de diretórios de forma robusta
from pathlib import Path

# Importa sys para incluir o diretório raiz no PYTHONPATH dinamicamente
import sys

# Define o diretório raiz do projeto (dois níveis acima deste arquivo)
ROOT_DIR = Path(__file__).resolve().parent.parent

# Adiciona o diretório raiz ao sys.path para permitir importações absolutas corretas
# Isso garante que módulos como src/ e notebooks/ sejam reconhecidos ao rodar via terminal ou Airflow
sys.path.append(str(ROOT_DIR))

# Importa as funções das quatro etapas do pipeline
from src.extractor import extract_brewery_data
from notebooks.bronze.stg_brz_brewerylist import transform_bronze
from notebooks.silver.brz_slv_brewerylist import transform_silver
from notebooks.gold.slv_gld_brewerylist import transform_gold

def run_pipeline():
    """
    Executa o pipeline completo em sequência:
    1. Extração da API (camada Staging)
    2. Transformação Bronze
    3. Transformação Silver
    4. Transformação Gold (Parquet + Excel + SQL Server)
    """

    print("Iniciando pipeline de dados...\n")

    print("[1/4] Extraindo dados da API...")
    extract_brewery_data()  # Coleta e salva arquivo Parquet da API

    print("[2/4] Transformando dados - camada Bronze...")
    transform_bronze()  # Pré-processa e organiza os dados da API

    print("[3/4] Transformando dados - camada Silver...")
    transform_silver()  # Limpa e estrutura os dados com particionamento

    print("[4/4] Transformando dados - camada Gold (SQL Server)...")
    transform_gold()  # Agrega e exporta para Parquet, Excel e banco

    print("\n✅ Pipeline completo executado com sucesso!")

# Permite rodar o script diretamente pela linha de comando: python -m orchestration.run_pipeline
if __name__ == "__main__":
    run_pipeline()
