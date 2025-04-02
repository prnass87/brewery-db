# Importa a biblioteca 'requests' para fazer requisições HTTP à API
import requests

# Importa a biblioteca 'polars', usada para manipulação eficiente de dados em DataFrames
import polars as pl

# Importa 'Path' para manipulação de caminhos de arquivos de forma segura e multiplataforma
from pathlib import Path

def extract_brewery_data(save_path: str = None) -> None:
    """
    Extrai dados da API OpenBreweryDB e salva em formato Parquet.

    Parâmetros:
    - save_path (str): Caminho onde o arquivo Parquet será salvo. 
                       Caso não seja informado, será salvo no diretório padrão do projeto.
    """
    # Define o caminho padrão caso nenhum seja informado
    # O caminho /opt/airflow/brewery-db/... está alinhado com o uso dentro do Docker (Airflow)
    if save_path is None:
        save_path = Path("/opt/airflow/brewery-db/data/staging/brewerydb.parquet")
    else:
        save_path = Path(save_path)  # Converte string para objeto Path, caso necessário

    # URL da API pública que fornece os dados das cervejarias
    url = "https://api.openbrewerydb.org/v1/breweries"

    # Realiza a requisição GET para a API
    response = requests.get(url)

    # Lança erro caso a resposta não seja bem-sucedida (ex: status 404 ou 500)
    response.raise_for_status()

    # Converte o JSON retornado pela API para uma lista de dicionários (estrutura de dados Python)
    data = response.json()

    # Converte os dados para um DataFrame do Polars
    df = pl.DataFrame(data)

    # Cria os diretórios necessários para salvar o arquivo, se ainda não existirem
    save_path.parent.mkdir(parents=True, exist_ok=True)

    # Salva o DataFrame como arquivo Parquet no caminho especificado
    df.write_parquet(str(save_path))

    # Confirmação no terminal
    print(f"✅ Dados extraídos e salvos em: {save_path}")

# Permite que o script seja executado diretamente como programa principal
if __name__ == "__main__":
    extract_brewery_data()
