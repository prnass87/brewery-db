# Importa a biblioteca polars para manipulação eficiente de dados em DataFrames
import polars as pl

# Importa a função datetime para registrar o timestamp do processamento
from datetime import datetime, timedelta

# Importa Path para manipulação segura de caminhos de arquivos
from pathlib import Path

def transform_bronze() -> None:
    """
    Transforma os dados da camada staging para bronze, tratando valores nulos e selecionando colunas relevantes.
    
    - Entrada: arquivo Parquet com dados brutos (camada Staging)
    - Saída: arquivo Parquet limpo e estruturado (camada Bronze)
    """

    # Define o caminho absoluto do arquivo de entrada (staging)
    input_path = Path("/opt/airflow/brewery-db/data/staging/brewerydb.parquet")

    # Define o caminho absoluto onde o arquivo da camada bronze será salvo
    output_path = Path("/opt/airflow/brewery-db/data/bronze/brewery/data.parquet")

    # Valida se o arquivo de entrada existe
    if not input_path.exists():
        raise FileNotFoundError(f"Arquivo de entrada não encontrado: {input_path}")

    # Lê o arquivo Parquet como LazyFrame (permite otimizações antes de executar)
    df_stg = pl.read_parquet(str(input_path)).lazy()

    # Seleciona e trata colunas relevantes:
    # - Preenche nulos
    # - Converte longitude/latitude para float
    df_bronze = df_stg.select(
        pl.col('id'),
        pl.col('name'),
        pl.col('brewery_type'),
        pl.col('address_1').fill_null(''),
        pl.col('address_2').fill_null(''),
        pl.col('address_3').fill_null(''),
        pl.col('city'),
        pl.col('state_province'),
        pl.col('postal_code'),
        pl.col('country'),
        pl.col('longitude').fill_null('0').cast(pl.Float64),
        pl.col('latitude').fill_null('0').cast(pl.Float64),
        pl.col('phone').fill_null(''),
        pl.col('website_url').fill_null(''),
        pl.col('state'),
        pl.col('street').fill_null(''),
    ).with_columns(
        # Adiciona coluna de timestamp de inserção na bronze
        dh_insert_bronze=pl.lit(datetime.now() - timedelta(hours=3))
    )

    # Garante que o diretório de saída exista
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Executa o LazyFrame (com .collect()) e salva como Parquet
    df_bronze.collect().write_parquet(
        str(output_path),
        compression='snappy'  # Compactação leve e eficiente para leitura
    )

    # Mensagem de sucesso
    print(f"✅ Camada bronze criada com sucesso em: {output_path}")

# Permite executar diretamente o script (ex: python stg_brz_brewerylist.py)
if __name__ == "__main__":
    transform_bronze()
