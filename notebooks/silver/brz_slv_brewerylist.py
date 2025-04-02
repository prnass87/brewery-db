# Importa a biblioteca polars para manipulação eficiente de dados em DataFrames
import polars as pl

# Importa datetime para registrar o momento da transformação
from datetime import datetime, timedelta

# Importa Path para lidar com caminhos de arquivos
from pathlib import Path

def transform_silver() -> None:
    """
    Transforma os dados da camada bronze para silver.
    
    - Trata valores nulos com padrões mais específicos
    - Mantém a estrutura e tipagem consistente
    - Adiciona particionamento por país e estado
    """

    # Caminho para o arquivo da camada bronze
    input_path = Path("/opt/airflow/brewery-db/data/bronze/brewery/data.parquet")

    # Caminho de saída para a camada silver (diretório)
    output_path = Path("/opt/airflow/brewery-db/data/silver/brewery")

    # Verifica se o arquivo de entrada existe
    if not input_path.exists():
        raise FileNotFoundError(f"Arquivo de entrada não encontrado: {input_path}")

    # Lê o arquivo da camada bronze como LazyFrame
    df_bronze = pl.read_parquet(str(input_path)).lazy()

    # Seleciona e aplica tratamentos adicionais aos dados:
    # - Preenche nulos em campos importantes com valores default
    # - Ajusta colunas para garantir consistência na silver
    df_silver = df_bronze.select(
        pl.col('id'),
        pl.col('name'),
        pl.col('brewery_type'),
        pl.col('address_1').fill_null(''),
        pl.col('address_2').fill_null(''),
        pl.col('address_3').fill_null(''),
        pl.col('city').fill_null('Unknown City'),              # Define cidade padrão quando ausente
        pl.col('state_province').fill_null('Unknown State'),  # Define estado padrão quando ausente
        pl.col('postal_code').fill_null(''),
        pl.col('country'),
        pl.col('longitude').fill_null(0).cast(pl.Float64),
        pl.col('latitude').fill_null(0).cast(pl.Float64),
        pl.col('phone').fill_null(''),
        pl.col('website_url').fill_null(''),
        pl.col('state'),
        pl.col('street').fill_null(''),
    ).with_columns(
        # Adiciona timestamp da transformação
        dh_insert_silver=pl.lit(datetime.now() - timedelta(hours=3))
    )

    # Garante que o diretório de saída existe
    output_path.mkdir(parents=True, exist_ok=True)

    # Executa o LazyFrame e salva como Parquet, com particionamento por país e estado
    df_silver.collect().write_parquet(
        str(output_path),
        partition_by=["country", "state_province"],  # Facilita leitura e consultas otimizadas
        compression="snappy"
    )

    # Mensagem de sucesso
    print(f"✅ Camada silver criada com sucesso em: {output_path}")

# Permite executar o script individualmente
if __name__ == "__main__":
    transform_silver()
