# Importa o Polars para manipulação de dados em DataFrame com alta performance
import polars as pl

# Importa datetime e timedelta para registro de horário de execução (ajustado para fuso horário)
from datetime import datetime, timedelta

# Importa Path para tratar caminhos de forma multiplataforma
from pathlib import Path

# Importa pandas para conversão final do DataFrame e exportação para Excel
import pandas as pd

# Importa SQLAlchemy para integração com o banco de dados SQL Server
from sqlalchemy import create_engine

def transform_gold() -> None:
    """
    Agrega os dados da camada silver e gera a camada gold:
    - Gera um Parquet com os dados agregados
    - Gera um Excel (.xlsx) para análise manual
    - Insere os dados em uma tabela no SQL Server
    """

    # Caminho onde estão os arquivos particionados da camada Silver
    input_path = Path("/opt/airflow/brewery-db/data/silver/brewery")

    # Caminho de saída para o arquivo Parquet da camada Gold
    output_parquet_path = Path("/opt/airflow/brewery-db/data/gold/brewery/brewery_gold.parquet")

    # Caminho de saída para o arquivo Excel da camada Gold
    output_excel_path = Path("/opt/airflow/brewery-db/data/gold/brewery/brewery_gold.xlsx")

    # Valida se o diretório de entrada da Silver existe
    if not input_path.exists():
        raise FileNotFoundError(f"Diretório de entrada não encontrado: {input_path}")

    # Lê todos os arquivos Parquet da camada Silver como LazyFrame
    df_silver = pl.read_parquet(str(input_path)).lazy()

    # Agrega os dados:
    # - Conta a quantidade de cervejarias por país, estado e tipo
    # - Adiciona um timestamp ajustado para o horário de Brasília
    df_gold = (
        df_silver
        .group_by(["country", "state_province", "brewery_type"])
        .agg(pl.count().alias("brewery_count"))
        .with_columns(
            dh_insert_gold=pl.lit(datetime.now() - timedelta(hours=3))  # UTC-3
        )
    )

    # Garante que o diretório da camada Gold exista
    output_parquet_path.parent.mkdir(parents=True, exist_ok=True)

    # Salva o resultado da camada Gold como Parquet (compactado com snappy)
    df_gold.collect().write_parquet(str(output_parquet_path), compression="snappy")
    print(f"✅ Parquet salvo em: {output_parquet_path}")

    # Converte o resultado para pandas para exportação como Excel
    df_gold_pd = df_gold.collect().to_pandas()
    df_gold_pd.to_excel(str(output_excel_path), index=False)
    print(f"✅ Excel salvo em: {output_excel_path}")

    # Cria engine de conexão com SQL Server (via pyodbc dentro do Docker)
    engine = create_engine(
        "mssql+pyodbc://brewery_user:brewery123@host.docker.internal/Brewery_Gold?driver=ODBC+Driver+17+for+SQL+Server"
    )

    # Insere os dados na tabela gld_aggregated_count
    df_gold_pd.to_sql(
        name="gld_aggregated_count",
        con=engine,
        if_exists="replace",  # Substitui se a tabela já existir (pode mudar para 'append')
        index=False
    )

    print("✅ Dados inseridos na tabela [gld_aggregated_count] no banco Brewery_Gold")

# Permite que o script seja executado de forma independente
if __name__ == "__main__":
    transform_gold()
