# Pasta `Airflow`

Esta pasta contém todos os arquivos necessários para configurar e executar o ambiente Apache Airflow com Docker. Eles são essenciais para orquestrar a pipeline ETL do projeto BreweryDB.

## Conteúdo

- `docker-compose.yml`: Orquestra os serviços (Airflow, PostgreSQL, etc.)
- `Dockerfile`: Define a imagem customizada com bibliotecas adicionais (ex: `polars`, `pyodbc`)
- `requirements.txt`: Lista de dependências Python utilizadas no projeto
- `dag_brewery_pipeline.py`: DAG responsável por executar as etapas do pipeline (extract, bronze, silver, gold)