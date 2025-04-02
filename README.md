# Brewery Data Pipeline

Este projeto implementa um pipeline de dados completo baseado na arquitetura medalhão (Bronze → Silver → Gold), utilizando Python, Polars, Airflow (via Docker), SQL Server e Excel.

## Visão Geral do Pipeline

O pipeline segue o seguinte fluxo:

1. **Extração (Staging)**: Dados extraídos da API [OpenBreweryDB](https://www.openbrewerydb.org/) e salvos em formato Parquet.
2. **Transformação Bronze**: Tratamento mínimo e estruturação inicial.
3. **Transformação Silver**: Limpeza, padronização e particionamento por `country` e `state_province`.
4. **Transformação Gold**:
   - Agregação dos dados (quantidade de cervejarias por tipo e localidade).
   - Geração de Parquet e Excel.
   - Inserção dos dados no banco SQL Server.

## Tecnologias Utilizadas

| Tecnologia       | Função no Projeto                       |
|------------------|---------------------------------------------|
| **Python**       | Linguagem principal                         |
| **UV**           | Ferramenta de gerenciamento de dependências Python (equivalente ao poetry ou pipfile) |
| **Polars**       | Processamento de dados                      |
| **Airflow**      | Orquestração das tarefas via DAG            |
| **Docker**       | Ambiente isolado para execução do Airflow    |
| **SQL Server**   | Armazenamento dos dados finais (camada Gold) |
| **VS Code**      | Ambiente de desenvolvimento                 |
| **GitHub**       | Controle de versão e publicação do projeto   |
| **Pandas**       | Conversão de dados para Excel                |
| **SQLAlchemy + pyodbc** | Integração com banco de dados       |


## Estrutura do Projeto

```
brewery-db/
├── src/                     # Extração da API
├── notebooks/
│   ├── bronze/              # Camada Bronze
│   ├── silver/              # Camada Silver
│   └── gold/                # Camada Gold
├── data/                    # Armazenamento de arquivos parquet/excel
├── orchestration/           # Script run_pipeline.py (script para teste de execução das etapas em pipeline)
├── airflow/                 # Arquivos do Airflow
│   └── dags/                # Dag utilizada no pipeline
├── images/                  # Imagens que evidemciam a utilização de ferramentas e resultados do projeto
```

## Resultado Final
Ao final da execução:
- Os dados estarão salvos em:
  - `data/gold/brewery/brewery_gold.parquet`
  - Um arquivo excel `data/gold/brewery/brewery_gold.xlsx`
  - E inseridos na tabela `gld_aggregated_count` no banco **SQL Server**.

---

## Describe how you would implement a monitoring and alerting process for this pipeline. Consider data quality issues, pipeline failures, and other potential problems in your response.

Para implementar um processo de monitoramento e alertas para esse pipeline, eu utilizaria os recursos do Airflow para lidar com falhas nas tarefas, configurar tentativas automáticas e habilitar notificações por e-mail em caso de sucesso ou erro. Dessa forma, eu seria notificado imediatamente se alguma etapa falhar.

Quanto à qualidade dos dados, incluiria validações após cada camada de transformação (Bronze, Silver e Gold) para verificar problemas como valores ausentes, coordenadas inválidas ou campos nulos inesperados. Se alguma dessas validações falhar, a tarefa lançaria um erro e um alerta seria disparado.

Além disso, monitoraria o tamanho dos arquivos gerados e o tempo de execução das tarefas para identificar comportamentos fora do padrão, como ausência de dados ou lentidão no processamento.