import os
import duckdb
from prefect import flow, task


@task(retries=2, retry_delay_seconds=30)
def extract_and_load_raw(con: duckdb.DuckDBPyConnection) -> None:
    # Substitua pela sua extracao real (API, banco, arquivos...)
    # Dica: processe localmente e grave em Parquet/tabelas temporarias.
    con.sql("""
        CREATE OR REPLACE TABLE raw_vendas AS
        SELECT * FROM read_csv_auto('https://exemplo.com/vendas.csv')
    """)


@task
def transform(con: duckdb.DuckDBPyConnection) -> None:
    con.sql("""
        CREATE OR REPLACE TABLE gold_vendas AS
        SELECT cliente, SUM(valor) AS total
        FROM raw_vendas
        GROUP BY cliente
    """)


@flow(log_prints=True)
def pipeline():
    # 1) Trabalho pesado em DuckDB local, dentro do runner (nao gasta compute da MotherDuck)
    local = duckdb.connect()
    extract_and_load_raw(local)
    transform(local)

    # 2) Publica so a camada final na MotherDuck
    token = os.environ["MOTHERDUCK_TOKEN"]
    md = duckdb.connect(f"md:meu_db?motherduck_token={token}")
    df = local.sql("SELECT * FROM gold_vendas").arrow()
    md.sql("CREATE OR REPLACE TABLE gold_vendas AS SELECT * FROM df")
    print("Carga concluida")


if __name__ == "__main__":
    pipeline()