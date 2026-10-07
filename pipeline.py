import os
import duckdb
from prefect import flow, task
from prefect.cache_policies import NO_CACHE


@task(retries=2, retry_delay_seconds=30, cache_policy=NO_CACHE)
def extract_and_load_raw(con: duckdb.DuckDBPyConnection) -> None:
    # Substitua pela sua extracao real (API, banco, arquivos...)
    # Dica: processe localmente e grave em Parquet/tabelas temporarias.
    con.sql("""
        CREATE OR REPLACE TABLE raw_vendas AS
        SELECT * FROM read_csv_auto('https://docs.google.com/spreadsheets/d/e/2PACX-1vSBGBpS8QHUsWZ6HLSykAmn0hqXIJh99oZf_BWuzP4ADh6Q_q7-TfEDtT8SwOgvKXVWtSgHOXDgplbo/pub?gid=800900601&single=true&output=csv'
	    ,normalize_names=True);
    """)


@task(cache_policy=NO_CACHE)
def transform(con: duckdb.DuckDBPyConnection) -> None:
    con.sql("""
        CREATE OR REPLACE TABLE gold_vendas AS
        SELECT 
        	codigo_venda
        	,_data
        	,id_loja
        	,produto
        	,quantidade
        	,valor_unitario
        	,valor_final
        FROM raw_vendas
    """)


@flow(log_prints=True)
def pipeline():
    # 1) Trabalho pesado em DuckDB local, dentro do runner (nao gasta compute da MotherDuck)
    local = duckdb.connect()
    extract_and_load_raw(local)
    transform(local)

    # 2) Publica so a camada final na MotherDuck
    token = os.environ.get("MOTHERDUCK_TOKEN", "").strip()
    if not token:
        raise RuntimeError(
            "MOTHERDUCK_TOKEN vazio ou ausente. Crie o secret em "
            "Settings > Secrets and variables > Actions > Repository secrets."
        )
    md = duckdb.connect(f"md:meu_db?motherduck_token={token}")
    df = local.sql("SELECT * FROM gold_vendas").arrow()
    md.sql("CREATE OR REPLACE TABLE gold_vendas AS SELECT * FROM df")
    print("Carga concluida")


if __name__ == "__main__":
    pipeline()