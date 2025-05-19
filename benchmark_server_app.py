import os
import sys
import time
import random
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

def get_base_path():
    # Caminho correto mesmo quando empacotado com PyInstaller
    return os.path.dirname(sys.executable if getattr(sys, 'frozen', False) else os.path.abspath(__file__))

def load_env():
    dotenv_path = os.path.join(get_base_path(), '.env')
    if not os.path.isfile(dotenv_path):
        raise FileNotFoundError(f"Arquivo .env não encontrado em: {dotenv_path}")
    load_dotenv(dotenv_path, override=True)

def run_benchmark(log_fn=print):
    try:
        load_env()

        server = os.getenv("DB_SERVER")
        banco = os.getenv("DB_NAME")
        username = os.getenv("DB_USERNAME")
        password = os.getenv("DB_PASSWORD")
        port = os.getenv("DB_PORT", "1433")
        table_name = os.getenv("DB_TABLE", "olist_benchmark")

        conn_url = URL.create(
            drivername="mssql+pyodbc",
            username=username,
            password=password,
            host=server,
            port=int(port),
            database=banco,
            query={"driver": "ODBC Driver 18 for SQL Server", "TrustServerCertificate": "yes"}
        )

        engine = create_engine(conn_url, fast_executemany=True)

        csv_path = os.path.join(get_base_path(), "data", "olist_dataset.csv")
        df = pd.read_csv(csv_path)
        records = df.to_dict(orient="records")

        with engine.connect() as conn:
            log_fn("Criando tabela no SQL Server...")
            conn.execute(text(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};"))
            columns_def = ", ".join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])
            conn.execute(text(f"CREATE TABLE {table_name} ({columns_def});"))
            conn.commit()

        log_fn("Inserindo dados para benchmark...")
        df.to_sql(table_name, engine, if_exists='append', index=False)

        log_fn("Iniciando benchmark de consultas aleatórias...")
        query_count = 0
        failures = 0
        start_time = time.time()

        with engine.connect() as conn:
            while query_count < 1000:  # Limita para evitar loop infinito
                try:
                    sample = random.choice(records)
                    key, value = random.choice(list(sample.items()))
                    query = text(f"SELECT TOP 1 * FROM {table_name} WHERE [{key}] = :val")
                    result = conn.execute(query, {"val": value}).fetchone()

                    query_count += 1
                    if query_count % 100 == 0:
                        elapsed = time.time() - start_time
                        log_fn(f"{query_count} consultas realizadas em {elapsed:.2f} segundos")

                except Exception as e:
                    log_fn(f"Erro de consulta: {e}")
                    failures += 1
                    if failures >= 3:
                        log_fn("Múltiplas falhas. Encerrando benchmark.")
                        break
                    time.sleep(1)

        end_time = time.time()
        total_time = end_time - start_time
        throughput = query_count / total_time if total_time > 0 else 0

        log_fn(f"\nBenchmark finalizado. Total de consultas realizadas: {query_count}")
        log_fn(f"Tempo total: {total_time:.2f} segundos")
        log_fn(f"Throughput médio: {throughput:.2f} consultas/segundo")

        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {table_name}"))
            log_fn("Tabela limpa após benchmark.")

    except Exception as e:
        log_fn(f"Erro no benchmark SQL Server: {e}")
