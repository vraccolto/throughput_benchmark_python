import os
import random
import time
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# Carrega variáveis do .env
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path, override=True)

# Lê variáveis de ambiente
server = os.getenv("DB_SERVER")
banco = os.getenv("DB_NAME")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
port = os.getenv("DB_PORT", "1433")
table_name = os.getenv("DB_TABLE", "olist_benchmark")

# Cria URL de conexão
conn_url = URL.create(
    drivername="mssql+pyodbc",
    username=username,
    password=password,
    host=server,
    port=int(port),
    database=banco,
    query={"driver": "ODBC Driver 18 for SQL Server", "TrustServerCertificate": "yes"}
)

# Cria engine do SQLAlchemy
engine = create_engine(conn_url, fast_executemany=True)

# Lê CSV
csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "olist_dataset.csv")
df = pd.read_csv(csv_path)
records = df.to_dict(orient="records")

# Cria/limpa tabela
with engine.connect() as conn:
    print("Criando tabela...")
    conn.execute(text(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};"))
    
    columns_def = ", ".join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])
    conn.execute(text(f"CREATE TABLE {table_name} ({columns_def});"))
    conn.commit()

# Insere dados
print("Inserindo dados para benchmark...")
df.to_sql(table_name, engine, if_exists='append', index=False)

# Inicia benchmark
print("Iniciando benchmark de consultas aleatórias...")
query_count = 0
failures = 0
start_time = time.time()

try:
    with engine.connect() as conn:
        while True:
            try:
                # Escolhe um campo e valor aleatório
                sample = random.choice(records)
                key, value = random.choice(list(sample.items()))

                # Executa a consulta segura
                query = text(f"SELECT TOP 1 * FROM {table_name} WHERE [{key}] = :val")
                result = conn.execute(query, {"val": value}).fetchone()

                query_count += 1
                if query_count % 100 == 0:
                    elapsed = time.time() - start_time
                    print(f"{query_count} consultas realizadas em {elapsed:.2f} segundos")

            except Exception as e:
                print(f"\nErro de consulta: {e}")
                failures += 1
                if failures >= 3:
                    print("Múltiplas falhas. Encerrando benchmark.")
                    break
                time.sleep(1)

except KeyboardInterrupt:
    print("Benchmark interrompido manualmente.")

end_time = time.time()
total_time = end_time - start_time

print(f"\nBenchmark finalizado. Total de consultas realizadas: {query_count}")
print(f"Tempo total: {total_time:.2f} segundos")
print(f"Throughput médio: {query_count / total_time:.2f} consultas/segundo")

# Limpa a tabela
with engine.begin() as conn:
    conn.execute(text(f"DELETE FROM {table_name}"))
