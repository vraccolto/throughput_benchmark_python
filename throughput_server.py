import pandas as pd
import time
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sqlalchemy.engine import URL

# Carrega as variáveis do arquivo .env
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path, override=True)

# Lê as variáveis do ambiente
server = os.getenv("DB_SERVER")
banco = os.getenv("DB_NAME")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
port = os.getenv("DB_PORT", "1433")

#print(f"Variáveis carregadas: SERVER={server}, BANCO={banco}, USER={username}, PASSWORD={password}, PORT={port}")

print('Iniciando inserção de dados')

# String de conexão inicial (sem banco)
conn_url = URL.create(
    drivername="mssql+pyodbc",
    username=username,
    password=password,
    host=server,
    port=int(port),
    database=banco,
    query={"driver": "ODBC Driver 18 for SQL Server", "TrustServerCertificate": "yes"}
)

# Conectar à instância e criar banco, se necessário
engine = create_engine(conn_url, fast_executemany=True)
with engine.connect() as conn:
    result = conn.execute(
        text(f"SELECT COUNT(*) FROM sys.databases WHERE name = :db_name"),
        {"db_name": banco}
    )
    exists = result.scalar()
    if not exists:
        print(f"Criando banco de dados '{banco}'...")
        conn.execute(text(f"CREATE DATABASE [{banco}]"))
        conn.commit()
    #else:
        #print(f"Banco de dados '{banco}' já existe.")

# Conectar ao banco agora criado
conn_url = URL.create(
    "mssql+pyodbc",
    username=username,
    password=password,
    host=server,
    port=int(port),
    database=banco,
    query={
        "driver": "ODBC Driver 18 for SQL Server",
        "TrustServerCertificate": "yes"
    }
)

engine = create_engine(conn_url, fast_executemany=True)

# Caminho do arquivo CSV
csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "olist_dataset.csv")
df = pd.read_csv(csv_path)
total_rows = len(df)

# Medir tempo de inserção
start_time = time.time()
df.to_sql("olist_dataset", con=engine, if_exists="replace", index=False)
end_time = time.time()

# Calcular throughput
elapsed_time = end_time - start_time
throughput = total_rows / elapsed_time if elapsed_time > 0 else 0

print(f"Linhas inseridas no banco: {total_rows}")
print(f"Tempo total de inserção: {elapsed_time:.2f} segundos")
print(f"Throughput: {throughput:.2f} linhas por segundo")

# Excluir dados para novo teste
with engine.connect() as conn:
    conn.execute(text("DELETE FROM olist_dataset"))
    conn.commit()
    #print("Dados excluídos da tabela 'olist_dataset'.")
