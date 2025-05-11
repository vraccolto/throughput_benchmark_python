from dotenv import load_dotenv
import os
import pyodbc

# Carrega as variáveis do arquivo .env
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path, override=True)

# Lê as variáveis do ambiente
server = os.getenv("DB_SERVER")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
port = os.getenv("DB_PORT", "1433")

#print(f"Variáveis carregadas: SERVER={server}, USER={username}, PASSWORD={password}, PORT={port}")


# Banco de teste temporário
temp_database = "teste_temp"

# String de conexão inicial (sem banco definido)
base_conn_str = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={server},{port};"
    f"UID={username};"
    f"PWD={password};"
    f"Encrypt=no;"
)

try:
    # Conexão inicial para criar o banco temporário
    conn = pyodbc.connect(base_conn_str, autocommit=True, timeout=5)
    cursor = conn.cursor()

    # Cria o banco e uma tabela simples
    cursor.execute(f"IF DB_ID('{temp_database}') IS NULL CREATE DATABASE {temp_database};")
    cursor.execute(f"USE {temp_database};")
    cursor.execute("""
        CREATE TABLE clientes (
            id INT PRIMARY KEY IDENTITY(1,1),
            nome NVARCHAR(100),
            email NVARCHAR(100)
        );
    """)
    print("✅ Conexão estabelecida com o SQL Server!")

    # Deleta o banco temporário
    cursor.execute("USE master;")
    cursor.execute(f"DROP DATABASE {temp_database};")
    #print("🗑️ Banco temporário removido com sucesso!")

    conn.close()

except:
    print("❌ Falha de conexão com o SQL Server!")
