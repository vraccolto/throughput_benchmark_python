import tkinter as tk
from tkinter import scrolledtext
import threading
import pandas as pd
import time
import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from pymongo import MongoClient
import urllib.parse

def get_base_path():
    base_dir = os.path.dirname(sys.executable if getattr(sys, 'frozen', False) else __file__)
    dotenv_path = os.path.join(base_dir, '.env')
    if not os.path.isfile(dotenv_path):
        raise FileNotFoundError(f"Arquivo .env não encontrado em: {dotenv_path}")
    return dotenv_path

def get_csv_path():
    base_dir = os.path.dirname(sys.executable if getattr(sys, 'frozen', False) else __file__)
    return os.path.join(base_dir, "data", "olist_dataset.csv")

def load_env():
    dotenv_path = os.path.join(get_base_path(), '.env')
    load_dotenv(dotenv_path, override=True)

def append_output(output_widget, message):
    output_widget.insert(tk.END, message + "\n")
    output_widget.see(tk.END)

def run_sql_server(output_widget):
    try:
        load_dotenv(get_base_path(), override=True)

        server = os.getenv("DB_SERVER")
        banco = os.getenv("DB_NAME")
        username = os.getenv("DB_USERNAME")
        password = os.getenv("DB_PASSWORD")
        port = os.getenv("DB_PORT", "1433")

        output_widget.insert(tk.END, "Iniciando inserção de dados...\n")

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

        # Verifica se o banco existe
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT COUNT(*) FROM sys.databases WHERE name = :db_name"),
                {"db_name": banco}
            )
            exists = result.scalar()
            if not exists:
                output_widget.insert(tk.END, f"Criando banco de dados '{banco}'...\n")
                conn.execute(text(f"CREATE DATABASE [{banco}]"))
                conn.commit()

        # Conecta diretamente ao banco
        conn_url = URL.create(
            "mssql+pyodbc",
            username=username,
            password=password,
            host=server,
            port=int(port),
            database=banco,
            query={"driver": "ODBC Driver 18 for SQL Server", "TrustServerCertificate": "yes"}
        )

        engine = create_engine(conn_url, fast_executemany=True)

        # Lê o CSV
        csv_path = get_csv_path()
        df = pd.read_csv(csv_path)
        total_rows = len(df)

        start_time = time.time()
        df.to_sql("olist_dataset", con=engine, if_exists="replace", index=False)
        end_time = time.time()

        elapsed_time = end_time - start_time
        throughput = total_rows / elapsed_time if elapsed_time > 0 else 0

        output_widget.insert(tk.END, f"Linhas inseridas: {total_rows}\n")
        output_widget.insert(tk.END, f"Tempo de inserção: {elapsed_time:.2f} segundos\n")
        output_widget.insert(tk.END, f"Throughput: {throughput:.2f} linhas/segundo\n")

        with engine.connect() as conn:
            conn.execute(text("DELETE FROM olist_dataset"))
            conn.commit()
            output_widget.insert(tk.END, "Dados excluídos da tabela.\n")

    except Exception as e:
        output_widget.insert(tk.END, f"Erro: {e}\n")

def run_mongodb(output_widget):
    try:
        load_env()
        mongo_host = os.getenv("MONGO_HOST")
        mongo_port = os.getenv("MONGO_PORT")
        mongo_db = os.getenv("MONGO_DB")
        mongo_collection = os.getenv("MONGO_COLLECTION")
        mongo_user = os.getenv("MONGO_USER")
        mongo_pass = os.getenv("MONGO_PASSWORD")

        encoded_pass = urllib.parse.quote_plus(mongo_pass)
        mongo_uri = f"mongodb://{mongo_user}:{encoded_pass}@{mongo_host}:{mongo_port}/{mongo_db}?authSource=admin"

        append_output(output_widget, "Iniciando inserção de dados no MongoDB...")

        client = MongoClient(mongo_uri)
        db = client[mongo_db]
        collection = db[mongo_collection]

        df = pd.read_csv(get_csv_path())
        total_rows = len(df)
        records = df.to_dict(orient="records")

        start_time = time.time()
        collection.insert_many(records)
        end_time = time.time()

        elapsed_time = end_time - start_time
        throughput = total_rows / elapsed_time if elapsed_time > 0 else 0

        append_output(output_widget, f"Linhas inseridas no MongoDB: {total_rows}")
        append_output(output_widget, f"Tempo total de inserção: {elapsed_time:.2f} segundos")
        append_output(output_widget, f"Throughput: {throughput:.2f} linhas por segundo")

        collection.delete_many({})
        append_output(output_widget, "Dados excluídos do MongoDB.")
    except Exception as e:
        append_output(output_widget, f"Erro (MongoDB): {e}")

def start_thread(func, output_widget):
    threading.Thread(target=func, args=(output_widget,), daemon=True).start()

# GUI
root = tk.Tk()
root.title("Benchmark de Inserção de Dados")
root.geometry("500x400")

btn_sql = tk.Button(root, text="Executar SQL Server", width=25, command=lambda: start_thread(run_sql_server, output))
btn_sql.pack(pady=5)

btn_mongo = tk.Button(root, text="Executar MongoDB", width=25, command=lambda: start_thread(run_mongodb, output))
btn_mongo.pack(pady=5)

btn_clear = tk.Button(root, text="Limpar Saída", width=25, command=lambda: output.delete(1.0, tk.END))
btn_clear.pack(pady=5)

output = scrolledtext.ScrolledText(root, wrap=tk.WORD, width=60, height=15)
output.pack(padx=10, pady=10)

root.mainloop()
