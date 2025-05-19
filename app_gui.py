import tkinter as tk
from tkinter import scrolledtext
import threading
import pandas as pd
import time
import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sqlalchemy.engine import URL


# Função que retorna o caminho do .env
def get_dotenv_path():
    base_dir = os.path.dirname(sys.executable if getattr(sys, 'frozen', False) else __file__)
    dotenv_path = os.path.join(base_dir, '.env')
    if not os.path.isfile(dotenv_path):
        raise FileNotFoundError(f"Arquivo .env não encontrado em: {dotenv_path}")
    return dotenv_path



# Função que retorna o caminho do CSV
def get_csv_path():
    base_dir = os.path.dirname(sys.executable if getattr(sys, 'frozen', False) else __file__)
    return os.path.join(base_dir, "data", "olist_dataset.csv")


def run_script(output_widget):
    try:
        load_dotenv(get_dotenv_path(), override=True)

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


# Executa em uma thread separada
def start_process(output_widget):
    threading.Thread(target=run_script, args=(output_widget,), daemon=True).start()


# GUI
root = tk.Tk()
root.title("Inserção de Dados")
root.geometry("400x300")

button = tk.Button(root, text="Executar Script", command=lambda: start_process(output))
button.pack(pady=10)

output = scrolledtext.ScrolledText(root, wrap=tk.WORD, width=50, height=12)
output.pack(padx=10, pady=10)

root.mainloop()
