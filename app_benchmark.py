import tkinter as tk
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText
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
import copy

# -------------------- Utils --------------------
def get_base_path():
    return os.path.dirname(sys.executable if getattr(sys, 'frozen', False) else __file__)

def get_csv_path():
    return os.path.join(get_base_path(), "data", "olist_dataset.csv")

def load_env():
    dotenv_path = os.path.join(get_base_path(), ".env")
    load_dotenv(dotenv_path, override=True)

# -------------------- Benchmark SQL Server --------------------
def run_benchmark_sql(log_fn, stop_event, update_table):
    try:
        load_env()
        server = os.getenv("DB_SERVER")
        banco = os.getenv("DB_NAME")
        username = os.getenv("DB_USERNAME")
        password = os.getenv("DB_PASSWORD")
        port = os.getenv("DB_PORT", "1433")

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

        df = pd.read_csv(get_csv_path())
        total_inserted = 0
        start_time = time.time()

        log_fn("[SQL Server] Iniciando inserção contínua...")

        while not stop_event.is_set():
            df.to_sql("olist_dataset", con=engine, if_exists="append", index=False)
            total_inserted += len(df)
            log_fn(f"[SQL Server] Linhas inseridas: {total_inserted}")

        elapsed = time.time() - start_time
        throughput = total_inserted / elapsed if elapsed > 0 else 0

        with engine.connect() as conn:
            conn.execute(text("DELETE FROM olist_dataset"))
            conn.commit()

        update_table("SQL Server", total_inserted, elapsed, throughput)
        log_fn("[SQL Server] Benchmark finalizado.")
    except Exception as e:
        log_fn(f"[SQL Server] Erro: {e}")

# -------------------- Benchmark MongoDB --------------------
def run_benchmark_mongo(log_fn, stop_event, update_table):
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

        client = MongoClient(mongo_uri)
        db = client[mongo_db]
        collection = db[mongo_collection]

        if collection.count_documents({}) > 0:
            log_fn("[MongoDB] Coleção contém dados. Limpando antes de iniciar o benchmark...")
            collection.delete_many({})
            log_fn("[MongoDB] Coleção limpa.")

        df = pd.read_csv(get_csv_path())
        # Remove _id se existir no dataframe
        records = [{k: v for k, v in row.items() if k != "_id"} for row in df.to_dict(orient="records")]

        total_inserted = 0
        start_time = time.time()

        log_fn("[MongoDB] Iniciando inserção contínua...")

        while not stop_event.is_set():
            # Copia os documentos para garantir _id novo gerado pelo MongoDB
            batch = [copy.deepcopy(doc) for doc in records]
            collection.insert_many(batch)
            total_inserted += len(batch)
            log_fn(f"[MongoDB] Linhas inseridas: {total_inserted}")

        elapsed = time.time() - start_time
        throughput = total_inserted / elapsed if elapsed > 0 else 0

        collection.delete_many({})
        update_table("MongoDB", total_inserted, elapsed, throughput)
        log_fn("[MongoDB] Benchmark finalizado.")
    except Exception as e:
        log_fn(f"[MongoDB] Erro: {e}")

# -------------------- GUI Setup --------------------
root = tk.Tk()
root.title("Benchmark Contínuo")
root.geometry("700x600")

output_box = ScrolledText(root, height=15, width=90)
output_box.pack(padx=10, pady=10)

# Result Table
results_table = ttk.Treeview(root, columns=("Sistema", "Linhas", "Tempo (s)", "Throughput"), show='headings', height=4)
for col in ("Sistema", "Linhas", "Tempo (s)", "Throughput"):
    results_table.heading(col, text=col)
    results_table.column(col, width=160, anchor='center')
results_table.pack(pady=10)

def log_output(msg):
    output_box.insert(tk.END, msg + '\n')
    output_box.see(tk.END)

def update_results_table(system_name, total_rows, elapsed_time, throughput):
    for row in results_table.get_children():
        if results_table.item(row)['values'][0] == system_name:
            results_table.item(row, values=(system_name, total_rows, f"{elapsed_time:.2f}", f"{throughput:.2f}"))
            return
    results_table.insert('', tk.END, values=(system_name, total_rows, f"{elapsed_time:.2f}", f"{throughput:.2f}"))

# Thread Management
stop_events = {"sql": threading.Event(), "mongo": threading.Event()}
threads = {"sql": None, "mongo": None}

def start_benchmark(key):
    if key == "sql":
        func = run_benchmark_sql
    else:
        func = run_benchmark_mongo

    def target():
        stop_events[key].clear()
        func(log_fn=log_output, stop_event=stop_events[key], update_table=update_results_table)

    if threads[key] is None or not threads[key].is_alive():
        t = threading.Thread(target=target, daemon=True)
        t.start()
        threads[key] = t

def stop_benchmark(key):
    stop_events[key].set()

# Botões
tk.Button(root, text="Executar SQL Server", width=40, command=lambda: start_benchmark("sql")).pack(pady=5)
tk.Button(root, text="Executar MongoDB", width=40, command=lambda: start_benchmark("mongo")).pack(pady=5)
tk.Button(root, text="Parar SQL Server", width=40, command=lambda: stop_benchmark("sql")).pack(pady=5)
tk.Button(root, text="Parar MongoDB", width=40, command=lambda: stop_benchmark("mongo")).pack(pady=5)
tk.Button(root, text="Limpar Saída", width=40, command=lambda: output_box.delete(1.0, tk.END)).pack(pady=5)

root.mainloop()
