# benchmark_mongo.py
import os
import random
import time
from dotenv import load_dotenv
from pymongo import MongoClient, errors
import urllib.parse
import pandas as pd

def run_benchmark(log_fn=print):
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    load_dotenv(dotenv_path)

    mongo_host = os.getenv("MONGO_HOST")
    mongo_port = os.getenv("MONGO_PORT")
    mongo_db = os.getenv("MONGO_DB")
    mongo_collection = os.getenv("MONGO_COLLECTION")
    mongo_user = os.getenv("MONGO_USER")
    mongo_pass = os.getenv("MONGO_PASSWORD")
    encoded_pass = urllib.parse.quote_plus(mongo_pass)

    mongo_uri = f"mongodb://{mongo_user}:{encoded_pass}@{mongo_host}:{mongo_port}/{mongo_db}?authSource=admin"
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)
    db = client[mongo_db]
    collection = db[mongo_collection]

    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "olist_dataset.csv")
    df = pd.read_csv(csv_path)
    records = df.to_dict(orient="records")

    log_fn("Inserindo dados para benchmark...")
    collection.insert_many(records)

    log_fn("Iniciando benchmark de consultas aleatórias...")

    query_count = 0
    failures = 0
    start_time = time.time()

    try:
        while True:
            try:
                sample = random.choice(records)
                key, value = random.choice(list(sample.items()))
                collection.find_one({key: value})
                query_count += 1

                if query_count % 100 == 0:
                    elapsed = time.time() - start_time
                    log_fn(f"{query_count} consultas realizadas em {elapsed:.2f} segundos")

            except (errors.AutoReconnect, errors.ServerSelectionTimeoutError, errors.ExecutionTimeout) as e:
                log_fn(f"\nErro de conexão detectado: {str(e)}")
                failures += 1
                if failures >= 3:
                    log_fn("Múltiplas falhas detectadas. Encerrando benchmark.")
                    break
                time.sleep(1)

    except KeyboardInterrupt:
        log_fn("Benchmark interrompido manualmente.")

    total_time = time.time() - start_time
    log_fn(f"\nBenchmark finalizado. Total de consultas realizadas: {query_count}")
    log_fn(f"Tempo total: {total_time:.2f} segundos")
    log_fn(f"Throughput médio: {query_count / total_time:.2f} consultas/segundo")

    collection.delete_many({})
