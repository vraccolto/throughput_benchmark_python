import os
import random
import time
from dotenv import load_dotenv
from pymongo import MongoClient, errors
import urllib.parse
import pandas as pd

# Carrega variáveis de ambiente
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# Lê variáveis
mongo_host = os.getenv("MONGO_HOST")
mongo_port = os.getenv("MONGO_PORT")
mongo_db = os.getenv("MONGO_DB")
mongo_collection = os.getenv("MONGO_COLLECTION")
mongo_user = os.getenv("MONGO_USER")
mongo_pass = os.getenv("MONGO_PASSWORD")
encoded_pass = urllib.parse.quote_plus(mongo_pass)

# Cria URI de conexão
mongo_uri = f"mongodb://{mongo_user}:{encoded_pass}@{mongo_host}:{mongo_port}/{mongo_db}?authSource=admin"

# Conecta ao MongoDB
client = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)  # timeout de 2s
db = client[mongo_db]
collection = db[mongo_collection]

# Lê CSV de dados
csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "olist_dataset.csv")
df = pd.read_csv(csv_path)
records = df.to_dict(orient="records")

# Garante que o banco esteja populado
print("Inserindo dados para benchmark...")
collection.insert_many(records)

print("Iniciando benchmark de consultas aleatórias...")

query_count = 0
failures = 0
start_time = time.time()

try:
    while True:
        try:
            # Escolhe um campo aleatório (ajuste conforme os campos reais)
            sample = random.choice(records)
            key, value = random.choice(list(sample.items()))

            # Faz uma consulta
            result = collection.find_one({key: value})

            query_count += 1

            # Opcional: imprimir de tempos em tempos
            if query_count % 100 == 0:
                elapsed = time.time() - start_time
                print(f"{query_count} consultas realizadas em {elapsed:.2f} segundos")

        except (errors.AutoReconnect, errors.ServerSelectionTimeoutError, errors.ExecutionTimeout) as e:
            print(f"\nErro de conexão detectado: {str(e)}")
            failures += 1
            if failures >= 3:  # após 3 falhas seguidas, encerra
                print("Múltiplas falhas detectadas. Encerrando benchmark.")
                break
            time.sleep(1)  # aguarda antes de tentar novamente

except KeyboardInterrupt:
    print("Benchmark interrompido manualmente.")

end_time = time.time()
total_time = end_time - start_time

print(f"\nBenchmark finalizado. Total de consultas realizadas: {query_count}")
print(f"Tempo total: {total_time:.2f} segundos")
print(f"Throughput médio: {query_count / total_time:.2f} consultas/segundo")

# Limpa os dados
collection.delete_many({})
