import pandas as pd
import time
import os
from dotenv import load_dotenv
from pymongo import MongoClient
import urllib.parse

# Carrega variáveis do .env
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path) #, override=True)

# Lê as variáveis do ambiente
mongo_host = os.getenv("MONGO_HOST")
mongo_port = os.getenv("MONGO_PORT")
mongo_db = os.getenv("MONGO_DB")
mongo_collection = os.getenv("MONGO_COLLECTION")
mongo_user = os.getenv("MONGO_USER")
mongo_pass = os.getenv("MONGO_PASSWORD")

# Codificar a senha para garantir que caracteres especiais sejam tratados corretamente
encoded_pass = urllib.parse.quote_plus(mongo_pass)

# print({mongo_host}, {mongo_port}, {mongo_user}, {mongo_pass}, {mongo_db}, {mongo_collection})

# Cria URL de conexão
mongo_uri = f"mongodb://{mongo_user}:{encoded_pass}@{mongo_host}:{mongo_port}/{mongo_db}?authSource=admin"

print("Iniciando inserção de dados no MongoDB")

# Conectar ao MongoDB
client = MongoClient(mongo_uri)
db = client[mongo_db]
collection = db[mongo_collection]

# Caminho do CSV
csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "olist_dataset.csv")
df = pd.read_csv(csv_path)
total_rows = len(df)

# Transforma DataFrame em dicionários
records = df.to_dict(orient="records")

# Medir tempo de inserção
start_time = time.time()
collection.insert_many(records)
end_time = time.time()

# Calcular throughput
elapsed_time = end_time - start_time
throughput = total_rows / elapsed_time if elapsed_time > 0 else 0

print(f"Linhas inseridas no MongoDB: {total_rows}")
print(f"Tempo total de inserção: {elapsed_time:.2f} segundos")
print(f"Throughput: {throughput:.2f} linhas por segundo")

# Excluir os dados para novo teste
collection.delete_many({})
