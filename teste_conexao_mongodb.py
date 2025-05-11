from dotenv import load_dotenv
import os
from pymongo import MongoClient

# Carrega variáveis do .env
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# Lê a URI do Mongo
mongo_uri = os.getenv("MONGO_URI")

# Tenta se conectar
try:
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
    client.admin.command('ping')  # Envia um ping para testar a conexão
    print("✅ Conexão estabelecida com o MongoDB!")
except:
    print("❌ Falha de conexão com o  MongoDB!")