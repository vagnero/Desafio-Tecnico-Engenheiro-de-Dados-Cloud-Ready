import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Se o código estiver rodando dentro do Airflow (Docker), usa o caminho completo do Linux
if os.getenv("AIRFLOW_HOME"):
    GOLD_DIR = "/opt/airflow/S3/data/gold/"
else:
    GOLD_DIR = "S3/data/gold/"

# Função alterna entre ambiente local e do docker
def get_engine():
    """Cria o motor de conexão detectando automaticamente o ambiente."""
    user = os.getenv("DB_USER", "airflow")
    password = os.getenv("DB_PASS", "airflow")
    db_name = os.getenv("DB_NAME", "camada_gold")
    
    if os.getenv("AIRFLOW_HOME"):
        host = "postgres"
        port = "5432"
        logging.info("🐳 DOCKER/AIRFLOW DETECTADO: Usando rede interna (postgres:5432)")
    else:
        host = "localhost"
        port = "5433"
        logging.info("🪟 WINDOWS DETECTADO: Usando desvio de porta (localhost:5433)")
        
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    return create_engine(connection_string)

def carregar_tabela(nome_tabela, engine):
    """Lê o Parquet da Gold e salva no Postgres."""
    caminho = os.path.join(GOLD_DIR, nome_tabela)
    logging.info(f"Procurando o arquivo em: {caminho}")
    
    if os.path.exists(caminho):
        logging.info(f"✅ Arquivo encontrado! Lendo dados da Gold: {nome_tabela}")
        df = pd.read_parquet(caminho)
        
        logging.info(f"Enviando {len(df)} linhas para a tabela '{nome_tabela}' no banco...")
        df.to_sql(nome_tabela, engine, if_exists='replace', index=False)
        logging.info(f"🚀 Carga da tabela '{nome_tabela}' finalizada com sucesso!")
    else:
        # --- MENSAGEM DE ERRO PARA EXIBIR NO AIRFLOW ---
        mensagem_erro = f"Arquivo NÃO encontrado em: {caminho}"
        logging.error(mensagem_erro)
        raise FileNotFoundError(mensagem_erro) 

def run():
    logging.info("--- INICIANDO CARGA FINAL (GOLD -> POSTGRES) ---")
    
    try:
        engine = get_engine()
        tabelas_gold = ["top_products", "customer_spending"]
        
        for tabela in tabelas_gold:
            carregar_tabela(tabela, engine)
            
    except Exception as e:
        logging.error(f"Erro ao conectar ou carregar dados no Postgres: {e}")
        raise # ISSO GARANTE QUE QUALQUER OUTRO ERRO TAMBÉM FIQUE VERMELHO!
    
    logging.info("--- CARGA CONCLUÍDA ---")

if __name__ == "__main__":
    run()