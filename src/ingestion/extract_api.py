import requests
import json
import os
import logging
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env para o ambiente
load_dotenv()

# Configuração simples de log para vermos o que está acontecendo
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Pegando as variáveis do .env (com um valor padrão de segurança caso o .env falhe)
API_BASE_URL = os.getenv("API_BASE_URL")

#Caminho do Staging Area
STAGING_DIR = "data/staging/"

def garantir_pasta_existe(caminho):
    """Cria a pasta se ela não existir no sistema"""
    if not os.path.exists(caminho):
        os.makedirs(caminho)
        logging.info(f"Pasta criada: {caminho}")

def buscar_e_salvar_dados(endpoint):
    """Bate na API do DummyJSON e salva o arquivo na pasta Staging"""
    url = f"{API_BASE_URL}/{endpoint}"
    logging.info(f"Iniciando extração do endpoint: {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status() # Verifica se deu uma resposta http de erro
        
        dados = response.json()
        
        # Monta o caminho do arquivo (ex: data/staging/carts_raw.json)
        caminho_arquivo = os.path.join(STAGING_DIR, f"{endpoint}_raw.json")
        
        with open(caminho_arquivo, "w", encoding="utf-8") as arquivo:
            json.dump(dados, arquivo, indent=4)
            
        logging.info(f"Sucesso! Dados salvos em: {caminho_arquivo}")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao buscar dados do endpoint {endpoint}: {e}")

def run():
    """Função principal que o orquestrador vai chamar"""
    logging.info("--- INICIANDO INGESTÃO (API -> STAGING) ---")
    garantir_pasta_existe(STAGING_DIR)
    
    endpoints = ["carts", "products", "users"]
    
    for endpoint in endpoints:
        buscar_e_salvar_dados(endpoint)
        
    logging.info("--- INGESTÃO CONCLUÍDA ---")

# Teste local para confirmar está pegando antes do Airflow orquestrar
if __name__ == "__main__":
    run()