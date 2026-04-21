import logging
import sys
import os

# Adiciona a pasta raiz ao path para garantir que as importações funcionem
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importação dos módulos do seu pipeline
from src.ingestion import extract_api
from src.transformation import raw_data_bronze, transform_silver, transform_gold
from src.loading import load_postgres

# Configuração de Logs para acompanhar o progresso no terminal
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

def main():
    try:
        logging.info(" INICIANDO PIPELINE END-TO-END (MEDALLION ARCHITECTURE)")

        # 1. Camada STAGING: Extração bruta da API (JSON)
        logging.info("Etapa 1/5: Extraindo dados da API para Staging...")
        extract_api.run()

        # 2. Camada BRONZE: Padronização (JSON -> Parquet) e Limpeza da Staging
        logging.info("Etapa 2/5: Padronizando dados para a Camada Bronze...")
        raw_data_bronze.run()

        # 3. Camada SILVER: Refino e Nivelamento (Explode de Arrays e Tipagem)
        logging.info("Etapa 3/5: Refinando dados para a Camada Silver...")
        transform_silver.run()

        # 4. Camada GOLD: Agregações de Negócio (Top Produtos e Score de Clientes)
        logging.info("Etapa 4/5: Gerando métricas para a Camada Gold...")
        transform_gold.run()

        # 5. Camada SERVING: Carga Final no PostgreSQL
        logging.info("Etapa 5/5: Carregando dados finais no banco de dados...")
        load_postgres.run()

        logging.info("✅ PIPELINE FINALIZADO COM SUCESSO!")
        logging.info("Os dados já podem ser consultados no PostgreSQL.")

    except Exception as e:
        logging.error(f"❌ FALHA CRÍTICA NO PIPELINE: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()