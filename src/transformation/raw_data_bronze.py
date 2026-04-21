import os
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

STAGING_DIR = "S3/data/staging/"
BRONZE_DIR = "S3/data/bronze/"

def run():
    logging.info("--- INICIANDO PADRONIZAÇÃO (STAGING -> BRONZE) ---")
    
    spark = SparkSession.builder \
        .appName("Lakehouse-Bronze") \
        .getOrCreate()
        
    if not os.path.exists(BRONZE_DIR):
        os.makedirs(BRONZE_DIR)

    arquivos_staging = [f for f in os.listdir(STAGING_DIR) if f.endswith('.json')]
    
    if not arquivos_staging:
        logging.warning("Nenhum arquivo encontrado na Staging Area.")
        return

    # Lista para rastrear quais arquivos foram convertidos com sucesso
    arquivos_para_deletar = []

    for arquivo in arquivos_staging:
        caminho_origem = os.path.join(STAGING_DIR, arquivo)
        nome_base = os.path.splitext(arquivo)[0]
        caminho_destino = os.path.join(BRONZE_DIR, nome_base)
        
        logging.info(f"Processando {arquivo}...")
        
        try:
            # Lendo com multiline para aceitar o JSON formatado
            df = spark.read.option("multiline", "true").json(caminho_origem)
            
            # Escrevendo Parquet
            df.coalesce(1).write.mode("overwrite").parquet(caminho_destino)
            
            # Se chegou aqui sem erro, adicionamos o arquivo JSON à lista de limpeza
            arquivos_para_deletar.append(caminho_origem)
            logging.info(f"Parquet gerado com sucesso para: {arquivo}")
            
        except Exception as e:
            logging.error(f"Erro ao processar {arquivo}: {e}")

    # FECHA O SPARK PRIMEIRO (Libera os arquivos no Windows)
    spark.stop()
    logging.info("Sessão Spark encerrada. Iniciando limpeza da Staging Area...")

    # AGORA DELETA OS ARQUIVOS
    for caminho in arquivos_para_deletar:
        try:
            os.remove(caminho)
            logging.info(f"Arquivo removido da Staging: {caminho}")
        except Exception as e:
            logging.error(f"Não foi possível deletar {caminho}. Motivo: {e}")

    logging.info("--- PADRONIZAÇÃO CONCLUÍDA ---")

if __name__ == "__main__":
    run()