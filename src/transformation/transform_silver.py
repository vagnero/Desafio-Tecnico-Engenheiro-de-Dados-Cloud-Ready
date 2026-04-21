import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, current_timestamp

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Caminhos Medallion Bronze e Silver
BRONZE_DIR = "S3/data/bronze/"
SILVER_DIR = "S3/data/silver/"

def processar_usuarios(spark):
    """Lê da Bronze, explode o array 'users' e salva na Silver"""
    logging.info("Processando tabela: Users")
    caminho_origem = os.path.join(BRONZE_DIR, "users_raw")
    caminho_destino = os.path.join(SILVER_DIR, "users")

    if os.path.exists(caminho_origem):
        df = spark.read.parquet(caminho_origem)
        
        # Explode a lista principal e seleciona apenas as colunas que importam
        df_silver = df.select(explode(col("users")).alias("u")).select(
            col("u.id").alias("user_id"),
            col("u.firstName").alias("first_name"),
            col("u.lastName").alias("last_name"),
            col("u.email").alias("email"),
            col("u.age").alias("age"),
            current_timestamp().alias("data_carga_silver") # Hário de transformação
        )
        
        df_silver.coalesce(1).write.mode("overwrite").parquet(caminho_destino)
        logging.info(f"Tabela 'Users' salva na Silver!")

def processar_produtos(spark):
    """Lê da Bronze, explode o array 'products' e salva na Silver."""
    logging.info("Processando tabela: Products")
    caminho_origem = os.path.join(BRONZE_DIR, "products_raw")
    caminho_destino = os.path.join(SILVER_DIR, "products")

    if os.path.exists(caminho_origem):
        df = spark.read.parquet(caminho_origem)
        
        df_silver = df.select(explode(col("products")).alias("p")).select(
            col("p.id").alias("product_id"),
            col("p.title").alias("product_name"),
            col("p.category").alias("category"),
            col("p.price").cast("double").alias("price"), # Garantindo tipo numérico
            current_timestamp().alias("data_carga_silver")
        )
        
        df_silver.coalesce(1).write.mode("overwrite").parquet(caminho_destino)
        logging.info(f"Tabela 'Products' salva na Silver!")

def processar_vendas(spark):
    """Lê da Bronze, explode os carrinhos E os itens para criar uma tabela de transações."""
    logging.info("Processando tabela: Sales (Carts)")
    caminho_origem = os.path.join(BRONZE_DIR, "carts_raw")
    caminho_destino = os.path.join(SILVER_DIR, "sales_transactions")

    if os.path.exists(caminho_origem):
        df = spark.read.parquet(caminho_origem)
        
        # 1º Passo: Explode a lista de carrinhos
        df_carrinhos = df.select(explode(col("carts")).alias("c"))
        
        # 2º Passo: Explode a lista de produtos DENTRO do carrinho
        # Isso cria a nossa "Tabela Fato" granular
        df_silver = df_carrinhos.select(
            col("c.id").alias("cart_id"),
            col("c.userId").alias("user_id"),
            explode(col("c.products")).alias("item")
        ).select(
            col("cart_id"),
            col("user_id"),
            col("item.id").alias("product_id"),
            col("item.quantity").cast("integer").alias("quantity"),
            col("item.price").cast("double").alias("unit_price"),
            (col("item.quantity") * col("item.price")).alias("total_price"), # Calculando valor da linha
            current_timestamp().alias("data_carga_silver")
        )
        df_silver.coalesce(1).write.mode("overwrite").parquet(caminho_destino)
        logging.info(f"Tabela 'Sales Transactions' salva na Silver!")

def run():
    logging.info("--- INICIANDO TRANSFORMAÇÃO (BRONZE -> SILVER) ---")
    
    spark = SparkSession.builder \
        .appName("Lakehouse-Silver") \
        .getOrCreate()
        
    # Executa as transformações
    processar_usuarios(spark)
    processar_produtos(spark)
    processar_vendas(spark)
    
    spark.stop()
    logging.info("--- TRANSFORMAÇÃO SILVER CONCLUÍDA ---")

if __name__ == "__main__":
    run()