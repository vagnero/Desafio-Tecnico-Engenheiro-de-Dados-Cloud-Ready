import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, current_timestamp, round as _round, desc

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Caminhos Medallion Silver e Gold
SILVER_DIR = "S3/data/silver/"
GOLD_DIR = "S3/data/gold/"

def criar_top_produtos(spark, df_sales, df_products):
    """Cruza vendas com produtos para descobrir os mais vendidos e mais lucrativos."""
    logging.info("Gerando Métrica: Top Produtos")
    caminho_destino = os.path.join(GOLD_DIR, "top_products")
    
    # Fazendo o JOIN da tabela de vendas com o catálogo de produtos
    df_join = df_sales.join(df_products, "product_id", "inner")
    
    # Agregando por Produto
    df_gold = df_join.groupBy("product_id", "product_name", "category").agg(
        _sum("quantity").alias("total_items_sold"),
        _round(_sum("total_price"), 2).alias("total_revenue")
    ).orderBy(desc("total_revenue")) # Ordenando do maior para o menor lucro
    
    # Adicionando data de carga
    df_gold = df_gold.withColumn("data_carga_gold", current_timestamp())
    
    logging.info("Visualização Gold: Top Produtos")
    df_gold.show(10, truncate=False)
    
    df_gold.coalesce(1).write.mode("overwrite").parquet(caminho_destino)
    logging.info("Tabela 'Top Products' salva na Gold!")

def criar_score_clientes(spark, df_sales, df_users):
    """Cruza vendas com usuários para ver quem gastou mais (Customer Lifetime Value)."""
    logging.info("Gerando Métrica: Score de Clientes (Gastos)")
    caminho_destino = os.path.join(GOLD_DIR, "customer_spending")
    
    # Fazendo o JOIN
    df_join = df_sales.join(df_users, "user_id", "inner")
    
    # Agregando por Cliente
    df_gold = df_join.groupBy("user_id", "first_name", "last_name", "email").agg(
        count("cart_id").alias("total_purchases"), # Quantas vezes comprou
        _sum("quantity").alias("total_items_bought"), # Quantos itens levou
        _round(_sum("total_price"), 2).alias("total_spent") # Total gasto
    ).orderBy(desc("total_spent"))
    
    df_gold = df_gold.withColumn("data_carga_gold", current_timestamp())
    
    logging.info("Visualização Gold: Score de Clientes")
    df_gold.show(10, truncate=False)
    
    df_gold.coalesce(1).write.mode("overwrite").parquet(caminho_destino)
    logging.info("Tabela 'Customer Spending' salva na Gold!")

def run():
    logging.info("--- INICIANDO TRANSFORMAÇÃO DE NEGÓCIO (SILVER -> GOLD) ---")
    
    spark = SparkSession.builder \
        .appName("Lakehouse-Gold") \
        .getOrCreate()
        
    # Carregando as tabelas limpas da Silver
    try:
        df_users = spark.read.parquet(os.path.join(SILVER_DIR, "users"))
        df_products = spark.read.parquet(os.path.join(SILVER_DIR, "products"))
        df_sales = spark.read.parquet(os.path.join(SILVER_DIR, "sales_transactions"))
        
        # Gerando as tabelas Gold
        criar_top_produtos(spark, df_sales, df_products)
        criar_score_clientes(spark, df_sales, df_users)
        
    except Exception as e:
        logging.error(f"Erro ao gerar a camada Gold: {e}. Verifique se as tabelas Silver existem.")
    
    spark.stop()
    logging.info("--- TRANSFORMAÇÃO GOLD CONCLUÍDA ---")

if __name__ == "__main__":
    run()