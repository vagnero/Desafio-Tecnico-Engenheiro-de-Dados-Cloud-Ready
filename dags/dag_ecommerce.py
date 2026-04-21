from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Adiciona o caminho /opt/airflow para o Python achar seus scripts em /src
sys.path.append('/opt/airflow')

from src.ingestion import extract_api
from src.transformation import raw_data_bronze, transform_silver, transform_gold
from src.loading import load_postgres

default_args = {
    'owner': 'vagner',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'pipeline_ecommerce_medallion',
    default_args=default_args,
    description='Pipeline Medallion Completo - API para Postgres',
    schedule_interval='@daily', # Rodar todo dia
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='extract_from_api', python_callable=extract_api.run)
    t2 = PythonOperator(task_id='standardize_to_bronze', python_callable=raw_data_bronze.run)
    t3 = PythonOperator(task_id='clean_to_silver', python_callable=transform_silver.run)
    t4 = PythonOperator(task_id='aggregate_to_gold', python_callable=transform_gold.run)
    t5 = PythonOperator(task_id='load_to_postgres', python_callable=load_postgres.run)

    t1 >> t2 >> t3 >> t4 >> t5  # Define a ordem de execução