# 1. Usa a imagem oficial do Apache Airflow (versão estável)
FROM apache/airflow:2.8.1-python3.10

# 2. Muda para o usuário 'root' temporariamente para poder instalar dependências do Linux
USER root

# 3. Atualiza os pacotes do Linux e instala o Java (Obrigatório para o PySpark rodar)
# O comando de limpeza no final mantém a imagem Docker pequena e leve
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         default-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# 4. Volta para o usuário 'airflow' (Boa prática de segurança do Docker)
USER airflow

# 5. Copia o seu requirements.txt da sua máquina local para o container
COPY requirements.txt /

# 6. Instala as suas bibliotecas Python (PySpark, pandas, requests, SQLAlchemy)
RUN pip install --no-cache-dir -r /requirements.txt