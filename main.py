import requests
import json
import pandas as pd
# Extraindo as vendas (carrinhos)
url = "https://dummyjson.com/carts"
response = requests.get(url)

if response.status_code == 200:
    dados = response.json()
    # Salvando na nossa Staging Area (S3/Local)
    #with open("data/bronze/vendas_raw.json", "w") as file:
    print(dados)
    print("Ingestão da Bronze concluída!")