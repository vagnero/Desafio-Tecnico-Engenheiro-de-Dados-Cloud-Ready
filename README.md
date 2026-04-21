# 🎲 Data Lakehouse Pipeline - Engenharia de Dados (Cloud-Ready)

Este repositório contém a resolução do Desafio Técnico de Engenharia de Dados. O projeto demonstra a construção de um pipeline analítico end-to-end, adotando a arquitetura **Medallion (Lakehouse)**. O foco principal é a aplicação de boas práticas de engenharia de dados, modularidade, governança e escalabilidade para ambientes em nuvem.

O repositório entrega um **MVP (Produto Mínimo Viável) 100% funcional**, focado na ingestão *Batch*, emulando localmente (via Docker) como os componentes de uma arquitetura Cloud corporativa operariam na prática.

**Link de vídeo explicando a solução:** https://drive.google.com/file/d/1PFWiAQ80eX94vjKuHrJc8xdC8XbwH2oY/view?usp=sharing
**Link de apresentação do canva:** https://www.canva.com/design/DAHHcN9NVeo/RkxqGowMTakAw993z5R8yQ/edit

## 📐 Arquitetura do Projeto

O diagrama abaixo ilustra a arquitetura completa desenhada para este desafio. Ela prevê fluxos de dados híbridos, suportando tanto o processamento **Batch** (cargas diárias) quanto **Streaming** (eventos em tempo real), garantindo a governança e a qualidade da informação em cada etapa.

![Diagrama da Arquitetura](/src/images/WINOVER_DESAFIO.png)

---

## 🚦 Imagens mostrando o fluxo de transformações de dados (usando o formato json para exibir até mesmo formatos parquet)

**1. CAMADA BRONZE | Dados em formato Json extraidos pela API:**
![Dados em formato Json extraidos pela API](/src/images/exempro_bronze_rawdata_json.png)

**2. CAMADA SILVER | Dados em formato parquet transformados em SILVER**
![Dados em formato parquet transformados em SILVER](/src/images/exempro_silver_transformation_parquet.png)

**3. CAMADA GOLD | Dados em formato parquet transformados em GOLD**
![Dados em formato parquet transformados em SILVER](/src/images/exempro_gold_transformation_parquet.png)

---

## 🛠️ Decisões Arquiteturais e Stack Tecnológica

A construção desta arquitetura não foi baseada apenas em "ferramentas da moda", mas sim em decisões pragmáticas focadas em entrega de valor, custo-benefício e facilidade de demonstração prática. Abaixo detalho o racional de cada escolha:

### 1. Orquestração: Apache Airflow
* **O Racional:** O Airflow é o "maestro" indiscutível da orquestração de dados. Ele foi escolhido (e seria a minha escolha independentemente da facilidade de simulação) pela sua capacidade madura de gerenciar grafos de dependência complexos (DAGs), garantir a idempotência, controlar retentativas e prover um monitoramento visual claro. Em uma esteira Cloud real, este componente seria facilmente substituído pelo serviço gerenciado Amazon MWAA (Managed Workflows for Apache Airflow).

### 2. Ingestão Batch: Scripts Python (Simulando AWS Lambda)
* **O Racional:** Para a esteira *Batch* (APIs, bancos relacionais e arquivos planos), a escolha por scripts Python puros oferece extrema flexibilidade no tratamento de exceções e regras de extração personalizadas. 
* **Visão Prática e Cloud:** No desenho da arquitetura, esses scripts representam funções **AWS Lambda**. Optar por esse padrão permite uma demonstração prática local perfeita no MVP, enquanto na nuvem garante uma ingestão *Serverless* e elástica, onde o custo existe apenas durante os segundos de execução.

### 3. Ingestão Streaming: Amazon Kinesis (Data Streams & Firehose)
* **O Racional:** Para fontes geradoras de alto volume e velocidade (Logs, IoT, cliques de E-commerce), a ingestão *Batch* tradicional é ineficiente. O **Kinesis Data Streams** atua como um *buffer* de alta capacidade para desacoplar produtores e consumidores. Em seguida, o **Kinesis Firehose** coleta, compacta e descarrega esses eventos diretamente no Data Lake (S3) de forma totalmente gerenciada, sem a necessidade de administrar servidores de ingestão.

### 4. Armazenamento Lakehouse: Amazon S3 (Medallion Architecture)
* **O Racional:** O S3 é o padrão da indústria devido ao seu armazenamento de objetos de custo quase zero e durabilidade de 99.999999911%. A separação lógica em camadas garante a evolução da maturidade do dado:
  * **Staging Area:** Área efêmera de pouso dos dados.
  * **Bronze (Raw):** Retém o dado cru exatamente como veio da fonte. Garante o histórico imutável e permite reprocessamento sem onerar as APIs ou bancos de origem.
  * **Silver (Cleansed):** Dados higienizados, tipados e armazenados no formato colunar **Parquet**, otimizando a leitura e a compressão.
  * **Gold (Curated):** Dados agregados, modelados em dimensões/fatos e contendo as métricas de negócios finais.

### 5. Processamento e ACID: Databricks (Spark) + Apache Iceberg + AWS Glue
* **O Racional:** Transformações pesadas e cruzamentos de dados entre as camadas exigem processamento distribuído, justificado pela escolha do **Databricks (Apache Spark)**. 
* **Governança:** A adoção do formato aberto **Apache Iceberg** eleva o S3 de um simples "lago" para um *Lakehouse*, permitindo transações ACID (UPDATE/DELETE), *Time Travel* e evolução de *schema*. O **AWS Glue Data Catalog** entra como o grande dicionário corporativo, centralizando os metadados para evitar que o repositório se torne um "Data Swamp".

### 6. Camada de Consumo (Serving): PostgreSQL
* **O Racional:** Embora o Lakehouse permita consultas diretas em formatos como Parquet/Iceberg via motores como Amazon Athena, a persistência da camada Gold em um banco relacional garante baixa latência, alta concorrência e melhor experiência para ferramentas de BI. Além disso, permite aplicar modelagem analítica (ex: star schema), otimizando consultas frequentes e agregadas.
* **Visão Prática:** O PostgreSQL foi escolhido por sua robustez, ampla compatibilidade com ferramentas como Power BI e Metabase, e facilidade de execução local via Docker para validação do MVP. Em ambiente produtivo, pode ser escalado com Amazon Aurora para maior desempenho e disponibilidade.

---

## 💻 Como Executar o MVP Local (Simulação Cloud-Ready)

> **Nota Técnica:** Para fins de avaliação, o pipeline de dados foi encapsulado utilizando **Docker e Docker Compose**. Isso abstrai a complexidade de instalação de componentes como Spark, Java e Airflow, garantindo que o ambiente rode perfeitamente de forma idêntica à máquina do desenvolvedor.

**Passo a passo:**

1. Clone este repositório:
   ```bash
   git clone https://github.com/vagnero/Desafio-Tecnico-Engenheiro-de-Dados-Cloud-Ready.git

  2. **Configure as variáveis de ambiente:**
   - Crie um arquivo chamado `.env` na raiz do projeto.
   - Copie o conteúdo a seguir:
   

API_BASE_URL=https://dummyjson.com


DB_HOST=localhost
DB_PORT=5433
DB_USER=airflow
DB_PASS=airflow
DB_NAME=camada_gold

    e cole e cole no seu novo `.env`.


3. **Construa a imagem do Docker:**
   ```bash
   docker-compose build
   ```
4. **Suba a infraestrutura completa (Airflow e PostgreSQL):**
   ```bash
   docker-compose up -d
   ```

5. **Acesse a interface do Apache Airflow no seu navegador:**
   - **URL:** `http://localhost:8080`
   - **Usuário:** `admin`
   - **Senha:** `admin`

6. **Inicie o Pipeline:**
   - Na lista de DAGs, localize a `pipeline_ecommerce_medallion`.
   - Ative o *toggle* para retirar do pause e clique no ícone de **Trigger (Play)**.

7. **Valide os resultados no banco de dados:**
   - Utilize o DBeaver ou pgAdmin para conectar ao banco de dados analítico:
     - **Host:** `localhost`
     - **Porta:** `5433`
     - **Database:** `camada_gold`
     - **User:** `airflow`
     - **Password:** `airflow`


8. **Passo opcional para rodar no Windows:**
  - 1 É necessário criar a pasta em: "C:\hadoop\bin".
  - 2 Após a criação dessa pasta, acesse o link do repositório: https://github.com/cdarlint/winutils, escolha a versão do hadoop e baixe os dois arquivos dll: "winutils.exe" e "hadoop.dll".
  - 3 Configure as variáveis de ambiente de usuário para enxergar essa pasta com os arquivos e estará pronto para rodar no Windows.