# Data Lakehouse Pipeline: Engenharia de Dados Cloud-Ready

Este projeto apresenta um pipeline analítico de e-commerce, desenvolvido em Python e executado localmente com Docker. A solução segue a arquitetura **Medallion**, organizando os dados nas camadas Bronze, Silver e Gold para demonstrar um fluxo completo de ingestão, transformação e disponibilização.

O objetivo é mostrar como uma solução de dados pode ser estruturada de forma modular, observável e preparada para evoluir de um ambiente local para serviços gerenciados em nuvem. O MVP implementa o processamento em lote e simula localmente componentes que poderiam ser executados na AWS.

Este projeto foi originalmente desenvolvido como um teste de processo seletivo para uma empresa. O material foi adaptado para compor meu portfólio, preservando a solução técnica e os recursos que ajudam a demonstrar seu funcionamento.

## Materiais complementares

- [Vídeo demonstrativo da solução](https://drive.google.com/file/d/1PFWiAQ80eX94vjKuHrJc8xdC8XbwH2oY/view?usp=sharing): mantido para mostrar o pipeline em funcionamento. A demonstração prática começa em aproximadamente 7:40.
- [Apresentação do projeto](https://www.canva.com/design/DAHHcN9NVeo/RkxqGowMTakAw993z5R8yQ/edit)

## Visão geral

- Ingestão de dados de uma API pública de e-commerce.
- Armazenamento do dado bruto na camada Bronze.
- Limpeza, padronização e conversão para Parquet na camada Silver.
- Modelagem e agregações analíticas na camada Gold.
- Orquestração do fluxo com Apache Airflow.
- Disponibilização dos dados analíticos em PostgreSQL para consumo por ferramentas de BI.

## Arquitetura do projeto

O diagrama representa uma arquitetura híbrida, com suporte a processamento **Batch** e possibilidade de evolução para **Streaming**. Cada etapa possui uma responsabilidade clara, facilitando a manutenção, o reprocessamento e a expansão da solução.

![Diagrama da arquitetura](src/images/desafio-engenharia-de-dados.jpg)

---

## Fluxo de transformação dos dados

As imagens abaixo mostram o resultado de cada etapa do pipeline. Os arquivos Parquet são apresentados em formato JSON apenas para facilitar a visualização no repositório.

### 1. Bronze: dados brutos
Dados extraídos da API e preservados no formato original.
![Dados brutos extraídos da API](src/images/exempro_bronze_rawdata_json.png)

### 2. Silver: dados tratados
Dados tipados, padronizados e convertidos para o formato Parquet.
![Dados tratados na camada Silver](src/images/exempro_silver_transformation_parquet.png)

### 3. Gold: dados analíticos
Dados modelados e preparados para consultas e indicadores de negócio.
![Dados analíticos na camada Gold](src/images/exempro_gold_transformation_parquet.png)

---

## Decisões arquiteturais e stack tecnológica

As escolhas técnicas priorizam modularidade, baixo acoplamento, facilidade de execução e possibilidade de migração para uma arquitetura gerenciada.

### Orquestração: Apache Airflow
O Airflow coordena as dependências entre ingestão, transformação e carga, além de oferecer retentativas e monitoramento visual. Em uma implantação AWS, essa função poderia ser executada pelo Amazon MWAA.

### Ingestão batch: Python
Scripts Python fazem a extração da API e permitem aplicar regras de tratamento específicas com controle explícito de erros. Em um ambiente AWS, essa etapa poderia ser executada com AWS Lambda.

### Ingestão streaming: Amazon Kinesis
Para eventos de alto volume e baixa latência, o Amazon Kinesis pode complementar o fluxo batch. O Data Streams desacopla produtores e consumidores, enquanto o Firehose pode entregar os eventos ao data lake.

### Armazenamento: Amazon S3 e arquitetura Medallion
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

## Como executar localmente

O ambiente é encapsulado com Docker Compose para reduzir a quantidade de dependências locais e reproduzir o pipeline de forma consistente.

### Pré-requisitos

- Docker e Docker Compose.
- Git.

### Passo a passo

1. Clone este repositório:

   ```bash
   git clone https://github.com/vagnero/Desafio-Tecnico-Engenheiro-de-Dados-Cloud-Ready.git
   cd Desafio-Tecnico-Engenheiro-de-Dados-Cloud-Ready
   ```

2. Crie um arquivo `.env` na raiz do projeto com:

   ```env
   API_BASE_URL=https://dummyjson.com
   DB_HOST=localhost
   DB_PORT=5433
   DB_USER=airflow
   DB_PASS=airflow
   DB_NAME=camada_gold
   ```


3. Construa as imagens:

   ```bash
   docker-compose build
   ```

4. Inicie o Airflow e o PostgreSQL:

   ```bash
   docker-compose up -d
   ```

5. Acesse o Airflow em `http://localhost:8080` usando `admin` como usuário e senha.

6. Na lista de DAGs, ative `pipeline_ecommerce_medallion` e clique em **Trigger** para iniciar o pipeline.

7. Valide os resultados no PostgreSQL usando DBeaver, pgAdmin ou outra ferramenta de sua preferência:

   - Host: `localhost`
   - Porta: `5433`
   - Database: `camada_gold`
   - Usuário: `airflow`
   - Senha: `airflow`

## Estrutura do projeto

```text
dags/       Orquestração do pipeline com Airflow
src/        Extração, transformações e carga dos dados
src/images/ Diagramas e evidências das transformações
```