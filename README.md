# 🚀 User Behavior Analytics - Data Engineering Pipeline

Projeto end-to-end de Engenharia de Dados em Python para análise de comportamento de usuários. Este pipeline orquestra a ingestão de dados, processamento distribuído (ML), modelagem analítica e checagem de qualidade, finalizando com a geração de um dashboard interativo.

## 🛠️ Tecnologias Utilizadas

* **Apache Airflow:** Orquestração e agendamento do pipeline (DAGs).
* **Apache Spark (PySpark):** Processamento de dados distribuído (aplicação de classificador de ML nos textos de reviews).
* **DuckDB:** Data Warehouse local / Engine OLAP para processamento analítico em memória com SQL.
* **Minio:** Armazenamento de objetos compatível com Amazon S3 (simulação de Data Lake local).
* **PostgreSQL:** Banco de dados relacional que atua como origem (OLTP) de compras de usuários e guarda os metadados do Airflow.
* **Pandas:** Testes de Data Quality (verificação de consistência e valores nulos).
* **Quarto & Plotly:** Geração do Data App (Dashboard HTML interativo).
* **Docker & Docker Compose:** Containerização e provisionamento da infraestrutura.
* **Pytest:** Testes automatizados da integridade da DAG.

## 🏗️ Arquitetura do Pipeline

O fluxo de dados da DAG (`user_analytics_dag`) segue as seguintes etapas:

1. **Setup do Data Lake:** Garante a existência do bucket `user-analytics` no S3/Minio.
2. **Ingestão (Extract):** 
   - Extrai um arquivo CSV local com reviews de filmes.
   - Extrai dados de compras do PostgreSQL.
3. **Processamento Spark (Transform):** Lê os reviews, aplica uma função de classificação de sentimentos e salva os resultados como `Parquet`.
4. **Modelagem Analítica (Transform):** O DuckDB executa um *JOIN* entre os dados brutos de compras e os reviews processados pelo Spark, gerando um CSV com as métricas agregadas por usuário (`amount_spent`, `num_positive_reviews`, `num_reviews`).
5. **Data Quality (Test):** Uma task analisa o arquivo final com Pandas em busca de dados vazios ou IDs nulos.
6. **Visualização (Serve):** O Quarto consome o arquivo aprovado e renderiza o painel em `dashboard.html`.

## 📂 Estrutura do Projeto

Para simplificar a execução e a visualização no ambiente de desenvolvimento, todos os arquivos do pipeline e de configuração encontram-se **na raiz do repositório** (estrutura plana):

* `docker-compose.yml`, `Dockerfile`, `requirements.txt`: Configurações de infra e dependências.
* `Makefile`: Atalhos de terminal (`make up`, `make down`, `make ci`).
* `user_analytics_dag.py`: O coração do projeto (Arquivo DAG do Airflow).
* `random_text_classification.py`: Script de processamento PySpark.
* `dashboard.qmd`: Template do dashboard Quarto/Plotly.
* `test_dag.py`: Arquivo de testes unitários da DAG usando Pytest.
* `movie_review.csv` / `user_purchase.csv`: Arquivos de dados de simulação (Mock).

## 🚀 Como Executar

### Pré-requisitos
* Git
* Docker e Docker Compose instalados na máquina.

### Passo a Passo

1. **Clone o repositório e acesse o diretório:**
   ```bash
   git clone [https://github.com/seu-usuario/user_behavior_analytics.git](https://github.com/seu-usuario/user_behavior_analytics.git)
   cd user_behavior_analytics
   ```
2. **Suba a infraestrutura:**
    ```bash
    make up
    ```
    Aguarde cerca de 30 a 60 segundos para que todos os containers sejam inicializados).

3. **Acesse as interfaces web:**
   
   Airflow: http://localhost:8080 (Usuário: airflow | Senha: airflow)
   
   ​Minio (S3): http://localhost:9001 (Usuário: minioadmin | Senha: minioadmin)

4. **Inicie o Pipeline:**

     No Airflow, ative (unpause) a DAG user_analytics_dag e clique no botão Trigger DAG (Play).

 5. **Visualize o Resultado:**

    Após a conclusão bem-sucedida de todas as tasks, abra o arquivo dashboard.html gerado na pasta do projeto para           interagir com os gráficos criados pelo Plotly.

  ### 🧪 Testes
  
  ​   Para executar os testes automatizados da DAG localmente, rode o comando:
  ```bash
  make ci
  ```
