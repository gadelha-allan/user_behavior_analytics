FROM apache/airflow:2.7.2-python3.10

USER root

# Instala Java (para o Spark), Wget e o Quarto CLI
RUN apt-get update && \
    apt-get install -y default-jre wget procps && \
    wget https://github.com/quarto-dev/quarto-cli/releases/download/v1.3.450/quarto-1.3.450-linux-amd64.deb && \
    dpkg -i quarto-1.3.450-linux-amd64.deb && \
    rm quarto-1.3.450-linux-amd64.deb && \
    apt-get clean

USER airflow

# Instala as dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
