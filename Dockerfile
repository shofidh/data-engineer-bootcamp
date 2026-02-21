# Dockerfile
FROM apache/airflow:2.10.5-python3.11

USER root

# Install Java 17 untuk Spark
RUN apt-get update \
    && apt-get install -y openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install PySpark + Airflow Spark provider
RUN pip install --no-cache-dir \
    "pyspark==3.5.4" \
    "apache-airflow-providers-apache-spark==5.0.0"