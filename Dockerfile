ARG AIRFLOW_VERSION=3.1.3
ARG PYTHON_VERSION=3.11

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ENV AIRFLOW_HOME=/opt/airflow

COPY requirements.txt /

# Install all requirements from requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Explicitly install connexion (latest compatible)
RUN pip install "connexion[swagger-ui]"
