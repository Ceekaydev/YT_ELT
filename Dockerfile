ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.10

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ENV AIRFLOW_HOME=/opt/airflow

COPY requirements.txt /

# Install all requirements from requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Explicitly install connexion (latest compatible)
RUN pip install "connexion[swagger-ui]"
