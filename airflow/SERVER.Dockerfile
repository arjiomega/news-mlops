FROM apache/airflow:3.0.3

USER root  # Switch to root for file copy, then revert
COPY dags/ /opt/airflow/dags/

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt


