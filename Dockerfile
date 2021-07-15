FROM apache/airflow:1.10.10

ENV AIRFLOW_HOME="/home/airflow"

RUN mkdir /home/airflow/dags

RUN mkdir /home/airflow/data
RUN mkdir /home/airflow/staging
RUN mkdir /home/airflow/output

COPY dag.py /home/airflow/dags

RUN airflow initdb

# ENTRYPOINT ["sh", "-c"]
ENTRYPOINT airflow scheduler & airflow webserver --port 8080

