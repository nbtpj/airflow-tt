FROM apache/airflow:latest
USER root
COPY ./requirements.txt ./requirements.txt
RUN apt-get update
RUN apt-get install -y libpq-dev
USER airflow
RUN pip install --upgrade setuptools
RUN pip install -r ./requirements.txt