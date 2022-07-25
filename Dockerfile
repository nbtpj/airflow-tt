FROM apache/airflow:latest
RUN pip install scrapy
RUN pip install redis