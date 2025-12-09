FROM apache/spark-py:v3.4.0

USER root

RUN pip install delta-spark==3.0.0

WORKDIR /app

COPY batch /app/batch
COPY batch.zip /app/batch.zip

RUN chmod -R 777 /opt/spark