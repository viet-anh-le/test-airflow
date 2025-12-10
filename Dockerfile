FROM apache/spark-py:v3.4.0

USER root

RUN pip install delta-spark==3.0.0

WORKDIR /app

COPY batch /app/batch
COPY batch.zip /app/batch.zip

RUN chmod a+rwx /opt
RUN chown -R 185:0 /opt/spark /opt/spark/work-dir /app \
    && chmod -R g+w /opt/spark /opt/spark/work-dir /app

USER 185