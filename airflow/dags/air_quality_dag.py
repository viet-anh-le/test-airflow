from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DOCKER_APP_DIR = "local:///app" 
SCRIPT_DIR = f"{DOCKER_APP_DIR}/batch/jobs"
DEPENDENCIES_ZIP = f"{DOCKER_APP_DIR}/batch.zip"

DATA_INPUT_ROOT = "s3a://bus-sensor-data"
DATA_OUTPUT_ROOT = "s3a://bus-sensor-data/delta"

SPARK_PACKAGES = "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4"

SPARK_K8S_CONF = {
    "spark.master": "k8s://https://kubernetes.default.svc.cluster.local:443",
    "spark.submit.deployMode": "cluster",
    
    "spark.kubernetes.container.image": "custom-spark-job:latest",
    "spark.kubernetes.container.image.pullPolicy": "Never", 
    
    "spark.kubernetes.namespace": "airflow",
    "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
    
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    
    "spark.hadoop.fs.s3a.endpoint": "https://cuong-dev.cloud:9000", 
    "spark.kubernetes.driver.container.runAsUser": "0",
    "spark.kubernetes.executor.container.runAsUser": "0",

}

MINIO_ENV_VARS = {
    "AWS_ACCESS_KEY_ID": "minio",
    "AWS_SECRET_ACCESS_KEY": "minio123"
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'air_quality_pipeline_k8s',
    default_args=default_args,
    description='Pipeline chạy Spark trên Kubernetes (Explicit Tasks)',
    schedule='@daily',
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['spark', 'k8s', 'production'],
) as dag:

    s1_etl = SparkSubmitOperator(
        task_id='s1_etl_bronze',
        application=f'{SCRIPT_DIR}/s1_etl.py',
        conn_id='spark_default',
        py_files=DEPENDENCIES_ZIP,
        packages=SPARK_PACKAGES,
        conf=SPARK_K8S_CONF,
        env_vars=MINIO_ENV_VARS,
        application_args=[
            '--input', f'{DATA_INPUT_ROOT}/sensor-data', 
            '--bronze', f'{DATA_OUTPUT_ROOT}/bronze'
        ]
    )

    s2_merge = SparkSubmitOperator(
        task_id='s2_merge_gold',
        application=f'{SCRIPT_DIR}/s2_merge.py',
        conn_id='spark_default',
        py_files=DEPENDENCIES_ZIP,
        packages=SPARK_PACKAGES,
        conf=SPARK_K8S_CONF,
        env_vars=MINIO_ENV_VARS,
        application_args=[
            '--bronze', f'{DATA_OUTPUT_ROOT}/bronze',
            '--gold_fdata', f'{DATA_OUTPUT_ROOT}/gold/full-data'
        ]
    )

    s3_daily = SparkSubmitOperator(
        task_id='s3_agg_daily',
        application=f'{SCRIPT_DIR}/s3_daily.py',
        conn_id='spark_default',
        py_files=DEPENDENCIES_ZIP,
        packages=SPARK_PACKAGES,
        conf=SPARK_K8S_CONF,
        env_vars=MINIO_ENV_VARS,
        application_args=[
            '--fact', f'{DATA_OUTPUT_ROOT}/gold/full-data',
            '--daily', f'{DATA_OUTPUT_ROOT}/gold/daily'
        ]
    )

    s4_hourly = SparkSubmitOperator(
        task_id='s4_agg_hourly',
        application=f'{SCRIPT_DIR}/s4_hourly.py',
        conn_id='spark_default',
        py_files=DEPENDENCIES_ZIP,
        packages=SPARK_PACKAGES,
        conf=SPARK_K8S_CONF,
        env_vars=MINIO_ENV_VARS,
        application_args=[
            '--fact', f'{DATA_OUTPUT_ROOT}/gold/full-data',
            '--hourly', f'{DATA_OUTPUT_ROOT}/gold/hourly'
        ]
    )

    s5_monthly = SparkSubmitOperator(
        task_id='s5_agg_monthly',
        application=f'{SCRIPT_DIR}/s5_monthly.py',
        conn_id='spark_default',
        py_files=DEPENDENCIES_ZIP,
        packages=SPARK_PACKAGES,
        conf=SPARK_K8S_CONF,
        env_vars=MINIO_ENV_VARS,
        application_args=[
            '--fact', f'{DATA_OUTPUT_ROOT}/gold/full-data',
            '--monthly', f'{DATA_OUTPUT_ROOT}/gold/monthly'
        ]
    )

    s6_geo = SparkSubmitOperator(
        task_id='s6_geo_map',
        application=f'{SCRIPT_DIR}/s6_geo_map_aqi.py',
        conn_id='spark_default',
        py_files=DEPENDENCIES_ZIP,
        packages=SPARK_PACKAGES,
        conf=SPARK_K8S_CONF,
        env_vars=MINIO_ENV_VARS,
        application_args=[
            '--fact', f'{DATA_OUTPUT_ROOT}/gold/full-data',
            '--geo', f'{DATA_OUTPUT_ROOT}/gold/geo-map'
        ]
    )

    s1_etl >> s2_merge >> [s3_daily, s4_hourly, s5_monthly, s6_geo]