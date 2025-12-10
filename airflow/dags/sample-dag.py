from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.models import Variable
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

default_args={
   'depends_on_past': False,
   'email': ['abcd@gmail.com'],
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=5)
}
with DAG(
   'my-second-dag',
   default_args=default_args,
   description='simple dag',
   schedule=timedelta(days=1),
   start_date=datetime(2022, 11, 17),
   catchup=False,
   tags=['example']
) as dag:
   t1 = SparkKubernetesOperator(
       task_id='n-spark-pi',
       trigger_rule="all_success",
       depends_on_past=False,
       retries=3,
       application_file="new-spark-pi.yaml",
       namespace="spark-jobs",
       kubernetes_conn_id="myk8s",
       do_xcom_push=True,
       dag=dag
   )