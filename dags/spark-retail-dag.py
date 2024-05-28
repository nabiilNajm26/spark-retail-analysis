from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "dibimbing",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_retail_analysis_dag",
    default_args=default_args,
    description="DAG to submit Spark job for retail analysis",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:

    spark_submit_task = SparkSubmitOperator(
        task_id="spark_retail_analysis",
        application="/spark-scripts/spark-retail-analysis.py",
        conn_id="spark_main",
        jars="/opt/airflow/postgresql-42.2.18.jar",
        application_args=[],
        dag=dag,
    )

    spark_submit_task