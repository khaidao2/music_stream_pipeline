from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'music_stream_spark_pipeline',
    default_args=default_args,
    description='Orchestrate Spark Streaming Pipeline',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['spark', 'streaming', 'gcs'],
) as dag:
    run_spark_job = BashOperator(
        task_id='run_spark_streaming_job',
        bash_command="""
            docker exec -t music_spark_master /opt/spark/bin/spark-submit \
              --conf spark.jars.ivy=/tmp/.ivy2 \
              --jars /opt/spark/libs/gcs-connector.jar \
              --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-avro_2.12:3.5.1 \
              /opt/spark/jobs/listen_events_pipeline.py
        """
    )

    run_spark_job
