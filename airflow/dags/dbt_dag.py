from __future__ import annotations

import datetime
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dbt_transformation_pipeline",
    start_date=datetime.datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "bigquery", "etl"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd /opt/dbt && \
        dbt deps --profiles-dir . && \
        dbt run --profiles-dir .
        """
    )