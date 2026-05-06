from __future__ import annotations

import datetime

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator

from airflow.providers.google.cloud.operators.gcs import (
    GCSListObjectsOperator,
    GCSDeleteObjectsOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

from config.config import Config

GCP_PROJECT_ID = Config.GCP_PROJECT_ID
GCS_BUCKET = Config.GCP_GCS_BUCKET_NAME
BQ_DATASET = Config.GCP_BQ_DATASET_NAME


# -------------------- SCHEMAS --------------------

listen_events_schema = [
    {"name": "artist", "type": "STRING", "mode": "NULLABLE"},
    {"name": "song", "type": "STRING", "mode": "NULLABLE"},
    {"name": "duration", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "ts", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "sessionId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "auth", "type": "STRING", "mode": "REQUIRED"},
    {"name": "level", "type": "STRING", "mode": "REQUIRED"},
    {"name": "itemInSession", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "city", "type": "STRING", "mode": "NULLABLE"},
    {"name": "zip", "type": "STRING", "mode": "NULLABLE"},
    {"name": "state", "type": "STRING", "mode": "NULLABLE"},
    {"name": "userAgent", "type": "STRING", "mode": "NULLABLE"},
    {"name": "lon", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "lat", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "userId", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "lastName", "type": "STRING", "mode": "NULLABLE"},
    {"name": "firstName", "type": "STRING", "mode": "NULLABLE"},
    {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
    {"name": "registration", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"}
]


auth_events_schema = [
    {"name": "ts", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "sessionId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "level", "type": "STRING", "mode": "REQUIRED"},
    {"name": "itemInSession", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "city", "type": "STRING", "mode": "NULLABLE"},
    {"name": "zip", "type": "STRING", "mode": "NULLABLE"},
    {"name": "state", "type": "STRING", "mode": "NULLABLE"},
    {"name": "userAgent", "type": "STRING", "mode": "NULLABLE"},
    {"name": "lon", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "lat", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "userId", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "lastName", "type": "STRING", "mode": "NULLABLE"},
    {"name": "firstName", "type": "STRING", "mode": "NULLABLE"},
    {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
    {"name": "registration", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "success", "type": "BOOLEAN", "mode": "REQUIRED"},
    {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"}
]


page_view_events_schema = [
    {"name": "ts", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "sessionId", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "page", "type": "STRING", "mode": "REQUIRED"},
    {"name": "auth", "type": "STRING", "mode": "REQUIRED"},
    {"name": "method", "type": "STRING", "mode": "REQUIRED"},
    {"name": "status", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "level", "type": "STRING", "mode": "REQUIRED"},
    {"name": "itemInSession", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "city", "type": "STRING", "mode": "NULLABLE"},
    {"name": "zip", "type": "STRING", "mode": "NULLABLE"},
    {"name": "state", "type": "STRING", "mode": "NULLABLE"},
    {"name": "userAgent", "type": "STRING", "mode": "NULLABLE"},
    {"name": "lon", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "lat", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "userId", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "lastName", "type": "STRING", "mode": "NULLABLE"},
    {"name": "firstName", "type": "STRING", "mode": "NULLABLE"},
    {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
    {"name": "registration", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"}
]


# -------------------- HELPERS --------------------

def has_files(**context):
    ti = context["ti"]
    files = ti.xcom_pull(task_ids=context["task_instance"].task_id.replace("check_", "list_"))
    return files is not None and len(files) > 0


def create_branch(event_type: str, schema: list):
    list_files = GCSListObjectsOperator(
        task_id=f"list_{event_type}",
        bucket=GCS_BUCKET,
        prefix=f"raw/{event_type}/",
        gcp_conn_id="google_cloud_default",
    )

    check_files = ShortCircuitOperator(
        task_id=f"check_{event_type}_files",
        python_callable=lambda **context: len(
            context["ti"].xcom_pull(task_ids=f"list_{event_type}")
        ) > 0,
    )

    load_bq = GCSToBigQueryOperator(
        task_id=f"load_{event_type}_to_bq",
        bucket=GCS_BUCKET,
        source_objects=[f"raw/{event_type}/*.parquet"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.{event_type}",
        source_format="PARQUET",
        schema_fields=schema,
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_NEVER",
        autodetect=False,
        gcp_conn_id="google_cloud_default",
    )

    copy_to_processed = GCSToGCSOperator(
        task_id=f"copy_{event_type}",
        source_bucket=GCS_BUCKET,
        source_object=f"raw/{event_type}/",
        destination_bucket=GCS_BUCKET,
        destination_object=f"processed/{event_type}/",
        move_object=False,
        gcp_conn_id="google_cloud_default",
    )

    delete_raw = GCSDeleteObjectsOperator(
        task_id=f"delete_{event_type}",
        bucket_name=GCS_BUCKET,
        prefix=f"raw/{event_type}/",
        gcp_conn_id="google_cloud_default",
    )

    list_files >> check_files >> load_bq >> copy_to_processed >> delete_raw


# -------------------- DAG --------------------

with DAG(
    dag_id="gcs_to_bigquery_pipeline",
    start_date=datetime.datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["gcs", "bigquery", "etl"],
) as dag:

    create_branch("listen_events", listen_events_schema)
    create_branch("auth_events", auth_events_schema)
    create_branch("page_view_events", page_view_events_schema)