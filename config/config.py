"""Configuration module for music_stream_pipeline."""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Configuration class to store environment variables."""

    # Kafka Configuration
    KAFKA_BROKER_ID = os.getenv("KAFKA_BROKER_ID", "1")
    KAFKA_ZOOKEEPER_CONNECT = os.getenv("KAFKA_ZOOKEEPER_CONNECT", "music_zookeeper:2181")
    KAFKA_LISTENERS = os.getenv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092")
    KAFKA_ADVERTISED_LISTENERS = os.getenv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://music_kafka:29092,EXTERNAL://localhost:9094")
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP = os.getenv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT")
    KAFKA_INTER_BROKER_LISTENER_NAME = os.getenv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR = os.getenv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
    KAFKA_AUTO_CREATE_TOPICS_ENABLE = os.getenv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
    KAFKA_NUM_PARTITIONS = os.getenv("KAFKA_NUM_PARTITIONS", "3")
    KAFKA_HEAP_OPTS = os.getenv("KAFKA_HEAP_OPTS", "-Xmx512m -Xms256m")

    # Kafka Broker List for clients
    KAFKA_BROKER_LIST = os.getenv("KAFKA_BROKER_LIST", "music_kafka:29092")
    KAFKA_EXTERNAL_BROKER_LIST = os.getenv("KAFKA_EXTERNAL_BROKER_LIST", "localhost:9094")

    # Paths
    EVENTSIM_DIR = os.getenv("EVENTSIM_DIR", "./eventsim")
    SPARK_JOBS_DIR = os.getenv("SPARK_JOBS_DIR", "./spark/jobs")
    SPARK_LIBS_DIR = os.getenv("SPARK_LIBS_DIR", "./spark/libs")
    SPARK_SCHEMAS_DIR = os.getenv("SPARK_SCHEMAS_DIR", "./schemas")
    AIRFLOW_DAGS_DIR = os.getenv("AIRFLOW_DAGS_DIR", "./airflow/dags")
    AIRFLOW_LOGS_DIR = os.getenv("AIRFLOW_LOGS_DIR", "./airflow/logs")
    AIRFLOW_PLUGINS_DIR = os.getenv("AIRFLOW_PLUGINS_DIR", "./airflow/plugins")
    VOLUMES_DIR = os.getenv("VOLUMES_DIR", "./volumes")
    KEYS_DIR = os.getenv("KEYS_DIR", "./keys")
    SCRIPTS_DIR = os.getenv("SCRIPTS_DIR", "./scripts")
    DBT_DIR = os.getenv("DBT_DIR", "./dbt_music_stream")

    # Environment
    ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

    # GCP Configuration
    GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "realestate-492305")
    GCP_GCS_BUCKET_NAME = os.getenv("GCP_GCS_BUCKET_NAME", "music-stream-data-lake-realestate-492305")
    GCP_BQ_DATASET_NAME = os.getenv("GCP_BQ_DATASET_NAME", "music_stream_warehouse")
    AIRFLOW_GCP_KEYFILE_PATH_IN_CONTAINER = os.getenv("AIRFLOW_GCP_KEYFILE_PATH_IN_CONTAINER", "/opt/airflow/keys/key.json")


