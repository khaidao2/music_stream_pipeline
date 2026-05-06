# Music Stream Data Pipeline

This project implements a complete, containerized data streaming pipeline that simulates, ingests, processes, and analyzes music listening events. It uses a modern data stack including Kafka, Spark Streaming, Airflow, dbt, and Google Cloud Platform (GCP).

## Architecture & Data Flow

The pipeline follows a modern ELT (Extract, Load, Transform) architecture:

1.  **Extract & Ingest**:
    *   `eventsim`: A containerized service generates simulated user listening events (`listen_events`, `auth_events`, `page_view_events`).
    *   **Kafka**: Events are streamed into a Kafka cluster, which acts as a durable, distributed message queue.
    *   **Apicurio Schema Registry**: Manages and enforces Avro schemas for all events flowing through Kafka.

2.  **Process & Stage (Spark -> GCS)**:
    *   **Airflow (`music_stream_spark_pipeline` DAG)**: Orchestrates a Spark Streaming job.
    *   **Spark Streaming**: The job consumes events from Kafka topics, parses them, adds a timestamp, and writes the data in partitioned Parquet format to a Google Cloud Storage (GCS) bucket, which serves as our Data Lake.

3.  **Load (GCS -> BigQuery)**:
    *   **Airflow (`gcs_to_bigquery_pipeline` DAG)**: An hourly scheduled DAG loads the new Parquet files from GCS into native, partitioned BigQuery tables.

4.  **Transform (dbt)**:
    *   **BigQuery**: Serves as the Data Warehouse.
    *   **dbt**: Transforms the raw data in BigQuery into clean, analytics-ready models (e.g., staging tables, fact tables, dimension tables).

## Prerequisites

*   **Docker** and **Docker Compose**
*   **Google Cloud Platform (GCP) Account**:
    *   A GCP Project.
    *   A GCS Bucket.
    *   A BigQuery Dataset.
    *   A Service Account with appropriate permissions (GCS Admin, BigQuery Admin).
*   **GCP Service Account Key**: A `key.json` file for your service account.

## Quickstart: Local Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd music_stream_pipeline
    ```

2.  **Place GCP Key:**
    *   Place your GCP service account key file in the `./keys` directory and name it `key.json`.

3.  **Create `.env` file:**
    *   Copy the example configuration: `cp .env.example .env`
    *   Review the `.env` file and update the `GCP_PROJECT_ID`, `GCP_GCS_BUCKET_NAME`, and `GCP_BQ_DATASET_NAME` variables with your specific GCP details.

4.  **Build and Start All Services:**
    *   This command will start all containers (Kafka, Spark, Airflow, etc.) in detached mode.
    ```bash
    docker compose up -d
    ```

## Running the Pipeline

1.  **Submit Schemas to Registry:**
    *   Run the schema registration script. This only needs to be done once.
    ```bash
    python3 scripts/register_schemas.py
    ```

2.  **Run the Spark Streaming Job:**
    *   Access the Airflow UI at `http://localhost:8084` (user: `admin`, pass: `admin`).
    *   Enable and trigger the `music_stream_spark_pipeline` DAG. This will start the Spark job that consumes from Kafka and writes to GCS.

3.  **Load Data into BigQuery:**
    *   Wait for some data to accumulate in GCS.
    *   In the Airflow UI, enable and trigger the `gcs_to_bigquery_pipeline` DAG. This will load the data from GCS into the native BigQuery tables.

4.  **Run dbt Transformations:**
    *   Navigate to the dbt directory:
    ```bash
    cd dbt_music_stream
    ```
    *   Set up your dbt profile to connect to your BigQuery instance.
    *   Run dbt:
    ```bash
    dbt run
    ```

## Accessing Services

*   **Airflow UI**: `http://localhost:8084`
*   **Spark Master UI**: `http://localhost:8083`
*   **Kafka UI**: `http://localhost:8091`

## Infrastructure as Code (IaC)

*   **Terraform**: The configuration in the `./terraform` directory is used to create the necessary GCP resources (GCS Bucket, BigQuery Dataset, and native tables).
    ```bash
    cd terraform
    terraform init
    terraform plan
    terraform apply
    ```
