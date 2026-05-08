terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file("../keys/key.json")
}

# 1. GCS Bucket (Data Lake)
resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  storage_class = var.storage_class
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  uniform_bucket_level_access = true
}

# 1.1 GCS Placeholder objects for processed folders
resource "google_storage_bucket_object" "processed_listen_events" {
  bucket = google_storage_bucket.data_lake.name
  name   = "processed/listen_events/.gitkeep"
  content = "placeholder"
}

resource "google_storage_bucket_object" "processed_auth_events" {
  bucket = google_storage_bucket.data_lake.name
  name   = "processed/auth_events/.gitkeep"
  content = "placeholder"
}

resource "google_storage_bucket_object" "processed_page_view_events" {
    bucket = google_storage_bucket.data_lake.name
    name   = "processed/page_view_events/.gitkeep"
    content = "placeholder"
  }
# 1.2. GCS Placeholder objects for raw folders
resource "google_storage_bucket_object" "raw_listen_events" {
  bucket = google_storage_bucket.data_lake.name
  name   = "raw/listen_events/.gitkeep"
  content = "placeholder"
}

resource "google_storage_bucket_object" "raw_auth_events" {
  bucket = google_storage_bucket.data_lake.name
  name   = "raw/auth_events/.gitkeep"
  content = "placeholder"
}

resource "google_storage_bucket_object" "raw_page_view_events" {
  bucket = google_storage_bucket.data_lake.name
  name   = "raw/page_view_events/.gitkeep"
  content = "placeholder"
}

# 2. BigQuery Dataset (Data Warehouse)
resource "google_bigquery_dataset" "warehouse" {
  dataset_id = var.bq_dataset_name
  location   = var.region
  description = "Music Streaming Warehouse"
}

# 3. BigQuery Native Tables
resource "google_bigquery_table" "listen_events" {
  dataset_id = google_bigquery_dataset.warehouse.dataset_id
  table_id   = "listen_events"
  deletion_protection = false
  
  time_partitioning {
    type  = "DAY"
    field = "event_timestamp"
  }

  clustering = ["userId"]

  schema = <<EOF
[
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
EOF
}

resource "google_bigquery_table" "auth_events" {
  dataset_id = google_bigquery_dataset.warehouse.dataset_id
  table_id   = "auth_events"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "event_timestamp"
  }

  clustering = ["userId"]

  schema = <<EOF
[
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
EOF
}

resource "google_bigquery_table" "page_view_events" {
  dataset_id = google_bigquery_dataset.warehouse.dataset_id
  table_id   = "page_view_events"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "event_timestamp"
  }

  clustering = ["userId"]

  schema = <<EOF
[
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
EOF
}
