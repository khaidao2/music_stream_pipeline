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

# 2. BigQuery Dataset (Data Warehouse)
resource "google_bigquery_dataset" "warehouse" {
  dataset_id = var.bq_dataset_name
  location   = var.region
  description = "Music Streaming Warehouse"
}
