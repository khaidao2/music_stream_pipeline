variable "project_id" {
  description = "The GCP Project ID"
  default     = "realestate-492305"
}

variable "region" {
  description = "GCP Region"
  default     = "asia-southeast1"
}

variable "storage_class" {
  description = "Storage class for the bucket"
  default     = "STANDARD"
}

variable "bucket_name" {
  description = "Name of the GCS bucket for Data Lake"
  default     = "music-stream-data-lake-realestate-492305"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset name for Data Warehouse"
  default     = "music_stream_warehouse"
}
