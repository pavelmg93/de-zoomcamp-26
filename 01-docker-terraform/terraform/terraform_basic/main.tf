terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}

provider "google" {
# Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS set
#  credentials = 
  project = "project-c94cb82b-89ba-4659-98a"
  region  = "us-central1"
}



resource "google_storage_bucket" "data-lake-bucket" {
  name          = "de-zc-pmg-data"
  location      = "us-central1"

  # Optional, but recommended settings:
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 70  // days
    }
  }

  force_destroy = true
}


resource "google_bigquery_dataset" "dataset" {
  dataset_id = "de_zc_pmg_bigquery_dataset"
  project    = "project-c94cb82b-89ba-4659-98a"
  location   = "US"
}