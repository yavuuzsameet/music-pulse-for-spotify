terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.29.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# --- Core Infrastructure ---

# Data Lake - Google Cloud Storage Bucket
resource "google_storage_bucket" "data_lake" {
  name          = var.gcs_bucket_name
  location      = var.location
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  labels = {
    environment = "dev"
    project     = "music-pulse"
    purpose     = "data-lake"
  }

  force_destroy = false
}

# Data Warehouse - BigQuery Dataset
resource "google_bigquery_dataset" "data_warehouse" {
  dataset_id = var.bq_dataset_id
  location   = var.location
  labels = {
    environment = "dev"
    project     = "music-pulse"
    purpose     = "data-warehouse"
  }
}

# Secret Management - Secret Containers (values added manually later)
resource "google_secret_manager_secret" "spotify_client_id" {
  secret_id = "spotify-client-id"

  replication {
    auto {}
  }

  labels = {
    environment = "dev"
    project     = "music-pulse"
    purpose     = "api-credential"
  }
}

resource "google_secret_manager_secret" "spotify_client_secret" {
  secret_id = "spotify-client-secret"

  replication {
    auto {}
  }

  labels = {
    environment = "dev"
    project     = "music-pulse"
    purpose     = "api-credential"
  }
}

resource "google_secret_manager_secret" "discord_bot_application_id" {
  secret_id = "discord-bot-application-id"

  replication {
    auto {}
  }

  labels = {
    environment = "dev"
    project     = "music-pulse"
    purpose     = "api-credential"
  }
}

resource "google_secret_manager_secret" "discord_bot_public_key" {
  secret_id = "discord-bot-public-key"

  replication {
    auto {}
  }

  labels = {
    environment = "dev"
    project     = "music-pulse"
    purpose     = "api-credential"
  }

}

resource "google_secret_manager_secret" "ticketmaster_api_key" {
  secret_id = "ticketmaster-api-key"

  replication {
    auto {}
  }

  labels = {
    environment = "dev"
    project     = "music-pulse"
    purpose     = "api-credential"
  }
}

resource "google_secret_manager_secret" "ticketmaster_api_secret" {
  secret_id = "ticketmaster-api-secret"
  replication {
    auto {}
  }

  labels = {
    environment = "dev"
    project     = "music-pulse"
    purpose     = "api-credential"
  }

}