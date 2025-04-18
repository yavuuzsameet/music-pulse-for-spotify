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

# Secret Management
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

resource "google_secret_manager_secret" "spotify_refresh_token" {
  secret_id = "spotify-refresh-token"
  project   = var.project_id

  replication {
    auto {} 
  }

  labels = {
    environment = "dev"
    project     = "music-pulse"
    purpose     = "api-credential"
  }
}

# resource "google_secret_manager_secret" "discord_bot_application_id" {
#   secret_id = "discord-bot-application-id"

#   replication {
#     auto {}
#   }

#   labels = {
#     environment = "dev"
#     project     = "music-pulse"
#     purpose     = "api-credential"
#   }
# }

# resource "google_secret_manager_secret" "discord_bot_public_key" {
#   secret_id = "discord-bot-public-key"

#   replication {
#     auto {}
#   }

#   labels = {
#     environment = "dev"
#     project     = "music-pulse"
#     purpose     = "api-credential"
#   }

# }

# resource "google_secret_manager_secret" "ticketmaster_api_key" {
#   secret_id = "ticketmaster-api-key"

#   replication {
#     auto {}
#   }

#   labels = {
#     environment = "dev"
#     project     = "music-pulse"
#     purpose     = "api-credential"
#   }
# }

# resource "google_secret_manager_secret" "ticketmaster_api_secret" {
#   secret_id = "ticketmaster-api-secret"
#   replication {
#     auto {}
#   }

#   labels = {
#     environment = "dev"
#     project     = "music-pulse"
#     purpose     = "api-credential"
#   }
#}

# --- Cloud Function Service Account and Permissions ---

# Service Account for Cloud Functions to run as
resource "google_service_account" "spotify_ingest_sa" {
  account_id   = "spotify-ingest-sa"
  display_name = "Service Account for Spotify Ingestion Function"
  project      = var.project_id
}

# Grant Service Account permission to access Spotify secrets
resource "google_secret_manager_secret_iam_member" "spotify_client_id_accessor" {
  project   = google_secret_manager_secret.spotify_client_id.project
  secret_id = google_secret_manager_secret.spotify_client_id.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.spotify_ingest_sa.email}"
}

resource "google_secret_manager_secret_iam_member" "spotify_client_secret_accessor" {
  project   = google_secret_manager_secret.spotify_client_secret.project
  secret_id = google_secret_manager_secret.spotify_client_secret.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.spotify_ingest_sa.email}"
}

resource "google_secret_manager_secret_iam_member" "spotify_refresh_token_accessor" {
  project   = google_secret_manager_secret.spotify_refresh_token.project
  secret_id = google_secret_manager_secret.spotify_refresh_token.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.spotify_ingest_sa.email}"
}

# Grant Service Account permission to write to the GCS Data Lake bucket
resource "google_storage_bucket_iam_member" "data_lake_writer" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.spotify_ingest_sa.email}"
}


# --- Cloud Function Definition ---

resource "google_cloudfunctions2_function" "spotify_ingest_function" {
  name        = "spotify-ingest-function" 
  location    = var.region                
  project     = var.project_id

  build_config {
    runtime     = "python310" 
    entry_point = "spotify_ingest_http"
    source {
      storage_source {
        bucket = google_storage_bucket.data_lake.name
        object = "tf-sources/placeholder.zip" 
      }
    }
  }

  service_config {
    max_instance_count = 1 
    min_instance_count = 0 
    available_memory   = "256Mi" 
    timeout_seconds    = 120     

    environment_variables = {
      GCP_PROJECT_ID  = var.project_id
      GCS_BUCKET_NAME = google_storage_bucket.data_lake.name
    }
    
    # Use the dedicated service account
    service_account_email = google_service_account.spotify_ingest_sa.email
    
    # Allow public HTTP access for now for Kestra trigger
    ingress_settings               = "ALLOW_ALL"
    all_traffic_on_latest_revision = true
  }

  # Ensure secrets exist before creating function that needs access to them
  depends_on = [
    google_secret_manager_secret.spotify_client_id,
    google_secret_manager_secret.spotify_client_secret,
    google_secret_manager_secret.spotify_refresh_token,
  ]
}

# Grant public access to invoke the underlying Cloud Run service
resource "google_cloud_run_service_iam_member" "invoker" {
  location = google_cloudfunctions2_function.spotify_ingest_function.location
  project  = google_cloudfunctions2_function.spotify_ingest_function.project
  service  = google_cloudfunctions2_function.spotify_ingest_function.name
  role     = "roles/run.invoker"
  member   = "allUsers"

  # Depends on the function being created, implicitly handles underlying service readiness
  depends_on = [google_cloudfunctions2_function.spotify_ingest_function]
}


# --- BigQuery External Table for Raw Spotify Top Tracks ---

resource "google_bigquery_table" "raw_spotify_top_tracks" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.data_warehouse.dataset_id 
  table_id   = "raw_spotify_top_tracks"  

  # Define the external data source configuration
  external_data_configuration {
    source_format = "NEWLINE_DELIMITED_JSON" # Specify the format of the files in GCS

    # Use BigQuery's schema auto-detection for JSON files
    autodetect = true

    source_uris = [
       # Use a single wildcard - combined with hive partitioning below
       "gs://${google_storage_bucket.data_lake.name}/spotify/raw/*"
    ]
    hive_partitioning_options {
      mode = "CUSTOM" 
      source_uri_prefix = "gs://${google_storage_bucket.data_lake.name}/spotify/raw/{year:INTEGER}/{month:INTEGER}/{day:INTEGER}" 
    }
  }

  # Ensure the dataset exists before creating the table
  depends_on = [google_bigquery_dataset.data_warehouse]
}

# --- BigQuery External Table for Raw Spotify Top Artists ---

resource "google_bigquery_table" "raw_spotify_top_artists" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.data_warehouse.dataset_id 
  table_id   = "raw_spotify_top_artists" 

  external_data_configuration {
    source_uris = [
      # Use wildcards to match files across date partitions
      "gs://${google_storage_bucket.data_lake.name}/spotify/raw/*"
    ]
    hive_partitioning_options {
      mode = "CUSTOM" 
      source_uri_prefix = "gs://${google_storage_bucket.data_lake.name}/spotify/raw/{year:INTEGER}/{month:INTEGER}/{day:INTEGER}" 
    }
    
    source_format = "NEWLINE_DELIMITED_JSON"
    autodetect    = true 
  }

  depends_on = [google_bigquery_dataset.data_warehouse]
}