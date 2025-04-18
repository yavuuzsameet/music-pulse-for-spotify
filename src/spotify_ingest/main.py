import base64
import json
import os
import requests
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

from google.cloud import secretmanager
from google.cloud import storage
import functions_framework

# --- Configuration ---
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

SPOTIFY_CLIENT_ID_SECRET_NAME = "spotify-client-id"
SPOTIFY_CLIENT_SECRET_SECRET_NAME = "spotify-client-secret"
SPOTIFY_REFRESH_TOKEN_SECRET_NAME = "spotify-refresh-token"

SECRET_VERSION = "latest" # Use the latest version of the secret

# Spotify API Config
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_API_BASE_URL = "https://api.spotify.com/v1"
TIME_RANGE = "short_term" 
LIMIT = 10

# Initialize clients globally to potentially reuse connections
secret_manager_client = secretmanager.SecretManagerServiceClient()
storage_client = storage.Client()

def get_secret(secret_id):
    """Fetches a secret value from Google Cloud Secret Manager."""
    if not GCP_PROJECT_ID:
        raise ValueError("GCP_PROJECT_ID environment variable not set.")

    name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/{SECRET_VERSION}"
    try:
        response = secret_manager_client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")
        print(f"Successfully accessed secret: {secret_id}")
        return payload
    except Exception as e:
        print(f"Error accessing secret {secret_id}: {e}")
        raise RuntimeError(f"Failed to access secret {secret_id}") from e

def refresh_spotify_access_token(client_id, client_secret, refresh_token):
    """Gets a new access token from Spotify using a refresh token."""
    auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    headers = {"Authorization": f"Basic {auth_header}"}

    try:
        response = requests.post(SPOTIFY_TOKEN_URL, headers=headers, data=payload, timeout=10)
        response.raise_for_status()
        token_info = response.json()
        print("Successfully refreshed Spotify access token.")
        # Note: A new refresh token might sometimes be returned, but often isn't.
        # If it were, you'd need to securely update the stored refresh token.
        # For simplicity here, we assume the original refresh token remains valid.
        return token_info.get("access_token")
    except requests.exceptions.RequestException as e:
        print(f"Error refreshing Spotify token: {e}")
        if response is not None:
            print(f"Response status: {response.status_code}")
            print(f"Response text: {response.text}")
        raise RuntimeError("Failed to refresh Spotify token") from e
    
def fetch_spotify_top_items(access_token, item_type):
    """Fetches top tracks or artists for the authenticated user."""
    if item_type not in ["tracks", "artists"]:
        raise ValueError("item_type must be 'tracks' or 'artists'")

    api_url = f"{SPOTIFY_API_BASE_URL}/me/top/{item_type}"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"time_range": TIME_RANGE, "limit": LIMIT}

    print(f"Fetching top {item_type} ({TIME_RANGE}, limit {LIMIT})...")
    try:
        response = requests.get(api_url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        print(f"Successfully fetched top {item_type}.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Spotify top {item_type}: {e}")
        if response is not None:
            print(f"Response status: {response.status_code}")
            print(f"Response text: {response.text}")
        raise RuntimeError(f"Failed to fetch Spotify top {item_type}") from e

def upload_to_gcs(bucket_name, destination_blob_name, data_dict):
    """Uploads dictionary data (as JSON string) to a GCS bucket."""
    if not bucket_name:
        raise ValueError("GCS_BUCKET_NAME environment variable not set.")
    try:
        data_string = json.dumps(data_dict, indent=2) # Convert dict to formatted JSON string
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(data_string, content_type='application/json')
        print(f"Successfully uploaded data to gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        print(f"Error uploading to GCS bucket {bucket_name}: {e}")
        raise RuntimeError("Failed to upload data to GCS") from e

# Define the Cloud Function entry point
@functions_framework.http # Or use @functions_framework.cloud_event for event triggers
def spotify_ingest_http(request):
    """HTTP Cloud Function entry point."""
    print("Spotify ingestion function triggered.")
    run_timestamp = datetime.now()

    try:
        # 1. Get Credentials from Secret Manager
        print("Fetching Spotify credentials...")
        client_id = get_secret(SPOTIFY_CLIENT_ID_SECRET_NAME)
        client_secret = get_secret(SPOTIFY_CLIENT_SECRET_SECRET_NAME)
        refresh_token = get_secret(SPOTIFY_REFRESH_TOKEN_SECRET_NAME) 

        if not all([client_id, client_secret, refresh_token]):
             raise ValueError("Could not retrieve Spotify credentials.")

        # 2. Refresh Access Token
        print("Refreshing Spotify access token...")
        access_token = refresh_spotify_access_token(client_id, client_secret, refresh_token)

        if not access_token:
            raise ValueError("Could not obtain Spotify access token.")
        
        # --- Define GCS paths ---
        base_gcs_path = f"spotify/raw/{run_timestamp.strftime('%Y/%m/%d')}"
        timestamp_suffix = run_timestamp.strftime('%Y%m%d_%H%M%S')

        # 3. Fetch Playlist Data from Spotify API
        # --- Fetch and Upload Top Tracks ---
        try:
            top_tracks_data = fetch_spotify_top_items(access_token, "tracks")
            tracks_blob_name = f"{base_gcs_path}/top_tracks_{TIME_RANGE}_{timestamp_suffix}.json"
            upload_to_gcs(GCS_BUCKET_NAME, tracks_blob_name, top_tracks_data)
        except Exception as e:
            print(f"Failed to process top tracks: {e}")

        print("Spotify ingestion successful.")
        return ("OK", 200)

    except Exception as e:
        print(f"Error during Spotify ingestion: {e}")
        # Depending on the trigger type, error reporting might differ
        # For HTTP functions, returning an error code is standard
        return (f"Error: {e}", 500)

# Example of how to run locally using functions-framework (for testing)
# Open terminal in this directory and run:
# functions-framework --target spotify_ingest_http --debug
# Then send a request, e.g., using curl: curl http://localhost:8080
# Make sure GCP_PROJECT_ID and GCS_BUCKET_NAME are set as environment variables
# And ensure you've run 'gcloud auth application-default login'