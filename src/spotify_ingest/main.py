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
SPOTIFY_PLAYLIST_ID = "" 

SECRET_VERSION = "latest" # Use the latest version of the secret

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
        raise e


def get_spotify_token(client_id, client_secret):
    """Gets an access token from Spotify using Client Credentials Flow."""
    auth_url = "https://accounts.spotify.com/api/token"
    auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")
    auth_data = {"grant_type": "client_credentials"}

    headers = {"Authorization": f"Basic {auth_header}"}

    try:
        response = requests.post(auth_url, headers=headers, data=auth_data, timeout=10)
        response.raise_for_status() 
        token_info = response.json()
        print("Successfully obtained Spotify token.")
        return token_info.get("access_token")
    except requests.exceptions.RequestException as e:
        print(f"Error obtaining Spotify token: {e}")
        print(f"Response status: {response.status_code if 'response' in locals() else 'N/A'}")
        print(f"Response text: {response.text if 'response' in locals() else 'N/A'}")
        raise e


def upload_to_gcs(bucket_name, destination_blob_name, data):
    """Uploads data (string) to a GCS bucket."""
    if not bucket_name:
        raise ValueError("GCS_BUCKET_NAME environment variable not set.")

    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(data, content_type='application/json')
        print(f"Successfully uploaded data to gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        print(f"Error uploading to GCS bucket {bucket_name}: {e}")
        raise e


# Define the Cloud Function entry point
@functions_framework.http # Or use @functions_framework.cloud_event for event triggers
def spotify_ingest_http(request):
    """HTTP Cloud Function entry point."""
    print("Spotify ingestion function triggered.")

    try:
        # 1. Get Credentials from Secret Manager
        print("Fetching Spotify credentials...")
        client_id = get_secret(SPOTIFY_CLIENT_ID_SECRET_NAME)
        client_secret = get_secret(SPOTIFY_CLIENT_SECRET_SECRET_NAME)

        if not client_id or not client_secret:
             raise ValueError("Could not retrieve Spotify credentials.")

        # 2. Get Spotify Access Token
        print("Getting Spotify access token...")
        access_token = get_spotify_token(client_id, client_secret)

        if not access_token:
            raise ValueError("Could not obtain Spotify access token.")

        # 3. Fetch Playlist Data from Spotify API
        print(f"Fetching playlist data for {SPOTIFY_PLAYLIST_ID}...")
        playlist_url = f""
        headers = {"Authorization": f"Bearer {access_token}"}
        # Add fields parameter to potentially limit response size if needed
        # params = {"fields": "items(track(name,id,artists(name)))"} # Example
        params = {} # Get all fields for now

        data_response = requests.get(playlist_url, headers=headers, params=params, timeout=10)
        data_response.raise_for_status()
        playlist_data = data_response.json()
        print("Successfully fetched playlist data.")

        # 4. Upload Raw Data to GCS
        now = datetime.now()
        destination_blob_name = f"spotify/raw/{now.strftime('%Y/%m/%d')}/italy_top50_{now.strftime('%Y%m%d_%H%M%S')}.json"
        print(f"Preparing to upload to GCS: gs://{GCS_BUCKET_NAME}/{destination_blob_name}")

        upload_to_gcs(GCS_BUCKET_NAME, destination_blob_name, json.dumps(playlist_data, indent=2))

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