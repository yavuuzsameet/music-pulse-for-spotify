import base64
import os
import requests
import json

from google.cloud import bigquery
from google.cloud import secretmanager
from dotenv import load_dotenv
import functions_framework

# --- Configuration ---
load_dotenv() # Load .env file for local execution

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID")
DIM_ARTISTS_TABLE_ID = os.environ.get("DIM_ARTISTS_TABLE_ID") 
STG_TRACKS_TABLE_ID = os.environ.get("STG_TRACKS_TABLE_ID") 

# Construct full BQ table IDs
DIM_ARTISTS_TABLE_FULL_ID = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{DIM_ARTISTS_TABLE_ID}"
STG_TRACKS_TABLE_FULL_ID = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{STG_TRACKS_TABLE_ID}"

# Secret Manager Secret IDs
SPOTIFY_CLIENT_ID_SECRET_NAME = "spotify-client-id"
SPOTIFY_CLIENT_SECRET_SECRET_NAME = "spotify-client-secret"
SPOTIFY_REFRESH_TOKEN_SECRET_NAME = "spotify-refresh-token"
SECRET_VERSION = "latest"

# Spotify API Config
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_API_BASE_URL = "https://api.spotify.com/v1"

# Initiliase clients
secret_manager_client = secretmanager.SecretManagerServiceClient()
bq_client = bigquery.Client(project=GCP_PROJECT_ID)

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
        return token_info.get("access_token")
    except requests.exceptions.RequestException as e:
        print(f"Error refreshing Spotify token: {e}")
        if response is not None: print(f"Response status: {response.status_code}, Response text: {response.text}")
        raise RuntimeError("Failed to refresh Spotify token") from e

def fetch_spotify_artist_details(access_token, artist_ids):
    """Fetches full artist details from Spotify API."""
    if not artist_ids:
        print("No artist IDs provided to fetch details.")
        return []

    # Ensure we only process non-empty IDs and join them
    ids_to_fetch = [artist_id for artist_id in artist_ids if artist_id]
    if not ids_to_fetch:
        print("No valid artist IDs remaining after filtering.")
        return []

    ids_str = ",".join(ids_to_fetch)

    print(f"Attempting to fetch details for {len(ids_to_fetch)} artists from Spotify...")
    artist_details_endpoint = f"{SPOTIFY_API_BASE_URL}/artists" 
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"ids": ids_str}

    try:
        response = requests.get(artist_details_endpoint, headers=headers, params=params, timeout=10)
        # Check for common errors
        if response.status_code == 403:
            print(f"WARN: Received 403 Forbidden for GET /artists request.")
            return [] # Return empty list on forbidden
        elif response.status_code == 404:
             print(f"WARN: Received 404 Not Found for GET /artists request. Endpoint URL correct?")
             return [] # Return empty list on not found

        response.raise_for_status() # Raise exception for other bad status codes

        artists_data = response.json()

        # The response is like {"artists": [ {...}, null, {...} ]}
        if artists_data and 'artists' in artists_data:
            # Filter out potential None results from the API response list
            fetched_artists = [artist for artist in artists_data['artists'] if artist is not None]
            print(f"Successfully fetched details for {len(fetched_artists)} artists.")
            return fetched_artists
        else:
             print(f"WARN: No 'artists' key found in response. Response: {artists_data}")
             return [] # Return empty list if key missing

    except requests.exceptions.RequestException as e:
        print(f"\nError fetching artist details: {e}")
        if 'response' in locals() and response is not None:
            print(f"Status Code: {response.status_code}")
            print(f"Response Text: {response.text}")
        return [] # Return empty list on request error
    except Exception as e:
        print(f"\nAn unexpected error occurred fetching artist details: {e}")
        return [] # Return empty list on unexpected error

def merge_artists_to_bq(artists_data, latest_snapshot_date):
    """Merges fetched artist data into the BigQuery dim_artists table using parallel arrays."""
    if not artists_data:
        print("No artist data provided to merge into BigQuery.")
        return 0

    # --- Prepare Parallel Lists for Query Parameters ---
    artist_ids_list = []
    artist_names_list = []
    artist_pops_list = []
    artist_genres_list = [] 
    artist_uris_list = []
    artist_images_list = [] 
    snapshot_dates_list = []

    latest_snapshot_date_str = latest_snapshot_date.isoformat() # Convert date to string once

    for artist in artists_data:
        if not artist or 'id' not in artist: continue
        artist_ids_list.append(artist.get('id'))
        artist_names_list.append(artist.get('name'))
        artist_pops_list.append(artist.get('popularity')) # BQ client handles None for INT64
        artist_genres_list.append(json.dumps(artist.get('genres', []))) # Convert list to JSON string
        artist_uris_list.append(artist.get('uri'))
        artist_images_list.append(artist.get('images', [{}])[0].get('url'))
        snapshot_dates_list.append(latest_snapshot_date_str) # Append the string date

    if not artist_ids_list: # Check if any valid artists were processed
         print("No valid artist rows constructed for merging.")
         return 0

    print(f"Attempting to MERGE {len(artist_ids_list)} artist records into {DIM_ARTISTS_TABLE_FULL_ID} using parallel arrays...")

    # --- Construct MERGE statement using UNNEST ---
    # Assumes dim_artists columns match INSERT list below
    # We simplify again, removing genres/images temporarily
    merge_sql = f"""
    MERGE `{DIM_ARTISTS_TABLE_FULL_ID}` AS target
    USING (
      SELECT
        id AS artist_id,
        name AS artist_name,
        pop AS artist_popularity,
        JSON_QUERY_ARRAY(genres) AS artist_genres, 
        uri AS artist_uri,
        img AS artist_image_url, 
        snap_date AS last_seen_artist_snapshot_date_str
      FROM
        UNNEST(@artist_ids_param) AS id WITH OFFSET idx_id JOIN
        UNNEST(@artist_names_param) AS name WITH OFFSET idx_name ON idx_id = idx_name JOIN
        UNNEST(@artist_pops_param) AS pop WITH OFFSET idx_pop ON idx_id = idx_pop JOIN
        UNNEST(@artist_genres_param) AS genres WITH OFFSET idx_genre ON idx_id = idx_genre JOIN
        UNNEST(@artist_uris_param) AS uri WITH OFFSET idx_uri ON idx_id = idx_uri JOIN
        UNNEST(@artist_images_param) AS img WITH OFFSET idx_img ON idx_id = idx_img JOIN
        UNNEST(@snapshot_dates_param) AS snap_date WITH OFFSET idx_date ON idx_id = idx_date
    ) AS source
    ON target.artist_id = source.artist_id
    WHEN MATCHED THEN
        UPDATE SET
            target.artist_name = source.artist_name,
            target.artist_popularity = source.artist_popularity,
            target.artist_genres = source.artist_genres, 
            target.artist_uri = source.artist_uri,
            target.artist_image_url = source.artist_image_url, 
            target.last_seen_artist_snapshot_date = SAFE.PARSE_DATE('%Y-%m-%d', source.last_seen_artist_snapshot_date_str)
    WHEN NOT MATCHED THEN
        INSERT (artist_id, artist_name, artist_popularity, artist_genres, artist_uri, artist_image_url, last_seen_artist_snapshot_date) 
        VALUES (
            source.artist_id,
            source.artist_name,
            source.artist_popularity,
            source.artist_genres, 
            source.artist_uri,
            source.artist_image_url, 
            SAFE.PARSE_DATE('%Y-%m-%d', source.last_seen_artist_snapshot_date_str)
        )
    """

    # --- Define multiple ArrayQueryParameters ---
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("artist_ids_param", "STRING", artist_ids_list),
            bigquery.ArrayQueryParameter("artist_names_param", "STRING", artist_names_list),
            bigquery.ArrayQueryParameter("artist_pops_param", "INT64", artist_pops_list),
            bigquery.ArrayQueryParameter("artist_genres_param", "STRING", artist_genres_list), # Add back later
            bigquery.ArrayQueryParameter("artist_uris_param", "STRING", artist_uris_list),
            bigquery.ArrayQueryParameter("artist_images_param", "STRING", artist_images_list), # Add back later
            bigquery.ArrayQueryParameter("snapshot_dates_param", "STRING", snapshot_dates_list)
        ]
    )

    # --- Execute Query ---
    try:
        print("Executing BigQuery MERGE statement...")
        query_job = bq_client.query(merge_sql, job_config=job_config)
        results = query_job.result() # Wait for the job to complete
        print(f"BigQuery MERGE job completed. Affected rows: {query_job.num_dml_affected_rows}")
        return query_job.num_dml_affected_rows
    except Exception as e:
        print(f"Error executing BigQuery MERGE statement: {e}")
        print(f"SQL Query: {merge_sql[:1500]}...")
        raise RuntimeError("Failed to merge data into BigQuery") from e


# --- Main Function ---
@functions_framework.http
def enrich_artists_http(request):
    """HTTP Cloud Function to enrich dim_artists table."""
    print("Artist enrichment function triggered.")
    try:
        # 1. Get latest snapshot date from staging tracks
        print(f"Finding latest snapshot date in {STG_TRACKS_TABLE_FULL_ID}...")
        query_latest_date = f"SELECT MAX(track_snapshot_date) as max_date FROM `{STG_TRACKS_TABLE_FULL_ID}`"
        query_job_date = bq_client.query(query_latest_date)
        results_date = list(query_job_date.result())

        if not results_date or results_date[0].max_date is None:
            print("No snapshot date found in staging table. Exiting.")
            return ("No data in staging", 200)

        latest_snapshot_date = results_date[0].max_date
        print(f"Latest snapshot date: {latest_snapshot_date}")
        print(f"Latest snapshot date type: {type(latest_snapshot_date)}")

        # 2. Get distinct primary artist IDs from the latest snapshot
        print("Fetching distinct primary artist IDs from latest snapshot...")
        query_track_artists = f"""
            SELECT DISTINCT primary_artist_id
            FROM `{STG_TRACKS_TABLE_FULL_ID}`
            WHERE track_snapshot_date = @latest_date AND primary_artist_id IS NOT NULL
        """
        job_config_track = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("latest_date", "DATE", latest_snapshot_date)]
        )
        query_job_track = bq_client.query(query_track_artists, job_config=job_config_track)
        track_artist_ids = {row.primary_artist_id for row in query_job_track.result()} 

        if not track_artist_ids:
            print("No primary artist IDs found in latest track snapshot. Exiting.")
            return ("No artists found in tracks", 200)

        print(f"Found {len(track_artist_ids)} unique primary artists in latest tracks.")

        # 3. Get existing artist IDs from dim_artists (that might need enrichment)
        #    For simplicity now, let's just get all IDs to find the difference.
        #    Could be optimized later to only select artists with NULL genres etc.
        print(f"Fetching existing artist IDs from {DIM_ARTISTS_TABLE_FULL_ID}...")
        query_dim_artists = f"SELECT DISTINCT artist_id FROM `{DIM_ARTISTS_TABLE_FULL_ID}` WHERE artist_id IS NOT NULL"
        query_job_dim = bq_client.query(query_dim_artists)
        existing_dim_artist_ids = {row.artist_id for row in query_job_dim.result()}
        print(f"Found {len(existing_dim_artist_ids)} artists currently in dimension table.")

        # 4. Determine artists needing potential fetch/insert/update
        #    These are artists seen in tracks but NOT YET in the dimension table
        artists_to_fetch = list(track_artist_ids - existing_dim_artist_ids)
        print(f"Identified {len(artists_to_fetch)} new artists to fetch from Spotify API.")

        # 5. Fetch details from Spotify if needed
        if artists_to_fetch:
            print("Fetching Spotify credentials for enrichment...")
            client_id = get_secret(SPOTIFY_CLIENT_ID_SECRET_NAME)
            client_secret = get_secret(SPOTIFY_CLIENT_SECRET_SECRET_NAME)
            refresh_token = get_secret(SPOTIFY_REFRESH_TOKEN_SECRET_NAME)

            access_token = refresh_spotify_access_token(client_id, client_secret, refresh_token)
            if not access_token:
                 raise ValueError("Could not obtain Spotify access token for enrichment.")

            fetched_artist_details = fetch_spotify_artist_details(access_token, artists_to_fetch)

            # 6. Merge fetched details into BigQuery
            if fetched_artist_details:
                merge_artists_to_bq(fetched_artist_details, latest_snapshot_date)
            else:
                print("No details fetched from Spotify API, skipping BQ merge.")
        else:
            print("No new artists identified from tracks require fetching.")


        print("Artist enrichment process completed successfully.")
        return ("OK", 200)

    except Exception as e:
        print(f"Error during artist enrichment: {e}")
        # Log error appropriately
        return (f"Error: {e}", 500)
