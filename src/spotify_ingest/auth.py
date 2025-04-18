import base64
import os
import requests
import webbrowser
import secrets 
from urllib.parse import urlencode, parse_qs, urlparse
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread

from google.cloud import secretmanager
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv() 

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
if not GCP_PROJECT_ID:
    raise ValueError("GCP_PROJECT_ID environment variable not set. Create a .env file or set it manually.")

# --- Spotify App Configuration ---
# Fetch Client ID/Secret from Secret Manager 
SPOTIFY_CLIENT_ID_SECRET_NAME = "spotify-client-id"
SPOTIFY_CLIENT_SECRET_SECRET_NAME = "spotify-client-secret"

REDIRECT_URI = "http://127.0.0.1:8000/callback"

# Permissions needed for the /me/top/... endpoints
# https://developer.spotify.com/documentation/web-api/reference/get-users-top-artists-and-tracks
SCOPES = "user-top-read"

# --- GCP Secret Manager Configuration ---
# ID of the secret where the refresh token will be stored
REFRESH_TOKEN_SECRET_ID = "spotify-refresh-token"
SECRET_VERSION = "latest" 

# --- Global variable to capture the authorization code ---
authorization_code = None
auth_state_sent = secrets.token_urlsafe(7)

secret_manager_client = secretmanager.SecretManagerServiceClient()

def get_secret(secret_id):
    """Fetches a secret value from Google Cloud Secret Manager."""
    name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/{SECRET_VERSION}"
    try:
        response = secret_manager_client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")
        print(f"Successfully accessed secret: {secret_id}")
        return payload
    except Exception as e:
        print(f"Error accessing secret {secret_id}: {e}")
        raise RuntimeError(f"Failed to access secret {secret_id}") from e

# --- HTTP Server to Handle Spotify Callback ---
class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global authorization_code
        query_components = parse_qs(urlparse(self.path).query)
        code = query_components.get("code", [None])[0]
        state_received = query_components.get("state", [None])[0]

        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()

        if state_received != auth_state_sent:
             self.wfile.write(b"<html><body><h1>Error: State mismatch! CSRF Attack?</h1></body></html>")
             print("ERROR: State mismatch. Authorization failed.")
             authorization_code = "ERROR_STATE_MISMATCH" # Signal error
        elif code:
            self.wfile.write(b"<html><body><h1>Success! Authorization code received.</h1><p>You can close this window.</p></body></html>")
            print("Authorization code received successfully.")
            authorization_code = code # Store the code
        else:
            error = query_components.get("error", ["Unknown error"])[0]
            self.wfile.write(f"<html><body><h1>Error: {error}</h1><p>Authorization failed. You can close this window.</p></body></html>".encode("utf-8"))
            print(f"Authorization failed with error: {error}")
            authorization_code = f"ERROR_{error}" # Signal error

        # Attempt to stop the server shortly after handling the request
        # Note: This immediate shutdown might sometimes cause browser errors, but is needed for script flow
        def shutdown_server():
            server.shutdown()
            print("Local callback server shut down.")
        Thread(target=shutdown_server).start()

if __name__ == "__main__":
    print("--- Spotify Refresh Token Retriever ---")

    # 1. Get Client ID and Secret
    print("Fetching Client ID and Secret from Secret Manager...")
    client_id = get_secret(SPOTIFY_CLIENT_ID_SECRET_NAME)
    client_secret = get_secret(SPOTIFY_CLIENT_SECRET_SECRET_NAME)

    # 2. Prepare Authorization URL
    auth_params = {
        "client_id": client_id,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES,
        "state": auth_state_sent # Include state for security
    }
    auth_url = f"https://accounts.spotify.com/authorize?{urlencode(auth_params)}"

    # 3. Start Local Server and Open Browser
    redirect_uri_parts = urlparse(REDIRECT_URI)
    server_address = (redirect_uri_parts.hostname, redirect_uri_parts.port)
    server = HTTPServer(server_address, CallbackHandler)

    print(f"\nStarting temporary local server on {REDIRECT_URI}...")
    server_thread = Thread(target=server.serve_forever)
    server_thread.daemon = True # Allow main thread to exit even if server thread is running
    server_thread.start()

    print("\n>>> Opening Spotify authorization URL in your browser <<<")
    print(">>> Please log in to Spotify and grant access to the application. <<<")
    print(f"DEBUG: Attempting to open URL: {auth_url}")
    webbrowser.open(auth_url)

    # 4. Wait for Callback (or error)
    print("\n>>> Waiting for Spotify to redirect back to local server... <<<")
    # The server runs until the handler calls server.shutdown()
    server_thread.join() # Wait for server thread to finish (after shutdown is called)

    # 5. Exchange Code for Tokens (if code received)
    if authorization_code and not authorization_code.startswith("ERROR_"):
        print("\nExchanging authorization code for tokens...")
        token_url = "https://accounts.spotify.com/api/token"
        auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")
        payload = {
            "grant_type": "authorization_code",
            "code": authorization_code,
            "redirect_uri": REDIRECT_URI,
        }
        headers = {"Authorization": f"Basic {auth_header}"}

        try:
            response = requests.post(token_url, headers=headers, data=payload, timeout=10)
            response.raise_for_status()
            token_info = response.json()

            access_token = token_info.get("access_token")
            refresh_token = token_info.get("refresh_token")
            expires_in = token_info.get("expires_in")

            if not refresh_token:
                 print("\nERROR: Did not receive a refresh token!")
                 print("Response:", token_info)
            else:
                print("\nSuccessfully obtained tokens!")
                print(f"Refresh Token: ********{refresh_token[-6:]}") 

                # 6. Store Refresh Token in Secret Manager
                print(f"\nStoring Refresh Token in Secret Manager ({REFRESH_TOKEN_SECRET_ID})...")
                secret_name = f"projects/{GCP_PROJECT_ID}/secrets/{REFRESH_TOKEN_SECRET_ID}"
                try:
                    # Add the refresh token as a new version
                    add_version_response = secret_manager_client.add_secret_version(
                        request={
                            "parent": secret_name,
                            "payload": {"data": refresh_token.encode("UTF-8")},
                        }
                    )
                    print(f"Successfully added new version: {add_version_response.name}")
                    print("\n--- Setup Complete! ---")
                    print("You can now run the main Cloud Function, which will use this stored refresh token.")

                except Exception as e:
                    print(f"\nERROR: Failed to store refresh token in Secret Manager!")
                    print(f"Please manually add the refresh token (see above) to the secret '{REFRESH_TOKEN_SECRET_ID}' in GCP Console.")
                    print(f"Error details: {e}")

        except requests.exceptions.RequestException as e:
            print(f"\nERROR exchanging code for tokens: {e}")
            if response is not None:
                print(f"Status Code: {response.status_code}")
                print(f"Response Body: {response.text}")

    elif authorization_code and authorization_code.startswith("ERROR_"):
        print(f"\nAuthorization failed: {authorization_code}. Refresh token not obtained.")
    else:
        print("\nAuthorization code not received (perhaps browser window closed or access denied). Refresh token not obtained.")