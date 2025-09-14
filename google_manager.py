import google.auth
from google.cloud import secretmanager
import json

def load_service_account():
    # Get project ID dynamically
    _, project_id = google.auth.default()

    secret_id = "nicogallu-account-service"
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name=name)

    payload = response.payload.data.decode("UTF-8")

    try:
        print("Decoding JSON payload")
        return json.loads(payload)
    except json.JSONDecodeError:
        print("failed to decode JSON, returning raw payload")
        return payload
