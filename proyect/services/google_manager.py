from google.cloud import secretmanager
from proyect.utils.logger import logger
import google.auth
import json

def load_service_account():
    """Load service account credentials from Google Secret Manager."""

    _, project_id = google.auth.default()

    secret_id = "nicogallu-account-service"
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name=name)

    payload = response.payload.data.decode("UTF-8")

    try:
        logger.info("Loading service account credentials...")
        return json.loads(payload)
    except json.JSONDecodeError:
        logger.error("Error decoding JSON from secret payload")
        return payload
