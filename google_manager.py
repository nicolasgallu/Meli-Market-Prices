from google.cloud import secretmanager
import json
import os

def load_service_account():
    # Cloud Functions sets GOOGLE_CLOUD_PROJECT automatically
    project_id = os.environ["GOOGLE_CLOUD_PROJECT"]
    secret_id = "nicogallu-account-service"  # must match the secret name in Secret Manager
    
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name=name)
    
    payload = response.payload.data.decode("UTF-8")
    
    # Return JSON if possible, otherwise raw string
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return payload
