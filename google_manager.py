from google.cloud import secretmanager
import json
import os
from dotenv import load_dotenv


def load_service_account():
    load_dotenv()
    # Your secret name in GCP
    secret_id = "nicogallu-account-service"  # <-- replace with the name you gave
    project_id = os.getenv("GCP_PROJECT")  # auto-set in Cloud Functions
    
    # Build the resource name of the secret
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    
    # Access Secret Manager
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name=name)
    
    # Decode secret payload
    payload = response.payload.data.decode("UTF-8")
    return json.loads(payload)  # this is your credentials dict
