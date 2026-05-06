import os
import requests
import json
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
from config.config import Config

SCHEMA_DIR = os.getenv("SCHEMA_DIR", Config.SPARK_SCHEMAS_DIR)
REGISTRY_URL = os.getenv("REGISTRY_URL", "http://localhost:8095/apis/registry/v2/groups/default/artifacts")

def register_schemas():
    if not os.path.exists(SCHEMA_DIR):
        logger.error(f"Directory {SCHEMA_DIR} does not exist.")
        return

    for filename in os.listdir(SCHEMA_DIR):
        if filename.endswith('.avsc'):
            file_path = os.path.join(SCHEMA_DIR, filename)
            
            # Set Artifact ID (e.g., listen_events-value)
            artifact_id = f"{filename.replace('.avsc', '')}-value"
            
            try:
                with open(file_path, 'r') as f:
                    schema_content = f.read()

                logger.info(f"Processing artifact: {artifact_id}...")

                # Registration request
                # Using ifExists=RETURN_OR_UPDATE to auto-update if it already exists
                params = {
                    'ifExists': 'RETURN_OR_UPDATE'
                }
                headers = {
                    'Content-Type': 'application/json; artifactType=AVRO',
                    'X-Registry-ArtifactId': artifact_id
                }

                response = requests.post(
                    REGISTRY_URL, 
                    data=schema_content, 
                    headers=headers, 
                    params=params
                )
                
                if response.status_code in [200, 201]:
                    logger.info(f"Successfully registered: {artifact_id}")
                else:
                    logger.error(f"Failed to register: {artifact_id} - Status: {response.status_code}")
                    logger.error(f"Response: {response.text}")
            except FileNotFoundError:
                logger.error(f"Schema file not found: {file_path}")
            except Exception as e:
                logger.error(f"Connection error occurred: {str(e)}")

if __name__ == "__main__":
    register_schemas()
