import logging
import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient, ContainerClient
from dotenv import load_dotenv
import validators
from io import BytesIO
import requests

# Configure logging level
logging.basicConfig(level=logging.INFO)

# load env variables
load_dotenv()

app = func.FunctionApp()

# env variables to acces into blob storage and API urls.
CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
API_URLs = [os.getenv("ETABLISSEMENT_URL"), os.getenv("SOCIETE_URL")]

# check validity of env variables
if not CONNECTION_STRING or not CONTAINER_NAME or not any(API_URLs):
    logging.error("Une ou plusieurs variables d'environnement sont manquantes.")
    exit(1)

# initialize connexion into the blob storage and container
blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

@app.schedule(schedule="0 */30 * * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False)
def gouv_api(myTimer: func.TimerRequest) -> None:
    """
        API client to download a stream files.
        Scheduled to be run each 30 minutes. 
    """
    for url in API_URLs:
        if not url:
            logging.error("URL not specified.")
            return
        
        # check the validity of url. 
        if not validators.url(url):
            logging.error(f"URL invalide : {url}")
            return
        
        logging.info(f"Start downloading : {url}")
        
        # get the name of the zip file.
        filename = os.path.basename(url)
        try:
            blob_client = container_client.get_blob_client(filename)
            
            # Delete older blob if exists
            if blob_client.exists():
                blob_client.delete_blob()
                logging.info(f"Blob {filename} supprimé.")

        # Download the new file and load it in the the container. 
            response = requests.get(url, stream=True, timeout=(5, 30))
            response.raise_for_status()
            
            # Downloading by chunks of 4MB 
            chunk_size = 4 * 1024 * 1024  
            with BytesIO() as f:
                total_size = int(response.headers.get('content-length', 0))
                downloaded_size = 0
                next_percentage_to_report = 10
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        percent_complete = (downloaded_size / total_size) * 100
                        if total_size > 0 and percent_complete >= next_percentage_to_report:
                            logging.info(f"Downloaded: {percent_complete:.1f}%")
                            next_percentage_to_report += 10 
                        f.seek(0)
                        blob_client.upload_blob(f, blob_type="AppendBlob", length=len(chunk))
                        f.seek(0)
                        f.truncate(0)
            
            logging.info(f"file {filename} has been saved successfully into the blob storage.")
        # exception if there are an error
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to download URL : {url}. Error : {e}")
            
        except Exception as e:
            logging.error(f"Error when trying to save in the blob staorage : {str(e)}")

    if myTimer.past_due:
        logging.warning('Trigger due !')

    logging.info('Function has been finished!')