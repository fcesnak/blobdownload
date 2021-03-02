import uuid
from io import StringIO
from azure.storage.blob import BlobServiceClient, ContainerClient


CONNECTION_STRING = "INSERT YOUR CONNECTION STRING HERE"
BLOB_CONTAINER = "bigquery"

class Blob:

    def __init__(self, container_name):
        self.container_name = container_name
        self.blob_service = self.authenticate_blob()
        self.container_client = self.authenticate_container(container_name)

    @staticmethod
    def authenticate_blob():
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)

        return blob_service

    @staticmethod
    def authenticate_container(container_name):
        container_service = ContainerClient.from_connection_string(CONNECTION_STRING, container_name=container_name)

        return container_service

    def create_container(self):
        self.blob_service.create_container(self.container_name)

        return

    def last_blob(self):
        files = self.container_client.list_blobs()
        blobs = {}

        for file in files.enumerate():
            blobs.update({file.name: file.creation_time})

        return max(blobs, key=blobs.get)

    def get_blob(self):
        local_path = os.path.expanduser("~/Data")
        local_file_name = "bigquery_" + str(uuid.uuid4()) + ".json"
        full_path = os.path.join(local_path, local_file_name)

        with open(local_file_name, "wb") as download_file:
            download_file.write(self.container_client.download_blob(last_blob()))

bigquery = Blob(BLOB_CONTAINER)
bigquery.get_blob()
