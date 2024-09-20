import requests
from os import environ

class BunnyAPI:
    def __init__(self):
        self.API_Endpoint_URL = environ["BUNNY_ENDPOINT_ADDRESS"]
    def file_QueueUpload(self, target_file_path: str, local_file_path: str):
        headers = {
            "target-file-path": target_file_path,
            "local-file-path": local_file_path
        }
        request = requests.post(f"http://{self.API_Endpoint_URL}/files/upload", headers=headers)

        return request.status_code
    
    def file_List(self, path: str):
        headers = {
            "path": path
        }
        request = requests.get(f"http://{self.API_Endpoint_URL}/files/list", headers=headers)

        return request.json()
    
    def file_Delete(self, target_file_path: str):
        headers = {
            "target-file-path": target_file_path
        }
        request = requests.delete(f"http://{self.API_Endpoint_URL}/files/delete", headers=headers)

        return request.status_code
    
    def file_Retrieve(self, target_file_path: str):
        headers = {
            "target-file-path": target_file_path
        }
        request = requests.get(f"http://{self.API_Endpoint_URL}/files/retrieve", headers=headers)

        return request.status_code

    def cache_Purge(self, target_url: str):
        headers = {
            "target-url": target_url
        }
        request = requests.post(f"http://{self.API_Endpoint_URL}/cache/purge", headers=headers)

        return request.status_code
    
    def upload_CreateSignature(self, videoID: str):
        headers = {
            "videoID": videoID
        }
        request = requests.get(f"http://{self.API_Endpoint_URL}/upload/create-signature", headers=headers)

        signatureData = request.json()
        return signatureData
        