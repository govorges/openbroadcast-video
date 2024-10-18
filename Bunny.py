import requests
from os import environ

class BunnyAPI:
    def __init__(self):
        self.API_Endpoint_URL = environ["BUNNY_ENDPOINT_ADDRESS"]
    def file_Upload(self, target_file_path: str, local_file_path: str):
        headers = {
            "target-file-path": target_file_path,
            "local-file-path": local_file_path
        }
        request = requests.post(f"http://{self.API_Endpoint_URL}/files/upload", headers=headers)

        return request.json()
    
    def file_List(self, path: str):
        headers = {
            "path": path
        }
        request = requests.get(f"http://{self.API_Endpoint_URL}/files/list", headers=headers)
        requestJson = request.json()

        return requestJson.get("object")
    
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
        request = requests.get(f"http://{self.API_Endpoint_URL}/stream/create-signature", headers=headers)
        requestJson = request.json()

        return requestJson.get("object")
        
    def stream_CreateVideo(self, videoTitle: str):
        headers = {
            "title": videoTitle
        }
        request = requests.get(f"http://{self.API_Endpoint_URL}/stream/create-video", headers=headers)
        requestJson = request.json()

        return requestJson.get("object")
    
    def stream_UpdateVideo(self, guid: str, payload: dict):
        headers = {
            "guid": guid,
        }
        request = requests.post(f"http://{self.API_Endpoint_URL}/stream/update-video", headers=headers, json=payload)

        return request.status_code
    
    def stream_RetrieveVideo(self, guid: str):
        headers = {
            "guid": guid
        }
        request = requests.get(f"http://{self.API_Endpoint_URL}/stream/retrieve-video", headers=headers)
        requestJson = request.json()

        return requestJson

    def stream_ListVideos(self, libraryId: str = None):
        headers = {
            "libraryId": libraryId
        }
        request = requests.get(f"http://{self.API_Endpoint_URL}/stream/videos", headers=headers)
        requestJson = request.json()

        return requestJson.get("object")
    
    def stream_DeleteVideo(self, guid: str):
        headers = {
            "guid": guid
        }
        request = requests.post(f"http://{self.API_Endpoint_URL}/stream/delete-video", headers=headers)
        requestJson = request.json()

        return requestJson