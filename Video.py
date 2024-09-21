import cv2
import random
import datetime
from os import path, environ
import json

from Bunny import BunnyAPI

import psycopg2

HOME_DIR = path.dirname(path.realpath(__file__))
PULL_ZONE_ROOT = environ["BUNNY_PULL_ZONE_ROOT"]
LIBRARY_CDN_HOSTNAME = environ["LIBRARY_CDN_HOSTNAME"]

class VideoHandler:
    def __init__(self):
        self.bunny = BunnyAPI()       
        self.postgres_connection = psycopg2.connect(
            database=environ["POSTGRESDB_DATABASE"],
            host=environ["POSTGRESDB_HOST"],
            user=environ["POSTGRESDB_USER"],
            password=environ["POSTGRES_PASSWORD"],
            port=environ["POSTGRESDB_DOCKER_PORT"]
        )
        self.postgres_cursor = self.postgres_connection.cursor()

        self.UPLOAD_FOLDER = path.join(HOME_DIR, "uploads")

    def internal__videoIDAlreadyExists(self, id: str):
        fileList = self.bunny.file_List("videos/")
        for file in fileList:
            if file.get("ObjectName").replace(".mp4", "") == id:
                return True
        return False

    def internal__GenerateVideoID(self):
        def gen():
            seq = "AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz1234567890"
            _id = ""
            for x in range(0, 12):
                _id += random.choice(seq)
            return _id
        
        id = gen()
        while self.internal__videoIDAlreadyExists(id):
            id = gen()
        return id
    
    def internal_IsValidVideoID(self, id):
        seq = "AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz1234567890"
        for char in id:
            if char not in seq:
                return False
        return True
    
    def internal__RemoveUploadObject(self, video_id):
        sql_query = f"""
        DELETE FROM public."Uploads" WHERE video_id = %s
        """
        self.postgres_cursor.execute(sql_query, (video_id,))
        self.postgres_connection.commit()

    def internal__RetrieveUploadObject(self, video_id):
        sql_query = f"""
        SELECT video_id, video_metadata, signature_metadata, date_creation FROM public."Uploads" WHERE video_id = %s
        """
        self.postgres_cursor.execute(sql_query, (video_id,))
        uploadData = self.postgres_cursor.fetchone()

        return uploadData
    
    def internal__createVideoObject(self, id: str, video_metadata: dict):
        videoJson = json.dumps(video_metadata)
        sql_query = f"""
        INSERT INTO public."Videos" (video_id, video_metadata)
        VALUES (%s, %s);
        """
        self.postgres_cursor.execute(sql_query, (id, videoJson))
        self.postgres_connection.commit()
        
    
    def createUploadObject(self, id: str, video_metadata: dict):
        if not self.internal_IsValidVideoID(id):
            return None
        
        videoObj = self.bunny.stream_CreateVideo(videoTitle=video_metadata.get("title"))
        self.bunny.stream_UpdateVideo(videoObj.get("guid"), payload = {
            "metaTags": [
                {
                    "property": "description",
                    "value": video_metadata.get("description", "A video uploaded to OpenBroadcast.")
                }
            ]
        })
        video_metadata["guid"] = videoObj.get("guid")

        metadata = {
            "title": video_metadata.get("title"),
            "description": video_metadata.get("description"),
            "category": video_metadata.get("category"),
            "duration": None,
            "resolution_w": None,
            "resolution_h": None,
            "fps": None,
            "guid": video_metadata['guid'],
            "library_id": environ["BUNNY_STREAMLIBRARY_ID"],
            "id": id,
            "thumbnail_url": f"{PULL_ZONE_ROOT}/thumbnails/{id}.png",
            "stream_url": f"{LIBRARY_CDN_HOSTNAME}/{video_metadata['guid']}/playlist.m3u8",
        }

        tusSignature = self.bunny.upload_CreateSignature(video_metadata['guid'])
        requiredKeys = ["signature", "signature_expiration_time", "library_id"]
        
        for key in requiredKeys:
            if tusSignature.get(key) is None or tusSignature.get(key) == "":
                return None
        
        sql_query = f"""
        INSERT INTO public."Uploads" (video_id, video_metadata, signature_metadata)
        VALUES (%s, %s, %s);
        """

        videoJson = json.dumps(metadata)
        signatureJson = json.dumps(tusSignature)

        self.postgres_cursor.execute(sql_query, (id, videoJson, signatureJson))
        self.postgres_connection.commit()

        return { "signature": tusSignature, "metadata": metadata }
    
    def captureUploadObject(self, id: str, signatureHash: str):
        if not self.internal_IsValidVideoID(id):
            return False
        
        uploadData = self.internal__RetrieveUploadObject(video_id=id)
        
        signature_metadata = uploadData[2]
        if signature_metadata.get("signature") != signatureHash:
            return False
        
        self.internal__RemoveUploadObject(video_id=id)
        self.internal__createVideoObject(id=id, video_metadata=uploadData[1])

        return True
