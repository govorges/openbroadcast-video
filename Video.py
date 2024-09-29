import random
import datetime
from os import path, environ

from threading import Thread
import time

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
        
        self.pollerThread = Thread(target=self.internal__pollUploadProgress, args=(), daemon=True)
        self.pollerThread.start()

    def internal__pollUploadProgress(self):
        while True: 
            time.sleep(10)

            sql_query = '''SELECT * FROM public."Uploads" ORDER BY date_creation ASC''' # earliest first
            self.postgres_cursor.execute(sql_query)

            uploads = self.postgres_cursor.fetchall()
            for upload in uploads: # tuples --- video_id | video_metadata | signature_metadata | date_creation
                video_id = upload[0]
                video_metadata = upload[1]
                signature_metadata = upload[2]

                video = self.bunny.stream_RetrieveVideo(video_metadata.get("guid"))
                statusCode = video.get("status")

                if statusCode in [0, 1, 2, 3] or statusCode in ["0", "1", "2", "3"]:
                    if signature_metadata.get("signature_expiration_time") < datetime.datetime.now().timestamp():
                        self.internal__RemoveUploadObject(video_id=video_id)
                elif statusCode == 4 or statusCode == "4":
                    self.internal__RemoveUploadObject(video_id=video_id)
                    self.internal__createVideoObject(id=video_id, video_metadata=video_metadata)
                else:
                    self.internal__RemoveUploadObject(video_id=video_id)
        

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
        video_guid = video_metadata.get("guid")
        fileData = self.bunny.stream_RetrieveVideo(video_guid)

        metadata = video_metadata
        metadata["feedTags"] = {
            "releasedate": datetime.datetime.now().strftime("%B %d %Y"),
            "title": video_metadata.get("title"),
            "description": video_metadata.get("description"),
            "url": video_metadata.get("stream_url"),
            "poster": video_metadata.get("thumbnail_url"),
            "streamformat": "hls",
            
            "length": fileData.get("length"),
            "width": fileData.get("width"),
            "height": fileData.get("height"),
            "framerate": fileData.get("framerate")
        }

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