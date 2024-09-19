import cv2
import random
import datetime
from os import path, environ
import json

from Bunny import BunnyAPI

import psycopg2

HOME_DIR = path.dirname(path.realpath(__file__))
PULL_ZONE_ROOT = environ["BUNNY_PULL_ZONE_ROOT"]

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

    def internal_videoIDAlreadyExists(self, id: str):
        fileList = self.bunny.file_List("videos/")
        for file in fileList:
            if file.get("ObjectName").replace(".mp4", "") == id:
                return True
        return False
    
    def internal_writeVideoThumbnail(self, id: str):
        image = cv2.imread(path.join(self.UPLOAD_FOLDER, f"{id}.png"))

        newImage = cv2.resize(image, (800, 450))
        
        cv2.imwrite(path.join(self.UPLOAD_FOLDER, f"{id}.png"), newImage)

    def internal_GenerateVideoJSON(self, id: str, video_metadata: dict):

        video_datastream_url = PULL_ZONE_ROOT + f"/videos/{id}.mp4"
        video_thumbnail_url = PULL_ZONE_ROOT + f"/videos/{id}.png"

        # heres hoping that opencv2 doesnt end up having some fucked up vulnerability in the future.
        # (foreshadowing...)
        cap = cv2.VideoCapture(path.join(HOME_DIR, "uploads", f"{id}.mp4"))
        fps = cap.get(cv2.CAP_PROP_FPS)      # OpenCV v2.x used "CV_CAP_PROP_FPS"
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        duration = str(int(frame_count / fps))

        outputJson = {
            "irc:title": video_metadata.get("title"),
            "irc:releasedate": datetime.datetime.now().strftime("%Y-%m-%d"),
            "irc:description": video_metadata.get("description"),
            "irc:url": video_datastream_url,
            "irc:poster": video_thumbnail_url,
            "irc:length": duration,
            "irc:streamformat": "mp4"
        } 

        return outputJson

    def internal__GrabVideoDuration(self, filename):
        video = cv2.VideoCapture(filename)
        duration = video.get(cv2.CAP_PROP_POS_MSEC)
        return duration

    def internal__GenerateVideoID(self):
        def gen():
            seq = "AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz1234567890"
            _id = ""
            for x in range(0, 12):
                _id += random.choice(seq)
            return _id
        
        id = gen()
        while self.internal_videoIDAlreadyExists(id):
            id = gen()
        return id
    
    def createUploadObject(self, id: str, video_metadata: dict):
        tusSignature = self.bunny.upload_CreateSignature(id)
        requiredKeys = ["signature", "signature_expiration_time", "library_id"]
        
        for key in requiredKeys:
            if tusSignature.get(key) is None or tusSignature.get(key) == "":
                return None
        
        sql_query = f"""
        INSERT INTO public."Uploads" (video_id, video_metadata, signature_metadata)
        VALUES (%s, %s, %s);
        """

        videoJson = json.dumps(video_metadata)
        signatureJson = json.dumps(tusSignature)

        self.postgres_cursor.execute(sql_query, (id, videoJson, signatureJson))
        self.postgres_connection.commit()

        return { "signature": tusSignature, "metadata": video_metadata }
    

    def IngestVideo(self, id: str, video_metadata: dict):
        try:
            self.internal_writeVideoThumbnail(id)
            self.bunny.file_QueueUpload(
                local_file_path = path.join(HOME_DIR, "uploads", f"{id}.png"),
                target_file_path = f"/videos/{id}.png"
            )   
            self.bunny.file_QueueUpload(
                local_file_path = path.join(HOME_DIR, "uploads", f"{id}.mp4"),
                target_file_path = f"/videos/{id}.mp4"
            )
            videoJson = self.internal_GenerateVideoJSON(id, video_metadata)
            videoJson = json.dumps(videoJson)
            sql_query = f"""
            INSERT INTO public."Videos" (video_id, video_metadata)
            VALUES (%s, %s);
            """
            self.postgres_cursor.execute(sql_query, (id, videoJson))
            self.postgres_connection.commit()
        except:
            return 0
        return 1

