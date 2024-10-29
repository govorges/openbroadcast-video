from os import path, environ
from threading import Thread

import random
import json
import time

import psycopg2.errors as psycopg2_errors
import datetime

from Bunny import BunnyAPI
from Database import Database

SERVICE_DIR = path.dirname(path.realpath(__file__))
HOME_DIR = SERVICE_DIR.rsplit(path.sep, 1)[0]

PULL_ZONE_ROOT = environ["BUNNY_PULL_ZONE_ROOT"]
LIBRARY_CDN_HOSTNAME = environ["LIBRARY_CDN_HOSTNAME"]

def wkw(**kwargs):
    return kwargs

class VideoHandler:
    def __init__(self):
        self.bunny = BunnyAPI() # Initialization args loaded from env
        self.database = Database() # same

        self.UPLOAD_FOLDER = path.join(HOME_DIR, "uploads")
        self.poller_thread = Thread(target=self.poll_upload_progress, args=(), daemon=True).start()
    def poll_upload_progress(self):
        poller_delay_seconds = random.uniform(10.0, 30.0) # 10.0 -> 30.0
        while True:
            time.sleep(poller_delay_seconds)

            sql_query = f"""
            SELECT * FROM public."Uploads" ORDER BY date_creation ASC
            """ # Earliest first.
            cursor = self.database.execute_sql_query(sql_query)

            uploads = cursor.fetchall()
            if len(uploads) == 0:
                continue

            for upload in uploads:
                video_id, video_metadata, signature_metadata = upload[0], upload[1], upload[2]
                
                remote_video_object_response = self.bunny.stream_RetrieveVideo(video_metadata.get("guid"))
                # Checking the message name guarantees that we are only deleting
                #       uploads that were never registered in the Stream Library
                if remote_video_object_response.get("message_name") == "video_not_found":
                    self.video_delete_by_id(video_id=video_id)
                    continue

                remote_video_object = remote_video_object_response.get("object")
                if remote_video_object is None:
                    # Unknown issue occurred, but this does not necessarily mean the video
                    #       doesn't exist, just that the response did not give us it.
                    #               (Could be connection issues to bunnyapi service or Bunny's API)
                    continue

                upload_status_code = int(remote_video_object.get("status"))
                if upload_status_code in [0, 1, 2, 3]:
                    if signature_metadata.get("signature_expiration_time") < datetime.datetime.now().timestamp():
                        self.uploads_delete_by_id(video_id=video_id)
                elif upload_status_code == 4:
                    self.uploads_delete_by_id(video_id=video_id)
                    self.create_video_object(
                        id = video_id,
                        video_metadata = video_metadata
                    )
                elif upload_status_code > 4:
                    self.uploads_delete_by_id(video_id=video_id)
            self.cleanup_stream_library()
    def cleanup_stream_library(self):
        stream_library_videos_response = self.bunny.stream_ListVideos()
        if stream_library_videos_response.get("message_name") != "video_list_retrieve_success":
            return
        
        stream_library_videos = stream_library_videos_response.get("object")['items']
        stream_library_guids = [x.get("guid") for x in stream_library_videos]

        local_videos = self.videos_list()
        local_guids = [x[1].get("guid") for x in local_videos]

        for video in local_videos:
            video_id = video[0]
            video_guid = video[1].get('guid')
            if video_guid not in stream_library_guids:
                # Video exists in our database but is not in the Stream Library
                #       -> Remove from database, this is a "dead" entry.
                self.video_delete_by_id(video_id=video_id)
        
        for video in stream_library_videos:
            video_guid = video.get("guid")

            video_id = None
            for tag in video.get("metaTags"):
                if tag['property'] == "video_id":
                    video_id = tag['value']
            if video_id is None: # For some reason this video does not have video_id metaTag.
                continue


            video_upload_status = video.get("status")
            if video_upload_status == 4 and video_guid not in local_guids:
                # This video is marked as "Uploaded" in Bunny but does not exist in our local guids.
                #       There is a race condition here. A video can be "Uploaded" in Bunny
                #           but could be in our "Uploads" still (did not poll upload progess quickly enough)
                #               So, we check if it's in our uploads before deleting it.
                if self.uploads_retrieve_by_id(video_id=video_id) is None:
                    self.bunny.stream_DeleteVideo(video_guid)
            if video_upload_status in [5, 6]: # Upload failed, we can remove the video from the Stream Library.
                self.bunny.stream_DeleteVideo(video_guid)
            

            





    def video_retrieve_by_id(self, video_id: str) -> tuple:
        '''Retrieves a video object using a `video_id` from Public."Videos"'''
        sql_query = f"""
        SELECT video_id, video_metadata, date_created, date_modified FROM public."Videos" WHERE video_id = %s
        """
        cursor = self.database.execute_sql_query(sql_query, args=(video_id,))
        return cursor.fetchone()
    def video_delete_by_id(self, video_id: str) -> None:
        '''Deletes a video object using a `video_id` from Public."Videos"'''
        sql_query = f"""
        DELETE FROM public."Videos" WHERE video_id = %s
        """
        self.database.execute_sql_query(sql_query, args=(video_id,))
    def videos_list(self) -> list:
        '''Retrieves a list of all video objects in Public."Videos"'''
        sql_query = f"""
        SELECT * FROM public."Videos"
        ORDER BY video_id ASC
        """
        cursor = self.database.execute_sql_query(sql_query)

        videos = cursor.fetchall()
        return videos



    def uploads_retrieve_by_id(self, video_id: str) -> tuple:
        '''Retrieves an upload using a `video_id` from Public."Uploads"'''
        sql_query = f"""
        SELECT video_id, video_metadata, signature_metadata, date_creation FROM public."Uploads" WHERE video_id = %s
        """
        cursor = self.database.execute_sql_query(sql_query, args=(video_id,))
        return cursor.fetchone()
    def uploads_delete_by_id(self, video_id: str) -> None:
        '''Deletes an upload using a `video_id` from Public."Uploads"'''
        sql_query = f"""
        DELETE FROM public."Uploads" WHERE video_id = %s
        """
        self.database.execute_sql_query(sql_query, args=(video_id,))
    def uploads_list(self) -> list:
        '''Retrieves a list of all upload objects in Public."Uploads"'''
        sql_query = f"""
        SELECT * FROM public."Uploads" ORDER BY date_creation ASC
        """
        cursor = self.database.execute_sql_query(sql_query)
        return cursor.fetchall()
    


    def utility_generate_video_id(self) -> str:
        '''Provides a unique video ID compatible with our service. (12-char alphanumeric)'''
        def gen():
            seq = "AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz1234567890"
            _id = ""
            for x in range(0, 12):
                _id += random.choice(seq)
            return _id
        
        id = gen()
        while self.utility_does_video_id_exist(id):
            id = gen()
        return id
    def utility_is_video_id_valid(self, video_id: str) -> bool:
        '''Checks if a video_id is properly formatted.'''
        if len(video_id) != 12:
            return False
        seq = "AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz1234567890"
        for char in video_id:
            if char not in seq:
                return False
        return True
    def utility_does_video_id_exist(self, video_id: str) -> bool:
        '''Checks if a video with video_id already exists in Public."Videos"'''
        if self.video_retrieve_by_id(video_id=video_id) is not None:
            return True
        return False



    def create_upload_object(self, id: str, video_metadata: dict) -> dict:
        if not self.utility_is_video_id_valid(id):
            function_response = wkw(
                type = "FAIL",
                message = "`id` is formatted incorrectly.",
                message_name = "invalid_video_id",
                object = f"{id}"
            )
            return function_response
        
        remote_video_object = self.bunny.stream_CreateVideo(
            videoTitle = video_metadata["title"]
        )
        self.bunny.stream_UpdateVideo(remote_video_object["guid"], payload = {
            "metaTags": [
                {
                    "property": "description",
                    "value": video_metadata.get("description", "A video uploaded to OpenBroadcast.")
                },
                {
                    "property": "video_id",
                    "value": id
                }
            ]
        })
        video_metadata["guid"] = remote_video_object["guid"]
        metadata = {
            "title": video_metadata.get("title"),
            "description": video_metadata.get("description", "A video uploaded to OpenBroadcast."),
            "category": video_metadata.get("category"),
            "guid": video_metadata['guid'],
            "library_id": environ["BUNNY_STREAMLIBRARY_ID"],
            "id": id,
            "thumbnail_url": f"{PULL_ZONE_ROOT}/thumbnails/{id}.png",
            "stream_url": f"{LIBRARY_CDN_HOSTNAME}/{video_metadata['guid']}/playlist.m3u8",
        }

        # Generating a TUS signature hash for the upload.
        #       Requires information unavailable on this service.
        signature = self.bunny.upload_CreateSignature(video_metadata["guid"])
    
        video_json = json.dumps(metadata)
        signature_json = json.dumps(signature)

        sql_query = f"""
        INSERT INTO public."Uploads" (video_id, video_metadata, signature_metadata)
        VALUES (%s, %s, %s);
        """
        try:
            self.database.execute_sql_query(sql_query, args=(id, video_json, signature_json))
        except psycopg2_errors.UniqueViolation:
            function_response = wkw(
                type = "FAIL",
                message = f"Upload object with id {id} already exists.",
                message_name = "duplicate_id",
                object = f"{id}"
            )
            return function_response
        except psycopg2_errors.InFailedSqlTransaction: 
            function_response = wkw(
                type = "FAIL",
                message = f"Transaction with database failed.",
                message_name = "in_failed_sql_transaction",
                object = None
            )
            return function_response
        except: # some mystical shit happened
            function_response = wkw(
                type = "FAIL",
                message = f"Transaction with database failed.",
                message_name = "database_tx_failed_misc",
                object = None
            )
            return function_response
        
        function_response = wkw(
            type = "SUCCESS",
            message = "Upload Created Successfully",
            message_name = "upload_creation_success",
            object = {
                "signature": signature,
                "metadata": metadata
            }
        )

        return function_response
    def create_video_object(self, id: str, video_metadata: dict) -> None:
        if self.video_retrieve_by_id(id) is not None:
            return # Video already exists. 
        
        video_guid = video_metadata["guid"]
        
        file_data = self.bunny.stream_RetrieveVideo(video_guid).get("object")
        if file_data is None:
            return # Video does not exist on Bunny or the connection with Bunny's API was lost.
        
        metadata = video_metadata
        metadata["feedTags"] = {
            "releasedate": datetime.datetime.now().strftime("%B %d %Y"),
            "title": video_metadata['title'],
            "description": video_metadata.get("description"),
            "url": video_metadata['stream_url'],
            "poster": video_metadata['thumbnail_url'],
            "streamformat": "hls",
            
            "length": file_data.get("length"),
            "width": file_data.get("width"),
            "height": file_data.get("height"),
            "framerate": file_data.get("framerate")
        }

        video_json = json.dumps(metadata)
        
        sql_query = f"""
        INSERT INTO public."Videos" (video_id, video_metadata)
        VALUES (%s, %s);
        """
        self.database.execute_sql_query(sql_query, args=(id, video_json))
        