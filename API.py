from flask import Flask, request, make_response, jsonify
from os import path
import json
import datetime

import cv2

from Video import VideoHandler
from Bunny import BunnyAPI

HOME_DIR = path.dirname(path.realpath(__file__))
UPLOAD_DIR = path.join(HOME_DIR, "uploads")

api = Flask(__name__)
api_VideoHandle = VideoHandler()
api_Bunny = BunnyAPI()

def BuildHTTPResponse(
        headers: dict = None,
        status_code = 200, **kwargs
    ):

    type = kwargs.get("type")
    message = kwargs.get("message")
    message_name = kwargs.get("message_name")

    route = kwargs.get("route")
    method = kwargs.get("method")

    object_data = kwargs.get("object_data")


    resp = make_response()
    resp.status_code = status_code

    if headers is not None:
        resp.headers = headers
    else:
        resp.headers.set("Content-Type", "application/json")
        resp.headers.set("Server", "video")
        resp.headers.set("Date", datetime.datetime.now())
        
    data = {
        "type": type, # Response type
        "message": message, # Response type message
        "route": route, # Request route
        "method": method, # Request method
        "message_name": message_name, # Response data object name (internal)
        "object_data": object_data # Response data object
    }

    resp.set_data(
        json.dumps(data, indent=4)
    )

    return resp

def BuildJSONResponseText(type: str, message: str, route: str, method: str):
    data = {
        "type": type,
        "message": message,
        "route": route,
        "method": method
    }
    return json.dumps(data, indent=4)

@api.route("/uploads/create", methods=["POST"])
def uploads__Create():
    response_data = {
        "type": None,
        "message": None,
        "route": "/uploads/create",
        "method": request.method,
        "object_data": None
    }
    
    # Start request error handling
    id = request.headers.get("id")
    if id is None or id == "":
        response_data["type"] = "FAIL"
        response_data["message"] = "The header \"id\" is not set or was set incorrectly"
    
    metadata = request.json
    if metadata is None or metadata == "":
        response_data["type"] = "FAIL"
        response_data["message"] = "The header \"metadata\" is not set or was set incorrectly"
    
    metadata_required_keys = ["title", "description", "category"]
    metadata_missing_keys = [key for key in metadata_required_keys if metadata.get(key) is None]
    
    if len(metadata_missing_keys) > 0:
        response_data["type"] = "FAIL"
        response_data["message"] = f"Request metadata did not contain [{metadata_missing_keys}]'."
        
        response_data["message_name"] = "metadata_missing_keys"
        response_data["object_data"] = metadata

    if response_data["type"] is not None:
        return BuildHTTPResponse(**response_data)
    # End request error handling
    
    upload_response_data = api_VideoHandle.createUploadObject(id, metadata)
    
    upload_response_type = upload_response_data.get("type")
    if upload_response_type is None:
        response_data["type"] = "FATAL"
        response_data["message"] = f"api_VideoHandle.createUploadObject() response `type` was None."

        response_data["message_name"] = "create_upload_object_fatal_TypeNotFound"

        response_data["object_data"] = None

        return BuildHTTPResponse(**response_data, status_code=500)

    if upload_response_type == "FAIL":
        return BuildHTTPResponse(**upload_response_data, status_code=400)
    
    # Some data is not for external use.
    upload_response_data['object_data']['metadata'].pop('stream_url')
    upload_response_data['object_data']['metadata'].pop('library_id')

    return BuildHTTPResponse(**upload_response_data)

@api.route("/videos/retrieve", methods=["GET"])
def videos__Retrieve():
    guid = request.headers.get("guid")
    if guid is None or guid == "":
        response_text = BuildJSONResponseText("WARNING", "The header \"id\" is not set or was set incorrectly", route="/uploads/capture", method="POST")
        return make_response(response_text, 400)
    
    video = api_Bunny.stream_RetrieveVideo(guid=guid)
    return jsonify(video) # !!! Powerful !!!

@api.route("/videos/thumbnail-upload", methods=["POST"])
def videos__ThumbnailUpload():
    target_file_path = request.headers.get("target-file-path")
    if target_file_path is None or target_file_path == "":
        response_text = BuildJSONResponseText("WARNING", "The header \"target-file-path\" is not set or was set incorrectly", route="/videos/thumbnail-upload", method="POST")
        return make_response(response_text, 400)

    local_file_path = request.headers.get("local-file-path")
    if local_file_path is None or local_file_path == "":
        response_text = BuildJSONResponseText("WARNING", "The header \"local-file-path\" is not set or was set incorrectly", route="/videos/thumbnail-upload", method="POST")
        return make_response(response_text, 400)
    
    target_image_resolution = request.headers.get("target_image_resolution") # 100x200 | WIDTHxHEIGHT
    if target_image_resolution is None or target_image_resolution == "":
        target_image_resolution = (800, 450)
    else:
        target_image_resolution = (int(target_image_resolution.lower().split('x')[0]), int(target_image_resolution.lower().split('x')[1]))

    # Resizing the thumbnail to the required size for our channel by default, but can be optionally specified
    thumbnail_image = cv2.imread(local_file_path)
    resized = cv2.resize(thumbnail_image, target_image_resolution)
    cv2.imwrite(local_file_path, resized)

    status_code = api_VideoHandle.bunny.file_QueueUpload(target_file_path=target_file_path, local_file_path=local_file_path)

    if status_code == 200:
        return make_response("Success", 200)
    return make_response("Fail", status_code)

@api.route("/videos/ingest", methods=["POST"])
def videos__Ingest():
    id = request.headers.get("id")
    if id is None or id == "":
        response_text = BuildJSONResponseText("WARNING", "The header \"id\" is not set or was set incorrectly", route="/videos/ingest", method="POST")
        return make_response(response_text, 400)
    
    metadata = request.json
    if metadata is None or metadata == "":
        response_text = BuildJSONResponseText("WARNING", "The header \"metadata\" is not set or was set incorrectly", route="/videos/ingest", method="POST")
        return make_response(response_text, 400)
    
    description = metadata.get("description")
    if description is None or description == "":
        response_text = BuildJSONResponseText("WARNING", "Invalid metadata (Description is NULL or Empty)", route="/videos/ingest", method="POST")
        return make_response(response_text, 400)
    elif not isinstance(description, str):
        response_text = BuildJSONResponseText("WARNING", "Invalid metadata (Description is of wrong type)", route="/videos/ingest", method="POST")
        return make_response(response_text, 400)
    
    title = metadata.get("title")
    if title is None or title == "":
        response_text = BuildJSONResponseText("WARNING", "Invalid metadata (Title is NULL or Empty)", route="/videos/ingest", method="POST")
        return make_response(response_text, 400)
    elif not isinstance(title, str):
        response_text = BuildJSONResponseText("WARNING", "Invalid metadata (Title is of wrong type)", route="/videos/ingest", method="POST")
        return make_response(response_text, 400)
    
    code = api_VideoHandle.IngestVideo(id=id, video_metadata=metadata)
    if code == 0:
        response_text = BuildJSONResponseText("WARNING", "Video not ingested.", route="/videos/ingest", method="POST")
        return make_response("Video not ingested.", 400)
    return make_response("Video ingested.", 200)

@api.route("/videos/generate_id", methods=["GET"])
def videos__GenerateID():
    response_data = {
        "id": api_VideoHandle.internal__GenerateVideoID()
    }
    return jsonify(response_data)