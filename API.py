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

    object = kwargs.get("object")


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
        "message_name": message_name, # Response data object name (internal)

        "route": route, # Request route
        "method": method, # Request method
        
        "object": object # Response data object
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
        "message_name": None,

        "route": "/uploads/create",
        "method": request.method,

        "object": None
    }
    
    # Start request error handling
    id = request.headers.get("id")
    if id is None or id == "":
        response_data["type"] = "FAIL"
        response_data["message"] = "The header \"id\" is not set or was set incorrectly"
        response_data["message_name"] = "id_missing"
    
    metadata = request.json
    if metadata is None or len(metadata.keys()) == 0:
        response_data["type"] = "FAIL"
        response_data["message"] = "The header \"metadata\" is not set or was set incorrectly"
        response_data["message_name"] = "metadata_missing"
    
    metadata_required_keys = ["title", "description", "category"]
    metadata_missing_keys = [key for key in metadata_required_keys if metadata.get(key) is None]
    
    if len(metadata_missing_keys) > 0 and len(metadata.keys()) != 0:
        response_data["type"] = "FAIL"
        response_data["message"] = f"Request metadata did not contain [{metadata_missing_keys}]'."
        response_data["message_name"] = "metadata_missing_keys"

        response_data["object"] = metadata

    if response_data["type"] is not None:
        return BuildHTTPResponse(**response_data, status_code=400)
    # End request error handling
    
    upload_response_data = api_VideoHandle.create_upload_object(id, metadata)
    
    upload_response_type = upload_response_data.get("type")
    if upload_response_type is None:
        response_data["type"] = "FATAL"
        response_data["message"] = f"api_VideoHandle.create_upload_object() response `type` was None."
        response_data["message_name"] = "create_upload_object_fatal_TypeNotFound"

        response_data["object"] = None

        return BuildHTTPResponse(**response_data, status_code=500)

    if upload_response_type != "SUCCESS":
        return BuildHTTPResponse(**upload_response_data, status_code=400)
    
    # Some data is not for external use.
    upload_response_data['object']['metadata'].pop('stream_url')
    upload_response_data['object']['metadata'].pop('library_id')

    return BuildHTTPResponse(**upload_response_data)

@api.route("/videos/retrieve", methods=["GET"])
def videos__Retrieve():
    response_data = {
        "type": None,

        "message": None,
        "message_name": None,

        "route": "/videos/retrieve",
        "method": request.method,

        "object": None
    }

    guid = request.headers.get("guid")
    if guid is None or guid == "":
        response_data["type"] = "FAIL"
        response_data["message"] = "The header \"guid\" is not set or was set incorrectly"
        response_data["message_name"] = "video_guid_missing"

        return BuildHTTPResponse(**response_data)
    
    response = api_Bunny.stream_RetrieveVideo(guid=guid)
    for key in response.keys():
        response_data[key] = response[key]

    if response.get("object") is None:
        return BuildHTTPResponse(**response_data)
    
    return jsonify(response_data["object"])

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
    try:
        resized = cv2.resize(thumbnail_image, target_image_resolution)
        cv2.imwrite(local_file_path, resized)
    except cv2.error: # and then if this doesn't exist you've got yourself a good ole fashioned error.
        thumbnail_image = cv2.imread(path.join(UPLOAD_DIR, "THUMBNAIL_DEFAULT.png"))
        resized = cv2.resize(thumbnail_image, target_image_resolution)
        cv2.imwrite(local_file_path, resized)

    upload_request = api_VideoHandle.bunny.file_Upload(target_file_path=target_file_path, local_file_path=local_file_path)

    return BuildHTTPResponse(**upload_request)

@api.route("/videos/generate_id", methods=["GET"])
def videos__GenerateID():
    response_data = {
        "id": api_VideoHandle.utility_generate_video_id()
    }
    return jsonify(response_data)