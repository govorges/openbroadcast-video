from flask import Flask, request, make_response, jsonify
from os import path
import json

from Video import VideoHandler

HOME_DIR = path.dirname(path.realpath(__file__))
UPLOAD_DIR = path.join(HOME_DIR, "uploads")

api = Flask(__name__)
api_VideoHandle = VideoHandler()

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
    id = request.headers.get("id")
    if id is None or id == "":
        response_text = BuildJSONResponseText("WARNING", "The header \"id\" is not set or was set incorrectly", route="/uploads/create", method="POST")
        return make_response(response_text, 400)
    
    metadata = request.json
    if metadata is None or metadata == "":
        response_text = BuildJSONResponseText("WARNING", "The header \"metadata\" is not set or was set incorrectly", route="/uploads/create", method="POST")
        return make_response(response_text, 400)

    uploadData = api_VideoHandle.createUploadObject(id, metadata)
    return jsonify(uploadData)

@api.route("/uploads/capture", methods=["POST"])
def uploads__Capture():
    id = request.headers.get("id")
    if id is None or id == "":
        response_text = BuildJSONResponseText("WARNING", "The header \"id\" is not set or was set incorrectly", route="/uploads/capture", method="POST")
        return make_response(response_text, 400)

    signatureHash = request.headers.get("signatureHash")
    if signatureHash is None or signatureHash == "":
        response_text = BuildJSONResponseText("WARNING", "The header \"signatureHash\" is not set or was set incorrectly", route="/uploads/capture", method="POST")
        return make_response(response_text, 400)
    
    success = api_VideoHandle.captureUploadObject(id=id, signatureHash=signatureHash)
    if not success:
        return make_response("Upload not captured. Video not registered.", 400)
    return make_response("Upload captured. Video registered.", 200)

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