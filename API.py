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
        "id": api_VideoHandle.GenerateVideoID()
    }
    return jsonify(response_data)