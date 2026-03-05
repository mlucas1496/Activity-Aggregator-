"""
Flask server: routes, SSE progress, file handling.
"""
import os
import sys
import uuid
import json
import time
import threading
import traceback
from flask import Flask, render_template, request, jsonify, Response, send_file

# Add app directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipeline.orchestrator import run_pipeline

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Bundled mapping files (read automatically, not uploaded)
SEARCH_STRINGS_PATH = os.path.join(DATA_DIR, "Search_Strings_V2.xlsx")
STATIC_MAPPING_PATH = os.path.join(DATA_DIR, "Static_Mapping_5.02.xlsx")

# In-memory session state
sessions = {}


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    """Save uploaded files and return a session ID."""
    session_id = str(uuid.uuid4())[:8]
    session_dir = os.path.join(UPLOAD_DIR, session_id)
    os.makedirs(session_dir, exist_ok=True)

    saved = {}
    for key in ["prev_week", "bank_statements", "all_transactions", "loan_report"]:
        f = request.files.get(key)
        if f and f.filename:
            path = os.path.join(session_dir, f"{key}_{f.filename}")
            f.save(path)
            saved[key] = path

    # Add bundled mapping files automatically
    saved["search_strings"] = SEARCH_STRINGS_PATH
    saved["static_mapping"] = STATIC_MAPPING_PATH

    sessions[session_id] = {
        "file_paths": saved,
        "status": "uploaded",
        "logs": [],
        "stages": {},
        "result": None,
        "error": None,
    }

    return jsonify({"session_id": session_id, "files": list(saved.keys())})


@app.route("/run/<session_id>", methods=["POST"])
def run(session_id):
    """Launch pipeline in a background thread."""
    session = sessions.get(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session["status"] == "running":
        return jsonify({"error": "Pipeline already running"}), 409

    session["status"] = "running"
    session["logs"] = []
    session["stages"] = {}
    session["result"] = None
    session["error"] = None

    def log(msg):
        session["logs"].append(msg)

    def set_stage(stage_id, status):
        session["stages"][stage_id] = status

    def worker():
        try:
            output_dir = os.path.join(OUTPUT_DIR, session_id)
            result = run_pipeline(session["file_paths"], output_dir, log, set_stage)
            session["result"] = result
            session["status"] = "done"
        except Exception as e:
            session["error"] = str(e)
            session["status"] = "error"
            log(f"ERROR: {e}")
            traceback.print_exc()
            # Mark running stages as error
            for k, v in list(session["stages"].items()):
                if v == "run":
                    session["stages"][k] = "err"

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

    return jsonify({"status": "started"})


@app.route("/progress/<session_id>")
def progress(session_id):
    """SSE endpoint for real-time progress updates."""
    session = sessions.get(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404

    def generate():
        last_log_idx = 0
        last_stages = {}

        while True:
            # Send new log entries
            current_logs = session["logs"]
            if len(current_logs) > last_log_idx:
                new_logs = current_logs[last_log_idx:]
                last_log_idx = len(current_logs)
                for msg in new_logs:
                    yield f"data: {json.dumps({'type': 'log', 'message': msg})}\n\n"

            # Send stage updates
            current_stages = dict(session["stages"])
            if current_stages != last_stages:
                yield f"data: {json.dumps({'type': 'stages', 'stages': current_stages})}\n\n"
                last_stages = current_stages

            # Send status
            status = session["status"]
            if status == "done":
                stats = session["result"]["stats"] if session["result"] else {}
                yield f"data: {json.dumps({'type': 'done', 'stats': stats})}\n\n"
                break
            elif status == "error":
                yield f"data: {json.dumps({'type': 'error', 'message': session['error']})}\n\n"
                break

            time.sleep(0.3)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/download/<session_id>")
def download(session_id):
    """Download the output XLSX."""
    session = sessions.get(session_id)
    if not session or not session.get("result"):
        return jsonify({"error": "No output available"}), 404

    result = session["result"]
    return send_file(
        result["file_path"],
        as_attachment=True,
        download_name=result["file_name"],
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5050, debug=False, threaded=True)
