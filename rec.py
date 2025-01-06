import os
import subprocess
import threading
import time
import json
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException
#from fastapi.responses import StreamingResponse
from starlette.responses import FileResponse
from pydantic import BaseModel
from fastapi.responses import Response
from starlette.responses import StreamingResponse

# FastAPI app
app = FastAPI()

# Configuration
STREAMS_DIR = "E:\Streams"
RETENTION_DAYS = 365
RECORD_DURATION = 3600  # 1 hour in seconds
CONFIG_FILE = "streams.json"

# Manage active streams
active_streams = {}


class Stream(BaseModel):
    address: str
    port: int
    stream_name: str
    channel_type: str

    # Validate that channel_type is either "radio" or "tv"
    def __init__(self, **data):
        super().__init__(**data)
        if self.channel_type not in {"radio", "tv"}:
            raise ValueError("channel_type must be 'radio' or 'tv'")


def ensure_dir(path):
    """Ensure directory exists."""
    os.makedirs(path, exist_ok=True)


def load_streams_from_config():
    """Load streams from the configuration file."""
    if not os.path.exists(CONFIG_FILE):
        return {}

    with open(CONFIG_FILE, "r") as file:
        try:
            return json.load(file)
        except json.JSONDecodeError:
            return {}


def save_streams_to_config():
    """Save active streams to the configuration file."""
    with open(CONFIG_FILE, "w") as file:
        json.dump(
            {
                stream_name: {k: v for k, v in data.items() if k != "thread"}
                for stream_name, data in active_streams.items()
            },
            file,
            indent=4
        )


def cleanup_old_files():
    """Periodically remove files older than RETENTION_DAYS."""
    now = datetime.now()
    cutoff = now - timedelta(days=RETENTION_DAYS)

    for stream_folder in os.listdir(STREAMS_DIR):
        folder_path = os.path.join(STREAMS_DIR, stream_folder)
        if os.path.isdir(folder_path):
            for file in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file)
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                if file_time < cutoff:
                    os.remove(file_path)


def record_stream(address, port, stream_name):
    """Record a multicast stream in TS format with compression."""
    stream_folder = os.path.join(STREAMS_DIR, stream_name)
    ensure_dir(stream_folder)

    log_file_path = os.path.join(stream_folder, "recording.log")

    with open(log_file_path, "a") as log_file:
        while stream_name in active_streams:
            current_time = datetime.now()

            # Calculate the time until the next top of the hour
            next_top_of_hour = current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            time_until_next_hour = (next_top_of_hour - current_time).total_seconds()

            # Record until the top of the hour
            timestamp = current_time.strftime("%Y-%m-%d_%H-%M-%S")
            ts_file = os.path.join(stream_folder, f"{timestamp}.ts")

            ffmpeg_command = [
                "ffmpeg",
                "-i", f"udp://@{address}:{port}",
                "-c:v", "libx264",
                "-preset", "veryfast",
                "-crf", "28",
                "-b:v", "1M",
                "-c:a", "aac",
                "-b:a", "64k",
                "-vf", "scale=640:-1",
                "-f", "mpegts",
                "-t", str(time_until_next_hour),
                ts_file
            ]

            try:
                subprocess.run(ffmpeg_command, stdout=log_file, stderr=log_file, check=True)
            except subprocess.CalledProcessError as e:
                log_file.write(f"Error recording stream {stream_name}: {e}\n")
                break

@app.on_event("startup")
def startup_event():
    """Resume active streams on application startup."""
    loaded_streams = load_streams_from_config()
    for stream_name, stream_data in loaded_streams.items():
        if stream_data.get("active", False):
            thread = threading.Thread(
                target=record_stream, 
                args=(stream_data["address"], stream_data["port"], stream_name)
            )
            thread.daemon = True
            thread.start()
            active_streams[stream_name] = {**stream_data, "thread": thread}

@app.on_event("shutdown")
def shutdown_event():
    """Save active streams and clean up on shutdown."""
    save_streams_to_config()

@app.post("/add_stream")
async def add_stream(stream: Stream):
    """Add a new stream."""
    if stream.stream_name in active_streams:
        raise HTTPException(status_code=400, detail="Stream already exists")

    thread = threading.Thread(target=record_stream, args=(stream.address, stream.port, stream.stream_name))
    thread.daemon = True
    thread.start()

    active_streams[stream.stream_name] = {
        "address": stream.address,
        "port": stream.port,
        "channel_type": stream.channel_type,
        "active": True,
        "thread": thread
    }
    save_streams_to_config()

    return {"message": "Stream added successfully and recording started"}

@app.post("/remove_stream")
async def remove_stream(stream_name: str):
    """Remove a stream."""
    if stream_name not in active_streams:
        raise HTTPException(status_code=404, detail="Stream not found")

    del active_streams[stream_name]
    save_streams_to_config()

    return {"message": "Stream removed successfully"}

@app.post("/set_active")
async def set_stream_active(stream_name: str, active: bool):
    """Set a stream as active or inactive."""
    if stream_name not in active_streams and not active:
        raise HTTPException(status_code=404, detail="Stream not found")

    if stream_name not in active_streams:
        streams_config = load_streams_from_config()
        if stream_name not in streams_config:
            raise HTTPException(status_code=404, detail="Stream not found in config")

        stream_data = streams_config[stream_name]
        if active:
            thread = threading.Thread(
                target=record_stream, 
                args=(stream_data["address"], stream_data["port"], stream_name)
            )
            thread.daemon = True
            thread.start()
            active_streams[stream_name] = {**stream_data, "thread": thread, "active": True}
    else:
        if not active and stream_name in active_streams:
            del active_streams[stream_name]

    streams_config = load_streams_from_config()
    if stream_name in streams_config:
        streams_config[stream_name]["active"] = active
        save_streams_to_config()

    return {"message": f"Stream '{stream_name}' set to {'active' if active else 'inactive'}"}

@app.get("/list_streams")
async def list_streams():
    """List all active streams, including their channel type."""
    return {
        k: {key: value for key, value in v.items() if key != "thread"}
        for k, v in active_streams.items()
    }

def generate_playlist(stream_name: str) -> list:
    """Generate a playlist of all TS files for the given stream."""
    stream_folder = os.path.join(STREAMS_DIR, stream_name)
    if not os.path.exists(stream_folder):
        raise HTTPException(status_code=404, detail="Stream folder not found")

    # Collect all TS files in chronological order
    files = sorted(
        [f for f in os.listdir(stream_folder) if f.endswith('.ts')],
        key=lambda x: os.path.getmtime(os.path.join(stream_folder, x))
    )

    if not files:
        raise HTTPException(status_code=404, detail="No recordings available for this stream")

    return [os.path.join(stream_folder, f) for f in files]


def stream_concatenated_files(file_list: list) -> StreamingResponse:
    """Stream concatenated files using FFmpeg."""
    # Create a temporary playlist file for FFmpeg
    playlist_path = "temp_playlist.txt"
    with open(playlist_path, "w") as playlist_file:
        for file_path in file_list:
            playlist_file.write(f"file '{file_path}'\n")

    # FFmpeg command to stream concatenated files
    ffmpeg_command = [
        "ffmpeg", "-f", "concat", "-safe", "0", "-i", playlist_path,
        "-c", "copy", "-f", "mp4", "pipe:1"
    ]

    # Stream the output as a response
    process = subprocess.Popen(ffmpeg_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    return StreamingResponse(
        process.stdout,
        media_type="video/mp4",
        headers={"Content-Disposition": "inline; filename=stream.mp4"}
    )
  
@app.get("/play/{stream_name}")
async def play_stream(stream_name: str, range: str = None):
    """Serve a TS recording or allow partial playback for the given stream."""
    stream_folder = os.path.join(STREAMS_DIR, stream_name)

    if not os.path.exists(stream_folder):
        raise HTTPException(status_code=404, detail="Stream not found")

    # Find the latest TS file
    ts_files = sorted(
        [f for f in os.listdir(stream_folder) if f.endswith(".ts")],
        key=lambda f: os.path.getmtime(os.path.join(stream_folder, f)),
        reverse=True
    )
    if not ts_files:
        raise HTTPException(status_code=404, detail="No recordings available")

    latest_ts = os.path.join(stream_folder, ts_files[0])

    # Handle Range requests
    file_size = os.path.getsize(latest_ts)
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": "video/mp2t",  # MPEG-TS content type
        "Content-Length": str(file_size),
    }

    if range:
        # Parse Range header
        byte_range = range.replace("bytes=", "").split("-")
        start = int(byte_range[0]) if byte_range[0] else 0
        end = int(byte_range[1]) if len(byte_range) > 1 and byte_range[1] else file_size - 1
        end = min(end, file_size - 1)  # Ensure end is within bounds

        headers.update({
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Content-Length": str(end - start + 1),
        })

        def stream_partial():
            with open(latest_ts, "rb") as ts_file:
                ts_file.seek(start)
                while start <= end:
                    chunk_size = 1024 * 1024  # 1MB chunks
                    data = ts_file.read(min(chunk_size, end - start + 1))
                    if not data:
                        break
                    yield data
                    start += len(data)

        return StreamingResponse(stream_partial(), status_code=206, headers=headers)

    # Full file response for non-range requests
    def stream_full():
        with open(latest_ts, "rb") as ts_file:
            while chunk := ts_file.read(1024 * 1024):  # Stream in 1MB chunks
                yield chunk

    return StreamingResponse(stream_full(), headers=headers)


def start_cleanup_thread():
    """Start a background thread for cleaning up old files."""
    def cleanup_loop():
        while True:
            cleanup_old_files()
            time.sleep(24 * 3600)  # Run daily

    thread = threading.Thread(target=cleanup_loop)
    thread.daemon = True
    thread.start()


def reload_streams():
    """Reload streams from the configuration file on startup."""
    streams = load_streams_from_config()
    for stream_name, details in streams.items():
        if "channel_type" not in details:
            details["channel_type"] = "unknown"  # Default for older configurations

        thread = threading.Thread(target=record_stream, args=(details["address"], details["port"], stream_name))
        thread.daemon = True
        thread.start()
        active_streams[stream_name] = {**details, "thread": thread}


# Initialize directories and background tasks
ensure_dir(STREAMS_DIR)
reload_streams()
start_cleanup_thread()
