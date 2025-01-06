from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pathlib import Path
from datetime import datetime, timedelta
import os
import uvicorn

app = FastAPI()

STREAMS_DIR = Path("e:/streams")  # Root directory for streams

def get_channels():
    """Retrieve all available channels in the streams directory."""
    if not STREAMS_DIR.exists():
        raise FileNotFoundError("Streams directory does not exist.")
    return [folder.name for folder in STREAMS_DIR.iterdir() if folder.is_dir()]

def parse_recordings(channel):
    """Parse all recordings for a given channel."""
    channel_dir = STREAMS_DIR / channel
    if not channel_dir.exists():
        raise FileNotFoundError(f"Channel '{channel}' not found.")

    recordings = []
    for file in channel_dir.iterdir():
        if file.suffix == ".ts":
            try:
                start_time = datetime.strptime(file.stem, "%Y%m%d_%H%M%S")
                recordings.append((start_time, file))
            except ValueError:
                continue

    return sorted(recordings, key=lambda x: x[0])

def build_timeline(recordings):
    """Build a 24-hour timeline with gaps marked."""
    timeline = []
    current_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = current_time + timedelta(days=1)

    idx = 0
    while current_time < end_time:
        if idx < len(recordings):
            start_time, file = recordings[idx]
            if current_time == start_time:
                timeline.append({"time": current_time, "file": str(file)})
                idx += 1
            else:
                timeline.append({"time": current_time, "file": None})
        else:
            timeline.append({"time": current_time, "file": None})
        current_time += timedelta(seconds=1)

    return timeline

@app.get("/channels")
def list_channels():
    """API endpoint to list all channels."""
    try:
        channels = get_channels()
        return JSONResponse(content={"channels": channels})
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/channels/{channel}/timeline")
def channel_timeline(channel: str):
    """API endpoint to get the timeline of a channel."""
    try:
        recordings = parse_recordings(channel)
        timeline = build_timeline(recordings)
        return JSONResponse(content={"timeline": timeline})
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
