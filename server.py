import os
import re
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from telethon import TelegramClient

API_ID = int(os.environ.get("API_ID", "34394791"))
API_HASH = os.environ.get("API_HASH", "0efc220ee6012f2499359e7a6f6fc42b")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "9139736297:AAGjYwmFGt6seuMnbb6qaBImVPv3VAii-hY")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", "-1003995387951"))

client = TelegramClient('siom_stream_session', API_ID, API_HASH)

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Connecting to Telegram...")
    await client.start(bot_token=BOT_TOKEN)
    print("Telegram Bot connected successfully!")
    yield
    print("Disconnecting Telegram...")
    await client.disconnect()

app = FastAPI(title="SiomJourney TG Streamer", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CHUNK_SIZE = 512 * 1024  # 512 KB per chunk

@app.get("/")
async def index():
    return {
        "status": "online",
        "service": "SiomJourney Telegram Stream Bridge",
        "channel": CHANNEL_ID
    }

@app.api_route("/stream/{message_id}", methods=["GET", "HEAD"])
async def stream_media(message_id: str, request: Request):
    try:
        clean_id = int(str(message_id).split(".")[0])
        msg = await client.get_messages(CHANNEL_ID, ids=clean_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not msg or not msg.media or not getattr(msg, 'file', None):
        raise HTTPException(status_code=404, detail=f"No media found for message id {message_id}")

    total_size = msg.file.size
    file_name = getattr(msg.file, 'name', None) or f"movie_{message_id}.mp4"
    mime_type = getattr(msg.file, 'mime_type', None) or "video/mp4"

    range_header = request.headers.get("range")
    start = 0
    end = total_size - 1

    if range_header:
        match = re.match(r"bytes=(\d+)-(\d*)", range_header)
        if match:
            start = int(match.group(1))
            if match.group(2):
                end = int(match.group(2))

    if start >= total_size:
        return Response(status_code=416, headers={"Content-Range": f"bytes */{total_size}"})

    end = min(end, total_size - 1)
    content_length = (end - start) + 1

    headers = {
        "Content-Range": f"bytes {start}-{end}/{total_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(content_length),
        "Content-Type": mime_type,
        "Content-Disposition": f'inline; filename="{file_name}"',
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Expose-Headers": "Content-Range, Accept-Ranges, Content-Length, Content-Type",
    }

    if request.method == "HEAD":
        return Response(status_code=206 if range_header else 200, headers=headers)

    async def iter_file():
        remaining = content_length
        async for chunk in client.iter_download(msg.media, offset=start, limit=content_length, chunk_size=CHUNK_SIZE):
            if remaining <= 0:
                break
            if len(chunk) > remaining:
                chunk = chunk[:remaining]
            remaining -= len(chunk)
            yield chunk

    return StreamingResponse(
        iter_file(),
        status_code=206 if range_header else 200,
        headers=headers
    )

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=False)
