import os
import re
import json
import asyncio
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from telethon import TelegramClient

API_ID = int(os.environ.get("API_ID", "34394791"))
API_HASH = os.environ.get("API_HASH", "0efc220ee6012f2499359e7a6f6fc42b")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "9139736297:AAGjYwmFGt6seuMnbb6qaBImVPv3VAii-hY")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", "-1003995387951"))
DB_MESSAGE_ID = int(os.environ.get("DB_MESSAGE_ID", "21"))

client = TelegramClient('siom_stream_session', API_ID, API_HASH)

db_state = {
    "visits": 1285,
    "views": {}
}
is_db_dirty = False

# ==========================================
# IN-MEMORY MEDIA & METADATA CACHE (RAM)
# ==========================================
# Caches Telegram media objects, file sizes, and MIME types so seeking
# forward/backward takes 0 ms without querying the Telegram API on each HTTP Range request.
MEDIA_CACHE = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_state, is_db_dirty
    print("Connecting to Telegram...")
    await client.start(bot_token=BOT_TOKEN)
    print("Telegram Bot connected successfully!")
    
    # Load database from Telegram Message #21
    try:
        msg = await client.get_messages(CHANNEL_ID, ids=DB_MESSAGE_ID)
        if msg and msg.text and ("SIOM_DB_STORE:" in msg.text or "SIOM_DB_VIEW_STORE:" in msg.text):
            raw_text = msg.text
            if "SIOM_DB_STORE:" in raw_text:
                raw_json = raw_text.split("SIOM_DB_STORE:", 1)[1].strip()
            else:
                raw_json = raw_text.split("SIOM_DB_VIEW_STORE:", 1)[1].strip()
            loaded = json.loads(raw_json)
            if isinstance(loaded, dict):
                if "visits" in loaded:
                    db_state["visits"] = loaded["visits"]
                if "views" in loaded:
                    db_state["views"] = loaded["views"]
                print(f"Loaded DB state: {len(db_state.get('views', {}))} movies, {db_state.get('visits')} visits.")
    except Exception as e:
        print(f"Error loading DB from TG msg #{DB_MESSAGE_ID}: {e}")

    # Background debounced sync task
    async def db_sync_loop():
        global is_db_dirty
        while True:
            await asyncio.sleep(5)
            if is_db_dirty:
                try:
                    payload = f"SIOM_DB_STORE: {json.dumps(db_state, ensure_ascii=False)}"
                    await client.edit_message(CHANNEL_ID, DB_MESSAGE_ID, payload)
                    is_db_dirty = False
                    with open("views_backup.json", "w", encoding="utf-8") as f:
                        json.dump(db_state, f, ensure_ascii=False)
                except Exception as sync_err:
                    print(f"Error syncing DB to Telegram: {sync_err}")

    sync_task = asyncio.create_task(db_sync_loop())
    yield
    sync_task.cancel()
    if is_db_dirty:
        try:
            payload = f"SIOM_DB_STORE: {json.dumps(db_state, ensure_ascii=False)}"
            await client.edit_message(CHANNEL_ID, DB_MESSAGE_ID, payload)
        except Exception:
            pass
    print("Disconnecting Telegram...")
    await client.disconnect()

app = FastAPI(title="SiomJourney TG Streamer & Global Views API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 1 MB chunk size for high-throughput streaming (reduces MTProto round-trip overhead by 50%)
CHUNK_SIZE = 1024 * 1024

@app.get("/")
async def index():
    return {
        "status": "online",
        "service": "SiomJourney Telegram Stream Bridge & Views DB",
        "channel": CHANNEL_ID,
        "visits": db_state.get("visits", 1285),
        "tracked_movies": len(db_state.get("views", {})),
        "cached_media": len(MEDIA_CACHE)
    }

# ==========================================
# GLOBAL VIEWS & VISITS ENDPOINTS
# ==========================================
@app.get("/api/views")
async def get_all_views():
    return {
        "success": True,
        "visits": db_state.get("visits", 1285),
        "views": db_state.get("views", {})
    }

@app.api_route("/api/views/{movie_id}/increment", methods=["GET", "POST"])
async def increment_view(movie_id: str):
    global is_db_dirty
    views = db_state.setdefault("views", {})
    current = int(views.get(movie_id, 0))
    views[movie_id] = current + 1
    is_db_dirty = True
    return {
        "success": True,
        "id": movie_id,
        "views": views[movie_id]
    }

@app.api_route("/api/visits/increment", methods=["GET", "POST"])
async def increment_visits():
    global is_db_dirty
    current = int(db_state.get("visits", 1285))
    db_state["visits"] = current + 1
    is_db_dirty = True
    return {
        "success": True,
        "visits": db_state["visits"]
    }

# ==========================================
# HIGH-SPEED STREAMING ENDPOINT WITH RAM CACHE
# ==========================================
@app.api_route("/stream/{message_id}", methods=["GET", "HEAD"])
async def stream_media(message_id: str, request: Request):
    try:
        clean_id = int(str(message_id).split(".")[0])
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid message id")

    # 1. Check in-memory RAM cache first
    cached = MEDIA_CACHE.get(clean_id)
    if cached is None:
        try:
            msg = await client.get_messages(CHANNEL_ID, ids=clean_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        if not msg or not msg.media or not getattr(msg, 'file', None):
            raise HTTPException(status_code=404, detail=f"No media found for message id {message_id}")

        total_size = msg.file.size
        file_name = getattr(msg.file, 'name', None) or f"movie_{clean_id}.mp4"
        mime_type = getattr(msg.file, 'mime_type', None) or "video/mp4"

        cached = {
            "media": msg.media,
            "size": total_size,
            "name": file_name,
            "mime": mime_type,
            "created_at": time.time()
        }
        MEDIA_CACHE[clean_id] = cached
    else:
        total_size = cached["size"]
        file_name = cached["name"]
        mime_type = cached["mime"]

    media_obj = cached["media"]

    # 2. Parse HTTP Range header
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
        "Cache-Control": "public, max-age=86400, stale-while-revalidate=604800",
    }

    if request.method == "HEAD":
        return Response(status_code=206 if range_header else 200, headers=headers)

    # 3. High-throughput chunk streaming directly from cached media
    async def iter_file():
        remaining = content_length
        async for chunk in client.iter_download(media_obj, offset=start, limit=content_length, chunk_size=CHUNK_SIZE):
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
