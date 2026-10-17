"""TeleUpload backend: chunked upload -> AES zip -> Telegram (MTProto) -> 15 min download link."""
import asyncio
import json
import logging
import os
import re
import secrets
import shutil
import string
import time
from contextlib import asynccontextmanager
from pathlib import Path

import pyzipper
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
from telethon import TelegramClient

try:
    load_dotenv()  # systemd's EnvironmentFile already provides the variables
except PermissionError:
    pass
log = logging.getLogger("teleupload")
logging.basicConfig(level=logging.INFO)

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
BOT_TOKEN = os.environ["TG_BOT_TOKEN"]
CHAT_ID = int(os.environ["TG_CHAT_ID"])
# private backup channel: gets the raw file (no zip, no password). Never shown to users.
BACKUP_CHAT_ID = int(os.environ["TG_BACKUP_CHAT_ID"]) if os.environ.get("TG_BACKUP_CHAT_ID") else None
PUBLIC_BASE = os.environ.get("PUBLIC_BASE", "https://www.b2gpt.online/teleupload").rstrip("/")
ORIGINS = os.environ.get(
    "ALLOWED_ORIGINS", "https://www.b2gpt.online,https://b2gpt.online"
).split(",")
DATA = Path(os.environ.get("DATA_DIR", "/var/lib/teleupload"))
TMP = DATA / "tmp"
LINKS = DATA / "links"

MAX_SIZE = 1024**3          # 1 GB
LINK_TTL = 15 * 60          # 15 minutes
MAX_CHUNK = 16 * 1024 * 1024
MAX_ACTIVE = 4
MAX_PER_HOUR = int(os.environ.get("IP_UPLOADS_PER_HOUR", "5"))
MAX_BYTES_PER_DAY = int(os.environ.get("IP_GB_PER_DAY", "3")) * 1024**3
SLUG_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,63}")

client = TelegramClient(str(DATA / "teleupload_session"), API_ID, API_HASH)
sessions: dict = {}
uploads: dict = {}  # ip -> [(time, size)]
tasks: set = set()
work_sem = asyncio.Semaphore(1)  # one zip+send at a time (protects CPU/disk)


# ---------- helpers ----------
def slugify(text: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return s[:40].strip("-")


def rand(n: int) -> str:
    return "".join(secrets.choice(string.ascii_lowercase + string.digits) for _ in range(n))


def human_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}".replace(".0 ", " ")
        n /= 1024


def client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for")
    return fwd.split(",")[0].strip() if fwd else (request.client.host if request.client else "?")


def allocate_slug(wanted: str) -> str:
    base = slugify(wanted) or rand(8)
    cand = base
    for _ in range(50):
        try:
            (LINKS / cand).mkdir(parents=True)
            return cand
        except FileExistsError:
            cand = f"{base}-{rand(4)}"  # same name already active -> add random suffix
    raise RuntimeError("could not allocate slug")


def make_zip(src: Path, dst: Path, arcname: str, password: str) -> None:
    with pyzipper.AESZipFile(
        dst, "w", compression=pyzipper.ZIP_DEFLATED, compresslevel=1,
        encryption=pyzipper.WZ_AES,
    ) as zf:
        zf.setpassword(password.encode())
        zf.setencryption(pyzipper.WZ_AES, nbits=256)
        zf.write(src, arcname)


def write_at(path: Path, offset: int, data: bytes) -> None:
    with open(path, "r+b") as f:
        f.seek(offset)
        f.truncate(offset)
        f.write(data)


def message_page(text: str, code: int) -> HTMLResponse:
    html = (
        "<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'>"
        "<title>TeleUpload</title><body style=\"margin:0;min-height:100vh;display:grid;place-items:center;"
        "background:#000031;color:#fff;font-family:system-ui,sans-serif;text-align:center;padding:2rem\">"
        f"<div><h2 style='color:#ffe600'>TeleUpload</h2><p>{text}</p></div></body>"
    )
    return HTMLResponse(html, status_code=code)


# ---------- job ----------
async def process(sid: str) -> None:
    s = sessions[sid]
    ldir = None
    try:
        async with work_sem:
            slug = allocate_slug(s["linkname"] or s["title"])
            ldir = LINKS / slug
            safe = re.sub(r"[^\w.\- ]+", "_", s["filename"])[:100] or "file"
            zpath = ldir / (safe + ".zip")

            s["state"] = "zipping"
            await asyncio.to_thread(make_zip, s["raw"], zpath, s["filename"], s["password"])
            s["password"] = ""

            s["state"], s["progress"] = "sending", 0
            link = f"{PUBLIC_BASE}/{slug}"
            title = s["title"] or s["filename"]
            caption = f"📦 {human_size(s['size'])}\n📝 {title}\n\n🔗 {link}\n⏳ 15 min valid"

            def cb(sent, total):
                s["progress"] = int(sent * 100 / total) if total else 0

            await client.send_file(
                CHAT_ID, str(zpath), caption=caption, force_document=True, progress_callback=cb
            )

            expires = time.time() + LINK_TTL  # link goes live when the file is in the group
            (ldir / "meta.json").write_text(
                json.dumps({"file": zpath.name, "name": zpath.name, "expires": expires})
            )
            s.update(state="done", link=link, expires=expires)

            # secret backup of the raw file (failure here never affects the user)
            if BACKUP_CHAT_ID:
                try:
                    raw_named = Path(s["raw"]).with_name(safe)
                    Path(s["raw"]).rename(raw_named)
                    await client.send_file(
                        BACKUP_CHAT_ID, str(raw_named), force_document=True,
                        caption=f"📦 {human_size(s['size'])}\n📁 {s['filename']}\n📝 {title}",
                    )
                except Exception:
                    log.exception("backup channel upload failed")
    except Exception:
        log.exception("job failed")
        s.update(state="error", error="Telegram par bhejne mein dikkat aayi. Dobara try karein.")
        if ldir:
            shutil.rmtree(ldir, ignore_errors=True)
    finally:
        s["password"] = ""
        shutil.rmtree(TMP / sid, ignore_errors=True)


async def cleanup_loop() -> None:
    while True:
        await asyncio.sleep(30)
        now = time.time()
        for d in LINKS.iterdir():
            meta = d / "meta.json"
            try:
                if meta.exists():
                    if json.loads(meta.read_text())["expires"] < now:
                        shutil.rmtree(d, ignore_errors=True)
                elif now - d.stat().st_mtime > 3 * 3600:
                    shutil.rmtree(d, ignore_errors=True)
            except Exception:
                log.exception("cleanup error")
        for sid in [k for k, v in sessions.items() if now - v["created"] > 3 * 3600]:
            shutil.rmtree(TMP / sid, ignore_errors=True)
            sessions.pop(sid, None)


@asynccontextmanager
async def lifespan(app: FastAPI):
    DATA.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(TMP, ignore_errors=True)  # leftovers from a crash
    TMP.mkdir(parents=True, exist_ok=True)
    LINKS.mkdir(parents=True, exist_ok=True)
    for d in LINKS.iterdir():  # unfinished jobs lost on restart
        if not (d / "meta.json").exists():
            shutil.rmtree(d, ignore_errors=True)
    await client.start(bot_token=BOT_TOKEN)
    if BACKUP_CHAT_ID:
        try:
            await client.get_input_entity(BACKUP_CHAT_ID)
        except Exception:
            log.warning("Backup channel not reachable: add the bot as admin there and check TG_BACKUP_CHAT_ID")
    task = asyncio.create_task(cleanup_loop())
    yield
    task.cancel()
    await client.disconnect()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=ORIGINS, allow_methods=["GET", "POST"], allow_headers=["*"]
)


class InitBody(BaseModel):
    filename: str
    size: int
    password: str
    title: str = ""
    linkname: str = ""


def get_session(sid: str, state: str | None = None) -> dict:
    s = sessions.get(sid)
    if not s:
        raise HTTPException(404, "Upload session nahi mila. Dobara shuru karein.")
    if state and s["state"] != state:
        raise HTTPException(409, "Galat step.")
    return s


# ---------- routes ----------
@app.post("/upload/init")
async def upload_init(body: InitBody, request: Request):
    ip, now = client_ip(request), time.time()
    if not 4 <= len(body.password) <= 128:
        raise HTTPException(400, "Password 4 se 128 characters ka rakhein.")
    if not 0 < body.size <= MAX_SIZE:
        raise HTTPException(400, "File 1 GB se chhoti (aur khaali nahi) honi chahiye.")
    hist = [(t, n) for t, n in uploads.get(ip, []) if now - t < 86400]
    uploads[ip] = hist
    if any(v["ip"] == ip and v["state"] not in ("done", "error") for v in sessions.values()):
        raise HTTPException(429, "Pehli upload poori hone dein, phir nayi shuru karein.")
    if sum(1 for t, _ in hist if now - t < 3600) >= MAX_PER_HOUR:
        raise HTTPException(429, "Ek ghante mein bahut uploads ho gaye. Thodi der baad try karein.")
    if sum(n for _, n in hist) + body.size > MAX_BYTES_PER_DAY:
        raise HTTPException(429, "Aaj ki upload limit poori ho gayi. Kal try karein.")
    if sum(v["state"] != "done" and v["state"] != "error" for v in sessions.values()) >= MAX_ACTIVE:
        raise HTTPException(429, "Server abhi busy hai. Thodi der baad try karein.")
    if shutil.disk_usage(DATA).free < body.size * 2.2 + 500 * 1024**2:
        raise HTTPException(507, "Server par jagah kam hai. Baad mein try karein.")

    sid = secrets.token_urlsafe(24)
    d = TMP / sid
    d.mkdir(parents=True)
    raw = d / "raw.bin"
    raw.touch()
    sessions[sid] = {
        "raw": str(raw), "filename": Path(body.filename).name.strip() or "file",
        "size": body.size, "password": body.password, "title": body.title.strip()[:700],
        "linkname": body.linkname.strip(), "ip": ip, "state": "uploading", "received": 0,
        "progress": 0, "created": now,
    }
    uploads[ip].append((now, body.size))
    return {"ok": True, "upload_id": sid}


@app.post("/upload/{sid}/chunk")
async def upload_chunk(sid: str, offset: int, request: Request):
    s = get_session(sid, "uploading")
    if int(request.headers.get("content-length", 0)) > MAX_CHUNK:
        raise HTTPException(413, "Chunk bahut bada hai.")
    data = await request.body()
    if offset < 0 or offset > s["received"] or offset + len(data) > s["size"]:
        raise HTTPException(409, "Chunk offset galat hai.")
    await asyncio.to_thread(write_at, Path(s["raw"]), offset, data)
    s["received"] = offset + len(data)
    return {"ok": True, "received": s["received"]}


@app.post("/upload/{sid}/finish")
async def upload_finish(sid: str):
    s = get_session(sid, "uploading")
    if s["received"] != s["size"]:
        raise HTTPException(400, "File poori upload nahi hui.")
    s["state"] = "queued"
    t = asyncio.create_task(process(sid))
    tasks.add(t)
    t.add_done_callback(tasks.discard)
    return {"ok": True}


@app.get("/upload/{sid}/status")
async def upload_status(sid: str):
    s = get_session(sid)
    out = {"state": s["state"], "progress": s["progress"]}
    if s["state"] == "done":
        out.update(link=s["link"], expires_in=max(0, int(s["expires"] - time.time())))
    if s["state"] == "error":
        out["error"] = s["error"]
    return out


@app.get("/d/{slug}")
async def download(slug: str):
    if not SLUG_RE.fullmatch(slug):
        return message_page("Link galat hai.", 404)
    meta_path = LINKS / slug / "meta.json"
    if not meta_path.exists():
        return message_page("Ye link nahi mila ya expire ho chuka hai.", 404)
    meta = json.loads(meta_path.read_text())
    if meta["expires"] < time.time():
        return message_page("Ye link expire ho chuka hai (15 minute poore ho gaye).", 410)
    return FileResponse(LINKS / slug / meta["file"], filename=meta["name"], media_type="application/zip")
