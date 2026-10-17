"""TeleUpload backend v2.
Files -> zip (no password) -> the recipient's chat with the bot -> 24h download link.
Raw copies go to a private backup channel (optional)."""
import asyncio
import json
import logging
import os
import re
import secrets
import shutil
import sqlite3
import string
import time
import zipfile
from contextlib import asynccontextmanager, closing
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
from telethon import Button, TelegramClient, errors, events

try:
    load_dotenv()  # systemd's EnvironmentFile already provides the variables
except PermissionError:
    pass
log = logging.getLogger("teleupload")
logging.basicConfig(level=logging.INFO)

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
BOT_TOKEN = os.environ["TG_BOT_TOKEN"]
# private backup channel: gets the raw files. Never shown to users.
BACKUP_CHAT_ID = int(os.environ["TG_BACKUP_CHAT_ID"]) if os.environ.get("TG_BACKUP_CHAT_ID") else None
PUBLIC_BASE = os.environ.get("PUBLIC_BASE", "https://www.b2gpt.online/teleupload").rstrip("/")
ORIGINS = os.environ.get(
    "ALLOWED_ORIGINS", "https://www.b2gpt.online,https://b2gpt.online"
).split(",")
DATA = Path(os.environ.get("DATA_DIR", "/var/lib/teleupload"))
TMP = DATA / "tmp"
LINKS = DATA / "links"
DB = DATA / "users.db"

MAX_TOTAL = 1024**3                  # 1 GB per upload (all files together)
MAX_FILES = 20
LINK_TTL = 24 * 3600                 # 24 hours
SUFFIX_LEN = int(os.environ.get("LINK_SUFFIX_LEN", "4"))
MAX_CHUNK = 16 * 1024 * 1024
MAX_ACTIVE = 4
MAX_PER_HOUR = int(os.environ.get("IP_UPLOADS_PER_HOUR", "5"))
MAX_BYTES_PER_DAY = int(os.environ.get("IP_GB_PER_DAY", "3")) * 1024**3
MAX_PER_RECIPIENT_HOUR = 10
MIN_FREE = int(os.environ.get("MIN_FREE_GB", "10")) * 1024**3
SLUG_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,63}")

client = TelegramClient(str(DATA / "teleupload_session"), API_ID, API_HASH)
BOT = {"username": ""}
sessions: dict = {}
uploads: dict = {}   # ip -> [(time, size)]
received: dict = {}  # recipient id -> [time]
misses: dict = {}    # ip -> [time] (unknown download links)
tasks: set = set()
work_sem = asyncio.Semaphore(1)  # one zip+send at a time (protects CPU/disk)


class JobError(Exception):
    pass


# ---------- helpers ----------
def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:40].strip("-")


def rand(n: int) -> str:
    return "".join(secrets.choice(string.ascii_lowercase + string.digits) for _ in range(n))


def human_size(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}".replace(".0 ", " ")
        n /= 1024


def client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for")
    return fwd.split(",")[0].strip() if fwd else (request.client.host if request.client else "?")


def safe_filename(name: str) -> str:
    name = Path(name.replace("\\", "/")).name.strip().strip(".")
    name = re.sub(r"[^\w.\- ()\[\]]+", "_", name)[:120]
    return name or "file"


def allocate_slug(wanted: str) -> str:
    """<name>-<random chars>, always with a random suffix."""
    base = slugify(wanted) or "file"
    for _ in range(100):
        cand = f"{base}-{rand(SUFFIX_LEN)}"
        try:
            (LINKS / cand).mkdir(parents=True)
            return cand
        except FileExistsError:
            continue
    raise RuntimeError("could not allocate slug")


def make_zip(files: list, dst: Path) -> None:
    with zipfile.ZipFile(dst, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=1) as zf:
        for f in files:
            zf.write(f["path"], f["name"])


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


# ---------- users who started the bot ----------
def db(sql: str, args: tuple = ()):
    with closing(sqlite3.connect(DB)) as con:
        rows = con.execute(sql, args).fetchall()
        con.commit()
        return rows


def init_db() -> None:
    db(
        "CREATE TABLE IF NOT EXISTS users ("
        "user_id INTEGER PRIMARY KEY, username TEXT, name TEXT, started REAL)"
    )
    db("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")


def save_user(user_id: int, username: str | None, name: str) -> None:
    db(
        "INSERT INTO users(user_id, username, name, started) VALUES(?,?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, name=excluded.name",
        (user_id, (username or "").lower() or None, name, time.time()),
    )


def find_user(q: str):
    """username (@name) or numeric user id -> (user_id, username, name) or None."""
    q = q.strip()
    if q.startswith("@") or re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{3,31}", q):
        rows = db("SELECT user_id, username, name FROM users WHERE username = ?", (q.lstrip("@").lower(),))
        return rows[0] if rows else None
    digits = re.sub(r"\D", "", q)
    if len(digits) < 5:
        return None
    rows = db("SELECT user_id, username, name FROM users WHERE user_id = ?", (int(digits),))
    return rows[0] if rows else None


@client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
async def on_private_message(event):
    sender = await event.get_sender()
    if not sender or getattr(sender, "bot", False):
        return
    text = (event.raw_text or "").strip().lower()
    name = " ".join(filter(None, [sender.first_name, sender.last_name])) or "User"
    if text.startswith("/stop"):
        db("DELETE FROM users WHERE user_id = ?", (sender.id,))
        await event.respond(
            "Theek hai. Ab koi aapko TeleUpload se file nahi bhej sakega. "
            "Dobara chalu karne ke liye /start likhein.",
            buttons=Button.clear(),
        )
        return
    save_user(sender.id, sender.username, name)
    if text.startswith("/start"):
        await event.respond(
            "Namaste! Aapne TeleUpload bot start kar diya.\n\n"
            "Ab koi bhi www.b2gpt.online/teleupload se aapke username ya user id se "
            "aapko file bhej sakta hai, aur wo yahin aayegi.\n"
            f"Aapka user id: {sender.id}\n\n"
            "Ye sab band karne ke liye /stop likhein.",
        )
    else:
        await event.respond("Commands: /start (chalu karein), /stop (band karein)")


# ---------- job ----------
async def process(sid: str) -> None:
    s = sessions[sid]
    ldir = None
    try:
        async with work_sem:
            slug = allocate_slug(s["linkname"] or s["title"])
            ldir = LINKS / slug
            files = s["files"]
            zip_name = (Path(files[0]["name"]).stem[:80] if len(files) == 1 else slug) + ".zip"
            zpath = ldir / zip_name

            s["state"] = "zipping"
            await asyncio.to_thread(make_zip, files, zpath)

            link = f"{PUBLIC_BASE}/{slug}"
            title = s["title"] or (files[0]["name"] if len(files) == 1 else f"{len(files)} files")

            if s["recipient_id"]:
                s["state"], s["progress"] = "sending", 0
                caption = (
                    f"📦 {human_size(s['total'])} | {len(files)} file\n"
                    f"📝 {title}\n\n🔗 {link}\n⏳ 24 ghante valid"
                )

                def cb(sent, total):
                    s["progress"] = int(sent * 100 / total) if total else 0

                try:
                    await client.send_file(
                        s["recipient_id"], str(zpath), caption=caption,
                        force_document=True, progress_callback=cb,
                    )
                except (errors.UserIsBlockedError, errors.InputUserDeactivatedError,
                        errors.PeerIdInvalidError, ValueError):
                    raise JobError(
                        "Us person tak file nahi pahunch saki. Shayad unhone bot block ya /stop kar diya hai. "
                        "Unse bot dobara start karne bolein."
                    )

            expires = time.time() + LINK_TTL
            (ldir / "meta.json").write_text(
                json.dumps({"file": zpath.name, "name": zpath.name, "expires": expires})
            )
            s.update(state="done", link=link, expires=expires, delivered=bool(s["recipient_id"]))

            # secret backup of the raw files (failure here never affects the user)
            if BACKUP_CHAT_ID:
                for f in files:
                    try:
                        await client.send_file(
                            BACKUP_CHAT_ID, f["path"], force_document=True,
                            caption=f"📦 {human_size(f['size'])}\n📁 {f['name']}\n📝 {title}",
                        )
                    except Exception:
                        log.exception("backup channel upload failed")
    except JobError as e:
        s.update(state="error", error=str(e))
        if ldir:
            shutil.rmtree(ldir, ignore_errors=True)
    except Exception:
        log.exception("job failed")
        s.update(state="error", error="File bhejne mein dikkat aayi. Dobara try karein.")
        if ldir:
            shutil.rmtree(ldir, ignore_errors=True)
    finally:
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
        for book, ttl in ((uploads, 86400), (received, 3600), (misses, 600)):
            for key in list(book):
                book[key] = [e for e in book[key] if now - (e[0] if isinstance(e, tuple) else e) < ttl]
                if not book[key]:
                    del book[key]


@asynccontextmanager
async def lifespan(app: FastAPI):
    DATA.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(TMP, ignore_errors=True)  # leftovers from a crash
    TMP.mkdir(parents=True, exist_ok=True)
    LINKS.mkdir(parents=True, exist_ok=True)
    for d in LINKS.iterdir():  # unfinished jobs lost on restart
        if not (d / "meta.json").exists():
            shutil.rmtree(d, ignore_errors=True)
    init_db()
    await client.start(bot_token=BOT_TOKEN)
    BOT["username"] = (await client.get_me()).username or ""
    log.info("Bot ready: @%s", BOT["username"])
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


class FileInfo(BaseModel):
    name: str
    size: int


class InitBody(BaseModel):
    files: list[FileInfo]
    recipient: str = ""
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
    if not 1 <= len(body.files) <= MAX_FILES:
        raise HTTPException(400, f"Ek baar mein 1 se {MAX_FILES} files bhej sakte hain.")
    total = sum(f.size for f in body.files)
    if any(f.size <= 0 for f in body.files) or total > MAX_TOTAL:
        raise HTTPException(400, "Sabhi files ka kul size 1 GB se chhota hona chahiye (aur koi file khaali nahi).")
    recipient = body.recipient.strip()
    if len(recipient) > 64:
        raise HTTPException(400, "Username ya user id bahut lamba hai.")

    uid = None  # empty recipient = only make the download link
    if recipient:
        user = find_user(recipient)
        if not user:
            bot = BOT["username"]
            raise HTTPException(404, {
                "message": (
                    "Ye person mila nahi. Pehle unse "
                    + (f"@{bot} " if bot else "TeleUpload bot ")
                    + "ko Telegram par start karne bolein, phir yahan dobara try karein. "
                    "(Sirf link chahiye to ye box khaali chhod dein.)"
                ),
                "bot_link": f"https://t.me/{bot}" if bot else "",
            })
        uid = user[0]

    hist = [(t, n) for t, n in uploads.get(ip, []) if now - t < 86400]
    uploads[ip] = hist
    if any(v["ip"] == ip and v["state"] not in ("done", "error") for v in sessions.values()):
        raise HTTPException(429, "Pehli upload poori hone dein, phir nayi shuru karein.")
    if sum(1 for t, _ in hist if now - t < 3600) >= MAX_PER_HOUR:
        raise HTTPException(429, "Ek ghante mein bahut uploads ho gaye. Thodi der baad try karein.")
    if sum(n for _, n in hist) + total > MAX_BYTES_PER_DAY:
        raise HTTPException(429, "Aaj ki upload limit poori ho gayi. Kal try karein.")
    if uid:
        got = [t for t in received.get(uid, []) if now - t < 3600]
        received[uid] = got
        if len(got) >= MAX_PER_RECIPIENT_HOUR:
            raise HTTPException(429, "Is person ko abhi bahut files bheji ja chuki hain. Thodi der baad try karein.")
    if sum(v["state"] not in ("done", "error") for v in sessions.values()) >= MAX_ACTIVE:
        raise HTTPException(429, "Server abhi busy hai. Thodi der baad try karein.")
    if shutil.disk_usage(DATA).free < total * 2.2 + MIN_FREE:
        raise HTTPException(507, "Server par jagah kam hai. Baad mein try karein.")

    sid = secrets.token_urlsafe(24)
    d = TMP / sid
    used, files = set(), []
    for i, f in enumerate(body.files):
        name = safe_filename(f.name)
        base, ext = os.path.splitext(name)
        n, k = name, 1
        while n.lower() in used:
            n, k = f"{base} ({k}){ext}", k + 1
        used.add(n.lower())
        fd = d / str(i)
        fd.mkdir(parents=True)
        p = fd / n
        p.touch()
        files.append({"name": n, "size": f.size, "path": str(p), "received": 0})

    sessions[sid] = {
        "files": files, "total": total, "recipient_id": uid,
        "title": body.title.strip()[:700], "linkname": body.linkname.strip(),
        "ip": ip, "state": "uploading", "progress": 0, "created": now,
    }
    uploads[ip].append((now, total))
    if uid:
        received[uid].append(now)
    return {"ok": True, "upload_id": sid}


@app.post("/upload/{sid}/chunk")
async def upload_chunk(sid: str, file: int, offset: int, request: Request):
    s = get_session(sid, "uploading")
    if not 0 <= file < len(s["files"]):
        raise HTTPException(400, "File number galat hai.")
    f = s["files"][file]
    if int(request.headers.get("content-length", 0)) > MAX_CHUNK:
        raise HTTPException(413, "Chunk bahut bada hai.")
    data = await request.body()
    if offset < 0 or offset > f["received"] or offset + len(data) > f["size"]:
        raise HTTPException(409, "Chunk offset galat hai.")
    await asyncio.to_thread(write_at, Path(f["path"]), offset, data)
    f["received"] = offset + len(data)
    return {"ok": True, "received": f["received"]}


@app.post("/upload/{sid}/finish")
async def upload_finish(sid: str):
    s = get_session(sid, "uploading")
    if any(f["received"] != f["size"] for f in s["files"]):
        raise HTTPException(400, "Files poori upload nahi hui.")
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
        out.update(link=s["link"], delivered=s["delivered"], expires_in=max(0, int(s["expires"] - time.time())))
    if s["state"] == "error":
        out["error"] = s["error"]
    return out


@app.get("/d/{slug}")
async def download(slug: str, request: Request):
    ip, now = client_ip(request), time.time()
    recent = [t for t in misses.get(ip, []) if now - t < 600]
    misses[ip] = recent
    if len(recent) >= 30:
        return message_page("Bahut zyada galat links try hue. Thodi der baad try karein.", 429)
    meta_path = LINKS / slug / "meta.json"
    if not SLUG_RE.fullmatch(slug) or not meta_path.exists():
        misses[ip] = recent + [now]
        return message_page("Ye link nahi mila ya expire ho chuka hai.", 404)
    meta = json.loads(meta_path.read_text())
    if meta["expires"] < now:
        return message_page("Ye link expire ho chuka hai (24 ghante poore ho gaye).", 410)
    return FileResponse(LINKS / slug / meta["file"], filename=meta["name"], media_type="application/zip")
