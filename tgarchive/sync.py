"""
SPECTRA-002 — Telegram Channel Archiver (v2.2)
================================================
*Multi-account* · *Proxy-rotating* · *Sidecar-metadata* · *Curses TUI*

Overview
--------
A forensic-grade Telegram archiver designed for SWORD-EPI. New in **v2.2**:

1. **Sidecar metadata** — every downloaded file now gains a `.json` twin
   containing the parent message (inc. probable passwords, captions, user,
   date, etc.).  Naming: `orig.ext` → `orig.ext.json`.
2. **Full ncurses TUI** — interactive configuration via *npyscreen*: pick
   account, channel, proxy profile, and options before launch.
3. **Argparse flags** — `--no-tui` for headless automation.

MIT-style licence.  © 2025 John (SWORD-EPI) – codename *SPECTRA-002*.
"""
from __future__ import annotations

# ── Standard Library ──────────────────────────────────────────────────────
import argparse
import asyncio
import contextlib
import itertools
import json
import logging
import os
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from types import TracebackType
from typing import Any, Dict, List, Optional, Tuple

# ── Third-party ───────────────────────────────────────────────────────────
import npyscreen  # type: ignore
import socks      # PySocks for proxy support
from PIL import Image  # type: ignore
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from telethon import TelegramClient, errors  # type: ignore
from telethon.tl.custom.message import Message as TLMessage  # type: ignore
from tqdm.asyncio import tqdm_asyncio  # type: ignore

# ── Globals ───────────────────────────────────────────────────────────────
APP_NAME = "spectra_002_archiver"
__version__ = "2.2.0"
TZ = timezone.utc
console = Console()

# ── Logging Setup ─────────────────────────────────────────────────────────
LOGS_DIR = Path.cwd() / "logs"
LOGS_DIR.mkdir(exist_ok=True)
log_file = LOGS_DIR / f"{APP_NAME}_{datetime.now(tz=TZ).strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(APP_NAME)

# ── Default configuration ────────────────────────────────────────────────
DEFAULT_CFG: Dict[str, Any] = {
    # legacy single-account
    "api_id": int(os.environ.get("TG_API_ID", 0)),
    "api_hash": os.environ.get("TG_API_HASH", ""),

    # multi-account list
    "accounts": [
        {
            "api_id": 123456,  # ← edit me
            "api_hash": "0123456789abcdef0123456789abcdef",
            "session_name": "spectra_1",
        },
    ],

    # rotating proxy
    "proxy": {
        "host": "rotating.proxyempire.io",
        "user": "PROXY_USER",
        "password": "PROXY_PASS",
        "ports": list(range(9000, 9010)),
    },

    # runtime options
    "entity": "",  # channel/group @link or id
    "db_path": "spectra.sqlite3",
    "media_dir": "media",
    "download_media": True,
    "download_avatars": True,
    "media_mime_whitelist": [],
    "batch": 500,
    "sleep_between_batches": 1.0,
    "use_takeout": False,
    "avatar_size": 128,
    "collect_usernames": True,
    "sidecar_metadata": True,
}

# ── Config loader ─────────────────────────────────────────────────────────
@dataclass
class Config:
    path: Path = Path("spectra_config.json")
    data: Dict[str, Any] = field(default_factory=lambda: DEFAULT_CFG.copy())

    def __post_init__(self):
        if self.path.exists():
            try:
                self.data.update(json.loads(self.path.read_text()))
            except json.JSONDecodeError as exc:
                logger.warning("Bad JSON in config – using defaults (%s)", exc)
        else:
            self.save()
            console.print(
                "[yellow]Config not found; default created at"
                f" {self.path}.  Edit credentials then rerun.[/yellow]"
            )
            sys.exit(1)

        # back-compat
        if not self.data.get("accounts"):
            self.data["accounts"] = [
                {
                    "api_id": self.data["api_id"],
                    "api_hash": self.data["api_hash"],
                    "session_name": "spectra_legacy",
                }
            ]

    # helpers
    def save(self):
        self.path.write_text(json.dumps(self.data, indent=2))

    def __getitem__(self, item):
        return self.data[item]

    @property
    def accounts(self):
        return self.data["accounts"]

    @property
    def proxy_conf(self):
        return self.data.get("proxy", {})

# ── Proxy cycler ──────────────────────────────────────────────────────────
class ProxyCycler:
    def __init__(self, proxy_cfg: Dict[str, Any]):
        self.host = proxy_cfg.get("host")
        self.user = proxy_cfg.get("user")
        self.password = proxy_cfg.get("password")
        self.ports = proxy_cfg.get("ports", [])
        if not all([self.host, self.user, self.password, self.ports]):
            self.proxies = [None]
        else:
            self.proxies = [
                (socks.SOCKS5, self.host, port, self.user, self.password) for port in self.ports
            ]
        self._it = itertools.cycle(self.proxies)

    def next(self):
        return next(self._it)

# ── DB handler ────────────────────────────────────────────────────────────
class DBHandler(contextlib.AbstractContextManager):
    def __init__(self, db_file: Path):
        self.db_file = db_file
        self.conn: sqlite3.Connection | None = None
        self.cur: sqlite3.Cursor | None = None

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_file)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self.cur = self.conn.cursor()
        self._schema()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.conn.rollback() if exc else self.conn.commit()
        self.conn.close()
        return False

    def _schema(self):
        self.cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                avatar_path TEXT
            );
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                date TEXT,
                edit_date TEXT,
                content TEXT,
                reply_to INTEGER
            );
            CREATE TABLE IF NOT EXISTS media (
                id INTEGER PRIMARY KEY,
                message_id INTEGER REFERENCES messages(id),
                mime_type TEXT,
                file_path TEXT
            );
            CREATE TABLE IF NOT EXISTS username_mentions (
                id INTEGER PRIMARY KEY,
                username TEXT,
                message_id INTEGER REFERENCES messages(id),
                date TEXT,
                source_type TEXT
            );
            """
        )

    def last_message_id(self):
        row = self.cur.execute("SELECT MAX(id) FROM messages").fetchone()
        return row[0] if row and row[0] else None

    # insert helpers
    def add_user(self, u):
        self.cur.execute(
            "INSERT OR REPLACE INTO users(id, username, first_name, last_name, avatar_path) VALUES (?, ?, ?, ?, ?)",
            (u.id, getattr(u, "username", None), getattr(u, "first_name", None), getattr(u, "last_name", None), None),
        )

    def add_message(self, d):
        self.cur.execute(
            "INSERT OR REPLACE INTO messages(id, user_id, date, edit_date, content, reply_to) VALUES (:id, :user_id, :date, :edit_date, :content, :reply_to)",
            d,
        )

    def add_media(self, d):
        self.cur.execute(
            "INSERT OR REPLACE INTO media(id, message_id, mime_type, file_path) VALUES (:id, :message_id, :mime_type, :file_path)",
            d,
        )

    def add_username(self, username, msg_id, date, source="mention"):
        self.cur.execute(
            "INSERT INTO username_mentions(username, message_id, date, source_type) VALUES (?, ?, ?, ?)",
            (username, msg_id, date, source),
        )

# ── Helper regexes ───────────────────────────────────────────────────────
USERNAME_RE = re.compile(r"@([A-Za-z0-9_]{5,32})")

def extract_usernames(text: str | None):
    return USERNAME_RE.findall(text or "")

# ── Sidecar writer ───────────────────────────────────────────────────────
async def write_sidecar(msg: TLMessage, file_path: Path):
    meta = {
        "msg_id": msg.id,
        "date": msg.date.astimezone(TZ).isoformat(),
        "sender_id": msg.sender_id,
        "sender_username": getattr(msg.sender, "username", None) if msg.sender else None,
        "reply_to": msg.reply_to_msg_id,
        "text": msg.message,
        "mime_type": msg.file.mime_type if msg.file else None,
    }
    file_path.with_suffix(file_path.suffix + ".json").write_text(json.dumps(meta, indent=2))

# ── Media downloader ─────────────────────────────────────────────────────
async def safe_download_media(msg: TLMessage, dest: Path, mime_whitelist, sidecar=True):
    if not msg.media:
        return None
    if mime_whitelist and msg.file.mime_type not in mime_whitelist:
        return None
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        path = await msg.download_media(file=dest)
        if path and sidecar:
            await write_sidecar(msg, Path(path))
        return path
    except errors.FloodWaitError as e:
        await asyncio.sleep(e.seconds + 5)
        return await safe_download_media(msg, dest, mime_whitelist, sidecar)

# ── Core archive pipeline ────────────────────────────────────────────────
async def archive_channel(cfg: Config, account: Dict[str, Any], proxy_tuple):  # noqa: C901
    progress = Progress(
        TextColumn("{task.description}"),
        BarColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    )
    media_dir = Path(cfg["media_dir"])
    media_dir.mkdir(exist_ok=True)

    async with TelegramClient(account["session_name"], account["api_id"], account["api_hash"], proxy=proxy_tuple) as client:  # type: ignore
        logger.info("Connected as %s via proxy=%s", await client.get_me(), proxy_tuple or "none")
        entity = await client.get_entity(cfg["entity"])

        with DBHandler(Path(cfg["db_path"])) as db:
            last_id = db.last_message_id()
            total = await client.get_messages(entity, limit=0)
            task = progress.add_task("[green]Archiving", total=len(total))

            with progress:
                async for msg in client.iter_messages(entity, offset_id=last_id or 0, reverse=True, wait_time=cfg["sleep_between_batches"]):
                    d = {
                        "id": msg.id,
                        "user_id": msg.sender_id,
                        "date": msg.date.astimezone(TZ).isoformat(),
                        "edit_date": msg.edit_date.astimezone(TZ).isoformat() if msg.edit_date else None,
                        "content": msg.message,
                        "reply_to": msg.reply_to_msg_id,
                    }
                    db.add_message(d)

                    if msg.sender:
                        db.add_user(msg.sender)

                    if cfg["collect_usernames"]:
                        for uname in extract_usernames(msg.message):
                            db.add_username(uname, msg.id, d["date"])

                    if cfg["download_media"] and msg.media:
                        dest = media_dir / f"{msg.id}_{msg.file.id}"
                        downloaded = await safe_download_media(msg, dest, cfg["media_mime_whitelist"], cfg["sidecar_metadata"])
                        if downloaded:
                            db.add_media({
                                "id": msg.file.id,
                                "message_id": msg.id,
                                "mime_type": msg.file.mime_type,
                                "file_path": str(Path(downloaded).relative_to(Path.cwd())),
                            })

                    progress.update(task, advance=1)

            logger.info("Archive complete (%s msgs)", progress.tasks[task].completed)

            if cfg["download_avatars"]:
                await download_avatars(client, db, media_dir / "avatars", cfg["avatar_size"])

async def download_avatars(client, db: DBHandler, avatar_root: Path, size):
    avatar_root.mkdir(parents=True, exist_ok=True)
    db.cur.execute("SELECT id FROM users WHERE avatar_path IS NULL")
    rows = db.cur.fetchall()
    for (uid,) in tqdm_asyncio(rows, desc="avatars", unit="avatar"):
        try:
            photo = await client.download_profile_photo(uid, file=avatar_root / f"{uid}.jpg")
            if photo:
                img = Image.open(photo)
                img.thumbnail((size, size))
                img.save(photo)
                db.cur.execute("UPDATE users SET avatar_path = ? WHERE id = ?", (str(photo), uid))
        except errors.FloodWaitError as e:
            logger.warning("Avatar flood-wait %s", e.seconds)
            await asyncio.sleep(e.seconds + 3)
        except Exception:
            logger.exception("Avatar fail %s", uid)

# ── Runner with proxy & account rotation ─────────────────────────────────
async def runner(cfg: Config):
    pc = ProxyCycler(cfg.proxy_conf)
    accounts = cfg.accounts
    for account in itertools.cycle(accounts):
        proxy = pc.next()
        try:
            await archive_channel(cfg, account, proxy)
            break
        except errors.FloodWaitError as e:
            logger.warning("Flood-wait %s s – rotating proxy", e.seconds)
            await asyncio.sleep(min(e.seconds, 60))
            continue
        except (errors.AuthKeyDuplicatedError, errors.AuthKeyInvalidError):
            logger.error("Auth key issue – switching account")
            continue
        except KeyboardInterrupt:
            raise
        except Exception:
            logger.exception("Unexpected error – retrying")
            continue

# ── npyscreen TUI ────────────────────────────────────────────────────────
class SpectraApp(npyscreen.NPSAppManaged):
    def onStart(self):
        self.addForm("MAIN", MenuForm, name="SPECTRA-002 Archiver")

class MenuForm(npyscreen.ActionForm):
    def create(self):
        self.cfg = Config()
        self.add(npyscreen.FixedText, value="Select Telegram account:")
        sessions = [acc["session_name"] for acc in self.cfg.accounts]
        self.acc_sel = self.add(npyscreen.TitleSelectOne, max_height=len(sessions)+2, values=sessions, scroll_exit=True)
        self.add(npyscreen.FixedText, value="Channel / group (entity):")
        self.entity = self.add(npyscreen.TitleText, name="@channel or id:", value=self.cfg["entity"])
        self.proxy_chk = self.add(npyscreen.Checkbox, name="Use rotating proxy", value=bool(self.cfg.proxy_conf.get("host")))
        self.dl_media = self.add(npyscreen.Checkbox, name="Download media", value=self.cfg["download_media"])
        self.sidecar = self.add(npyscreen.Checkbox, name="Write sidecar metadata", value=self.cfg["sidecar_metadata"])

    def on_ok(self):
        idx = self.acc_sel.value[0] if self.acc_sel.value else 0
        self.cfg.data["accounts"] = [self.cfg.accounts[idx]]
        self.cfg.data["entity"] = self.entity.value
        self.cfg.data["download_media"] = self.dl_media.value
        self.cfg.data["sidecar_metadata"] = self.sidecar.value
        self.parentApp.setNextForm(None)
        self.cfg.save()
        console.clear()
        asyncio.run(runner(self.cfg))

    def on_cancel(self):
        self.parentApp.setNextForm(None)

# ── Entrypoint & CLI ─────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(description="SPECTRA-002 Telegram archiver")
    p.add_argument("--no-tui", action="store_true", help="run without ncurses UI")
    args = p.parse_args()

    cfg = Config()

    if not args.no_tui and sys.stdin.isatty():
        SpectraApp().run()
    else:
        try:
            asyncio.run(runner(cfg))
        except KeyboardInterrupt:
            console.print("\n[bold red]Interrupted.[/]")
        except Exception:
            logger.exception("Fatal")
            sys.exit(99)

if __name__ == "__main__":
    main()
