"""
SPECTRA-002 — Telegram Channel Archiver (v2.4)
================================================
*Multi-account* · *Proxy-rotating* · *Sidecar-metadata* · *Curses TUI* · *Topic Support* · *Auto-Config*

Overview
--------
A forensic-grade Telegram archiver designed for SWORD-EPI. New in **v2.4**:

1. **Auto-Config Integration** — automatically loads configs from TELESMASHER generator
   and selects available accounts without manual intervention.
2. **Topic/Thread Support** — now archives all topics/threads in groups,
   not just the main chat.
3. **Sidecar metadata** — every downloaded file now gains a `.json` twin
   containing the parent message (inc. probable passwords, captions, user,
   date, etc.).  Naming: `orig.ext` → `orig.ext.json`.
4. **Full ncurses TUI** — interactive configuration via *npyscreen*: pick
   account, channel, proxy profile, and options before launch.
5. **Argparse flags** — `--no-tui` for headless automation.

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
import random
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from types import TracebackType
from typing import Any, Dict, List, Optional, Tuple, Union

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
from telethon.tl.types import InputPeerChannel, InputPeerChat  # type: ignore
from tqdm.asyncio import tqdm_asyncio  # type: ignore

# ── Globals ───────────────────────────────────────────────────────────────
APP_NAME = "spectra_002_archiver"
__version__ = "2.4.0"
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
    "archive_topics": True,  # New option to control topic archiving
    "default_forwarding_destination_id": None,
}

# ── Config loader ─────────────────────────────────────────────────────────
@dataclass
class Config:
    path: Path = Path("spectra_config.json")
    data: Dict[str, Any] = field(default_factory=lambda: DEFAULT_CFG.copy())

    def __post_init__(self):
        # First try to load generated configs from TELESMASHER
        auto_config_loaded = self._try_load_generated_configs()

        loaded_from_file = False
        if not auto_config_loaded and self.path.exists():
            try:
                file_data = json.loads(self.path.read_text())
                self.data.update(file_data)
                logger.info(f"Loaded legacy config from {self.path}")
                loaded_from_file = True
            except json.JSONDecodeError as exc:
                logger.warning("Bad JSON in config – using defaults (%s)", exc)
        
        if not loaded_from_file and not auto_config_loaded:
            # If no config file and no auto-config, save default
            self.save()
            console.print(
                "[yellow]Config not found; default created at"
                f" {self.path}.  Edit credentials then rerun.[/yellow]"
            )
            sys.exit(1)
        
        # Ensure default_forwarding_destination_id is present
        if "default_forwarding_destination_id" not in self.data:
            self.data["default_forwarding_destination_id"] = DEFAULT_CFG["default_forwarding_destination_id"]

        # back-compat
        if not self.data.get("accounts"):
            self.data["accounts"] = [
                {
                    "api_id": self.data["api_id"],
                    "api_hash": self.data["api_hash"],
                    "session_name": "spectra_legacy",
                }
            ]
            
        # Convert TELESMASHER accounts format if needed
        self._normalize_accounts()
    
    def _try_load_generated_configs(self) -> bool:
        """Try to load configs from TELESMASHER generator format."""
        config_paths = [
            Path("config/telegram_reporter_config.json"),
            Path("./config/telegram_reporter_config.json"),
            Path("../config/telegram_reporter_config.json"),
            Path.cwd() / "config" / "telegram_reporter_config.json",
        ]
        
        for cfg_path in config_paths:
            if cfg_path.exists():
                try:
                    generated_config = json.loads(cfg_path.read_text())
                    logger.info(f"Found generated config at {cfg_path}")
                    
                    # Copy accounts and proxy settings
                    if "accounts" in generated_config:
                        self.data["telesmasher_accounts"] = generated_config["accounts"]
                        logger.info(f"Loaded {len(generated_config['accounts'])} accounts from generated config")
                    
                    if "proxy" in generated_config:
                        self.data["proxy"] = generated_config["proxy"]
                        logger.info("Loaded proxy settings from generated config")
                    
                    return True
                except (json.JSONDecodeError, PermissionError) as e:
                    logger.warning(f"Error loading generated config: {e}")
        
        return False
    
    def _normalize_accounts(self):
        """Convert TELESMASHER account format to SPECTRA format if needed."""
        telesmasher_accounts = self.data.get("telesmasher_accounts", [])
        
        if telesmasher_accounts:
            # Convert format and add to existing accounts
            for idx, acc in enumerate(telesmasher_accounts):
                if all(k in acc for k in ("phone_number", "api_id", "api_hash")):
                    phone = acc["phone_number"].replace("+", "")
                    new_account = {
                        "api_id": acc["api_id"],
                        "api_hash": acc["api_hash"],
                        "session_name": f"spectra_auto_{phone}_{idx}",
                        "phone_number": acc["phone_number"],
                        "password": acc.get("password", ""),
                    }
                    
                    # Check if this account already exists in our list
                    exists = False
                    for existing in self.data["accounts"]:
                        if (existing.get("api_id") == new_account["api_id"] and 
                            existing.get("api_hash") == new_account["api_hash"]):
                            exists = True
                            break
                    
                    if not exists:
                        self.data["accounts"].append(new_account)
            
            logger.info(f"Normalized {len(telesmasher_accounts)} TELESMASHER accounts")

    # helpers
    def save(self):
        self.path.write_text(json.dumps(self.data, indent=2))

    def __getitem__(self, item):
        return self.data[item]

    def __setitem__(self, key, value):
        self.data[key] = value

    @property
    def accounts(self):
        return self.data["accounts"]

    @property
    def default_forwarding_destination_id(self):
        return self.data.get("default_forwarding_destination_id")

    @default_forwarding_destination_id.setter
    def default_forwarding_destination_id(self, value: Optional[str]):
        self.data["default_forwarding_destination_id"] = value
    
    @property
    def active_accounts(self) -> List[Dict[str, Any]]:
        """Return a list of accounts that are likely to work."""
        # Preferred accounts are those created from TELESMASHER configs
        preferred = [acc for acc in self.accounts 
                    if acc.get("api_id") and acc.get("api_hash") and 
                    acc.get("session_name", "").startswith("spectra_auto_")]
        
        # If we have preferred accounts, use those; otherwise use all accounts
        return preferred if preferred else self.accounts
    
    def auto_select_account(self) -> Optional[Dict[str, Any]]:
        """Automatically select an account to use."""
        active = self.active_accounts
        
        if not active:
            logger.warning("No active accounts available")
            return None
        
        # Randomly select an account for load balancing
        selected = random.choice(active)
        logger.info(f"Auto-selected account: {selected.get('session_name', 'unknown')}")
        return selected

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
            
            CREATE TABLE IF NOT EXISTS topics (
                id INTEGER PRIMARY KEY,
                entity_id INTEGER NOT NULL,
                title TEXT,
                date_created TEXT
            );
            
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                topic_id INTEGER REFERENCES topics(id),
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

    def last_message_id(self, topic_id=None):
        if topic_id is not None:
            row = self.cur.execute("SELECT MAX(id) FROM messages WHERE topic_id = ?", (topic_id,)).fetchone()
        else:
            row = self.cur.execute("SELECT MAX(id) FROM messages").fetchone()
        return row[0] if row and row[0] else None

    # insert helpers
    def add_user(self, u):
        self.cur.execute(
            "INSERT OR REPLACE INTO users(id, username, first_name, last_name, avatar_path) VALUES (?, ?, ?, ?, ?)",
            (u.id, getattr(u, "username", None), getattr(u, "first_name", None), getattr(u, "last_name", None), None),
        )

    def add_topic(self, topic_id, entity_id, title, date_created):
        self.cur.execute(
            "INSERT OR REPLACE INTO topics(id, entity_id, title, date_created) VALUES (?, ?, ?, ?)",
            (topic_id, entity_id, title, date_created),
        )
        return topic_id

    def add_message(self, d):
        self.cur.execute(
            "INSERT OR REPLACE INTO messages(id, user_id, topic_id, date, edit_date, content, reply_to) VALUES (:id, :user_id, :topic_id, :date, :edit_date, :content, :reply_to)",
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
        "topic_id": getattr(msg, "topic", {}).get("id") if hasattr(msg, "topic") else None,
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

# ── Topic handler ─────────────────────────────────────────────────────────
async def get_topics(client, entity):
    """Get all topics in a group if supported."""
    try:
        topics = await client.get_topics(entity)
        if not topics:
            return []
        return topics
    except (AttributeError, errors.ChatAdminRequiredError, errors.FloodWaitError) as e:
        if isinstance(e, errors.FloodWaitError):
            logger.warning("Flood-wait %s s when fetching topics", e.seconds)
            await asyncio.sleep(e.seconds + 3)
            return await get_topics(client, entity)
        logger.info("Topics not supported or accessible for this entity")
        return []

# ── Message archiving ─────────────────────────────────────────────────────
async def archive_messages(client, entity, topic_id, db, cfg, media_dir, progress, task):
    """Archive messages for a specific topic or main channel."""
    last_id = db.last_message_id(topic_id)
    
    kwargs = {
        "offset_id": last_id or 0,
        "reverse": True,
        "wait_time": cfg["sleep_between_batches"]
    }
    
    if topic_id is not None:
        kwargs["topic"] = topic_id
    
    async for msg in client.iter_messages(entity, **kwargs):
        d = {
            "id": msg.id,
            "user_id": msg.sender_id,
            "topic_id": topic_id,
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
            topic_subdir = f"topic_{topic_id}" if topic_id is not None else "main"
            dest_dir = media_dir / topic_subdir
            dest = dest_dir / f"{msg.id}_{msg.file.id}"
            downloaded = await safe_download_media(msg, dest, cfg["media_mime_whitelist"], cfg["sidecar_metadata"])
            if downloaded:
                db.add_media({
                    "id": msg.file.id,
                    "message_id": msg.id,
                    "mime_type": msg.file.mime_type,
                    "file_path": str(Path(downloaded).relative_to(Path.cwd())),
                })

        progress.update(task, advance=1)

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
        entity_id = entity.id

        with DBHandler(Path(cfg["db_path"])) as db:
            # Count approximate total messages (this is an estimate)
            total_msgs = await client.get_messages(entity, limit=0)
            archive_task = progress.add_task("[green]Archiving main chat", total=len(total_msgs))
            
            with progress:
                # Archive main chat first
                await archive_messages(client, entity, None, db, cfg, media_dir, progress, archive_task)
                
                # Now handle topics if enabled
                if cfg["archive_topics"]:
                    topics = await get_topics(client, entity)
                    if topics:
                        logger.info(f"Found {len(topics)} topics to archive")
                        
                        for topic in topics:
                            topic_id = topic.id
                            topic_title = getattr(topic, "title", f"Topic {topic_id}")
                            topic_date = datetime.now(TZ).isoformat()
                            
                            db.add_topic(topic_id, entity_id, topic_title, topic_date)
                            
                            # Get an estimate of messages in this topic
                            try:
                                topic_msg_count = await client.get_messages(entity, limit=0, topic=topic_id)
                                topic_task = progress.add_task(
                                    f"[cyan]Topic: {topic_title}", 
                                    total=len(topic_msg_count)
                                )
                                
                                await archive_messages(
                                    client, entity, topic_id, db, cfg, 
                                    media_dir, progress, topic_task
                                )
                            except Exception as e:
                                logger.exception(f"Error archiving topic {topic_id}: {e}")
                                continue

            logger.info("Archive complete (%s msgs)", progress.tasks[archive_task].completed)

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
async def runner(cfg: Config, auto_account=None):
    pc = ProxyCycler(cfg.proxy_conf)
    
    # Use auto-selected account if provided, otherwise use accounts from config
    accounts = [auto_account] if auto_account else cfg.accounts
    
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
        
        # Auto-select account based on TELESMASHER configs if available
        self.auto_account = self.cfg.auto_select_account()
        
        # Show special notice if using auto-selected account
        if self.auto_account:
            self.add(npyscreen.FixedText, value=f"[AUTO-SELECTED ACCOUNT: {self.auto_account.get('session_name')}]")
            self.add(npyscreen.FixedText, value="")
        
        self.add(npyscreen.FixedText, value="Select Telegram account (or keep auto-selected):")
        sessions = [acc["session_name"] for acc in self.cfg.accounts]
        default_idx = sessions.index(self.auto_account["session_name"]) if self.auto_account else 0
        self.acc_sel = self.add(
            npyscreen.TitleSelectOne, 
            max_height=min(len(sessions)+2, 8),  # Limit height but make scrollable
            values=sessions, 
            scroll_exit=True,
            value=[default_idx]
        )
        
        self.add(npyscreen.FixedText, value="Channel / group (entity):")
        self.entity = self.add(npyscreen.TitleText, name="@channel or id:", value=self.cfg["entity"])
        self.proxy_chk = self.add(npyscreen.Checkbox, name="Use rotating proxy", value=bool(self.cfg.proxy_conf.get("host")))
        self.dl_media = self.add(npyscreen.Checkbox, name="Download media", value=self.cfg["download_media"])
        self.sidecar = self.add(npyscreen.Checkbox, name="Write sidecar metadata", value=self.cfg["sidecar_metadata"])
        self.archive_topics = self.add(npyscreen.Checkbox, name="Archive topics/threads", value=self.cfg["archive_topics"])
        self.auto_mode = self.add(npyscreen.Checkbox, name="Use auto-selected account", value=bool(self.auto_account))

    def on_ok(self):
        if self.auto_mode.value and self.auto_account:
            selected_account = self.auto_account
        else:
            idx = self.acc_sel.value[0] if self.acc_sel.value else 0
            selected_account = self.cfg.accounts[idx]
            
        self.cfg.data["entity"] = self.entity.value
        self.cfg.data["download_media"] = self.dl_media.value
        self.cfg.data["sidecar_metadata"] = self.sidecar.value
        self.cfg.data["archive_topics"] = self.archive_topics.value
        self.parentApp.setNextForm(None)
        self.cfg.save()
        console.clear()
        asyncio.run(runner(self.cfg, selected_account))

    def on_cancel(self):
        self.parentApp.setNextForm(None)

# ── Entrypoint & CLI ─────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(description="SPECTRA-002 Telegram archiver")
    p.add_argument("--no-tui", action="store_true", help="run without ncurses UI")
    p.add_argument("--auto", action="store_true", help="use auto-selected account and skip TUI")
    p.add_argument("--entity", help="channel or group to archive (e.g. @channel)")
    args = p.parse_args()

    cfg = Config()
    
    # Auto mode: select account and run immediately if entity is provided
    if args.auto:
        auto_account = cfg.auto_select_account()
        if auto_account:
            # Use provided entity or existing config
            if args.entity:
                cfg.data["entity"] = args.entity
                
            if cfg["entity"]:
                try:
                    asyncio.run(runner(cfg, auto_account))
                except KeyboardInterrupt:
                    console.print("\n[bold red]Interrupted.[/]")
                except Exception:
                    logger.exception("Fatal")
                    sys.exit(99)
            else:
                console.print("[bold red]ERROR:[/] Entity (channel/group) required for auto mode")
                console.print("Use --entity @channelname or set it in config file")
                sys.exit(1)
        else:
            console.print("[bold red]ERROR:[/] No valid accounts found for auto mode")
            console.print("Make sure config/telegram_reporter_config.json exists with valid accounts")
            sys.exit(1)
    # Interactive mode with TUI
    elif not args.no_tui and sys.stdin.isatty():
        SpectraApp().run()
    # CLI mode
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
