#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SPECTRA-002: Telegram Channel Archiving Script
A comprehensive tool for archiving Telegram channels with robust error handling,
progress visualization, and modular design for high-level cybersecurity applications.
Developed for John under NSA-style codename conventions.
"""

import os
import sys
import logging
import json
import tempfile
import shutil
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from io import BytesIO
from PIL import Image

import telethon
from telethon import TelegramClient, errors
from telethon.tl.types import Message as TelethonMessage
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm
from tqdm import tqdm
import sqlite3

# Setup logging with detailed output to both console and file
log_file = f"spectra_002_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)


class Config:
    """
    Handles configuration loading and validation for SPECTRA-002.
    Stores API credentials, media settings, and fetch limits.
    """
    def __init__(self, config_file: str = "spectra_config.json"):
        self.config_file = config_file
        self.defaults = {
            "api_id": 0,
            "api_hash": "",
            "group": "",
            "media_dir": "media",
            "download_media": True,
            "download_avatars": True,
            "media_mime_types": [],
            "fetch_limit": 0,
            "fetch_batch_size": 100,
            "fetch_wait": 2,
            "use_takeout": False,
            "avatar_size": (128, 128)
        }
        self.settings = self.load_config()

    def load_config(self) -> Dict:
        """Loads configuration from file or creates a default one if not present."""
        if not os.path.exists(self.config_file):
            logging.warning(f"Config file {self.config_file} not found. Creating default.")
            with open(self.config_file, 'w') as f:
                json.dump(self.defaults, f, indent=2)
            return self.defaults
        try:
            with open(self.config_file, 'r') as f:
                return {**self.defaults, **json.load(f)}
        except Exception as e:
            logging.error(f"Error loading config: {e}. Using defaults.")
            return self.defaults


class DBHandler:
    """
    Manages SQLite database interactions for storing users, messages, and media.
    Ensures robust error handling and transaction management.
    """
    def __init__(self, db_file: str = "spectra_database.db"):
        self.db_file = db_file
        self.conn = None
        self.cursor = None
        self.setup_database()

    def setup_database(self) -> None:
        """Sets up the SQLite database with necessary tables if they don't exist."""
        self.conn = sqlite3.connect(self.db_file)
        self.cursor = self.conn.cursor()
        # Create tables (simplified for demonstration)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                tags TEXT,
                avatar TEXT
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY,
                type TEXT,
                date TEXT,
                edit_date TEXT,
                content TEXT,
                reply_to INTEGER,
                user_id INTEGER,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS media (
                id INTEGER PRIMARY KEY,
                type TEXT,
                url TEXT,
                title TEXT,
                description TEXT,
                thumb TEXT
            )
        """)
        self.conn.commit()
        logging.info("Database initialized successfully.")

    def get_last_message_id(self) -> Tuple[Optional[int], Optional[str]]:
        """Retrieves the last message ID and date from the database."""
        self.cursor.execute("SELECT id, date FROM messages ORDER BY id DESC LIMIT 1")
        result = self.cursor.fetchone()
        return (result[0], result[1]) if result else (None, None)

    def insert_user(self, user: Dict) -> None:
        """Inserts a user record into the database."""
        try:
            self.cursor.execute(
                "INSERT OR REPLACE INTO users (id, username, first_name, last_name, tags, avatar) VALUES (?, ?, ?, ?, ?, ?)",
                (user['id'], user['username'], user['first_name'], user['last_name'], ','.join(user['tags']), user['avatar'])
            )
        except Exception as e:
            logging.error(f"Error inserting user {user['id']}: {e}")

    def insert_message(self, message: Dict) -> None:
        """Inserts a message record into the database."""
        try:
            self.cursor.execute(
                "INSERT OR REPLACE INTO messages (id, type, date, edit_date, content, reply_to, user_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (message['id'], message['type'], message['date'].isoformat(), 
                 message['edit_date'].isoformat() if message['edit_date'] else None, 
                 message['content'], message['reply_to'], message['user_id'])
            )
        except Exception as e:
            logging.error(f"Error inserting message {message['id']}: {e}")

    def insert_media(self, media: Dict) -> None:
        """Inserts a media record into the database."""
        try:
            self.cursor.execute(
                "INSERT OR REPLACE INTO media (id, type, url, title, description, thumb) VALUES (?, ?, ?, ?, ?, ?)",
                (media['id'], media['type'], media['url'], media['title'], media['description'], media['thumb'])
            )
        except Exception as e:
            logging.error(f"Error inserting media {media['id']}: {e}")

    def commit(self) -> None:
        """Commits changes to the database."""
        try:
            self.conn.commit()
        except Exception as e:
            logging.error(f"Database commit error: {e}")

    def close(self) -> None:
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")


class TelegramClientWrapper:
    """
    A wrapper around Telethon's TelegramClient for simplified interaction,
    with async support and enhanced error handling.
    """
    def __init__(self, session_file: str, config: Dict):
        self.session_file = session_file
        self.config = config
        self.client = self._setup_client()
        logging.info("Telegram client initialized.")

    def _setup_client(self) -> TelegramClient:
        """Configures the Telegram client with proxy settings if provided."""
        proxy = self.config.get("proxy", {})
        if proxy.get("enable"):
            client = TelegramClient(
                self.session_file, self.config["api_id"], self.config["api_hash"],
                proxy=(proxy["protocol"], proxy["addr"], proxy["port"])
            )
        else:
            client = TelegramClient(self.session_file, self.config["api_id"], self.config["api_hash"])
        return client

    async def start_client(self) -> None:
        """Starts the Telegram client."""
        await self.client.start()
        if self.config.get("use_takeout", False):
            await self._init_takeout()
        logging.info("Telegram client started.")

    async def _init_takeout(self) -> None:
        """Initializes Telegram takeout for data export."""
        for retry in range(3):
            try:
                takeout_client = await self.client.takeout(finalize=True).__aenter__()
                await takeout_client.get_messages("me")
                self.client = takeout_client
                logging.info("Takeout initialized successfully.")
                return
            except errors.TakeoutInitDelayError as e:
                logging.warning(f"Takeout delay: Wait {e.seconds} seconds or approve request.")
                input("Press Enter after approving data export request...")
            except errors.TakeoutInvalidError:
                logging.error("Takeout invalidated. Delete session file and retry.")
        raise Exception("Failed to initialize takeout after retries.")

    async def get_group_id(self, group: str) -> int:
        """Resolves group ID from name, username, or ID."""
        await self.client.get_dialogs()  # Sync entity cache
        try:
            entity = await self.client.get_entity(group)
            return entity.id
        except ValueError as e:
            logging.critical(f"Group {group} not found or user not a participant: {e}")
            sys.exit(1)

    async def fetch_messages(self, group_id: int, offset_id: int = 0, limit: int = 100, ids: Optional[List[int]] = None) -> List[TelethonMessage]:
        """Fetches messages from a Telegram group with flood wait handling."""
        try:
            wait_time = 0 if self.config.get("use_takeout", False) else None
            messages = await self.client.get_messages(
                group_id, offset_id=offset_id, limit=limit, wait_time=wait_time, ids=ids, reverse=True
            )
            return messages
        except errors.FloodWaitError as e:
            logging.warning(f"Flood wait triggered. Waiting {e.seconds} seconds.")
            time.sleep(e.seconds)
            return await self.fetch_messages(group_id, offset_id, limit, ids)


class SpectraSync:
    """
    Main class for syncing Telegram channel data into a local SQLite database.
    Implements CLI progress bars, detailed logging, and terminal GUI.
    """
    def __init__(self, config: Dict, session_file: str = "session.session"):
        self.config = config
        self.session_file = session_file
        self.console = Console()
        self.db = DBHandler()
        self.client = TelegramClientWrapper(session_file, config)
        self.total_messages = 0
        self.processed_messages = 0
        logging.info("SPECTRA-002 Sync initialized.")

    async def sync(self, ids: Optional[List[int]] = None, from_id: Optional[int] = None) -> None:
        """
        Syncs messages from Telegram to the local database with progress tracking.
        Supports specific message IDs or starting from a specific ID.
        """
        if ids:
            last_id, last_date = (ids[0], None)
            logging.info(f"Fetching specific message IDs: {ids}")
        elif from_id:
            last_id, last_date = (from_id, None)
            logging.info(f"Fetching from message ID: {last_id}")
        else:
            last_id, last_date = self.db.get_last_message_id()
            logging.info(f"Fetching from last message ID: {last_id} (Date: {last_date})")

        group_id = await self.client.get_group_id(self.config["group"])
        self.total_messages = 0
        
        with tqdm(total=0, desc="Fetching Messages", unit="msg") as pbar:
            while True:
                messages = await self.client.fetch_messages(
                    group_id, offset_id=last_id if last_id else 0, limit=self.config["fetch_batch_size"], ids=ids
                )
                if not messages:
                    logging.info("No more messages to fetch.")
                    break

                self.total_messages += len(messages)
                pbar.total = self.total_messages if not ids else len(ids)
                pbar.update(len(messages))

                for msg in messages:
                    self.process_message(msg)
                    self.processed_messages += 1
                    if self.processed_messages % 300 == 0:
                        self.db.commit()
                        logging.info(f"Processed {self.processed_messages} messages.")

                last_id = messages[-1].id if messages else last_id
                self.db.commit()

                if self.config["fetch_limit"] > 0 and self.processed_messages >= self.config["fetch_limit"]:
                    logging.info(f"Reached fetch limit of {self.config['fetch_limit']} messages.")
                    break

                if not ids:  # Don't sleep for specific ID fetches
                    logging.info(f"Sleeping for {self.config['fetch_wait']} seconds to avoid rate limits.")
                    time.sleep(self.config["fetch_wait"])

        self.db.commit()
        logging.info(f"Sync completed. Total messages processed: {self.processed_messages}.")

    def process_message(self, msg: TelethonMessage) -> None:
        """Processes a single Telegram message, extracting user, media, and content data."""
        try:
            user_data = self._extract_user(msg.sender, msg.chat)
            self.db.insert_user(user_data)

            media_data = None
            if msg.media:
                media_data = self._process_media(msg)

            message_data = {
                "id": msg.id,
                "type": self._etermine_message_type(msg),
                "date": msg.date,
                "edit_date": msg.edit_date,
                "content": self._extract_content(msg),
                "reply_to": msg.reply_to_msg_id if msg.reply_to else None,
                "user_id": user_data['id']
            }
            self.db.insert_message(message_data)
            if media_data:
                self.db.insert_media(media_data)
        except Exception as e:
            logging.error(f"Error processing message ID {msg.id}: {e}", exc_info=True)

    def _extract_user(self, user: any, chat: any) -> Dict:
        """Extracts user data from a message sender or chat entity."""
        tags = []
        if user is None and chat and chat.title:
            tags.append("group_self")
            avatar = self._download_avatar(chat) if self.config["download_avatars"] else None
            return {"id": chat.id, "username": chat.title, "first_name": None, "last_name": None, "tags": tags, "avatar": avatar}

        is_normal_user = isinstance(user, telethon.tl.types.User)
        if is_normal_user and user.bot:
            tags.append("bot")
        if getattr(user, "scam", False):
            tags.append("scam")
        if getattr(user, "fake", False):
            tags.append("fake")

        avatar = self._download_avatar(user) if self.config["download_avatars"] else None
        return {
            "id": user.id,
            "username": user.username if hasattr(user, "username") else str(user.id),
            "first_name": user.first_name if is_normal_user else None,
            "last_name": user.last_name if is_normal_user else None,
            "tags": tags,
            "avatar": avatar
        }

    def _download_avatar(self, entity: any) -> Optional[str]:
        """Downloads and resizes an avatar for a user or chat."""
        fname = f"avatar_{entity.id}.jpg"
        fpath = os.path.join(self.config["media_dir"], fname)
        if os.path.exists(fpath):
            return fname

        try:
            logging.info(f"Downloading avatar for ID {entity.id}...")
            b = BytesIO()
            profile_photo = self.client.client.download_profile_photo(entity, file=b)
            if profile_photo is None:
                logging.info(f"No avatar found for ID {entity.id}.")
                return None
            img = Image.open(b)
            img.thumbnail(self.config["avatar_size"], Image.LANCZOS)
            os.makedirs(self.config["media_dir"], exist_ok=True)
            img.save(fpath, "JPEG")
            return fname
        except Exception as e:
            logging.error(f"Error downloading avatar for ID {entity.id}: {e}")
            return None

    def _process_media(self, msg: TelethonMessage) -> Optional[Dict]:
        """Processes media attached to a message (photo, document, poll, etc.)."""
        if isinstance(msg.media, telethon.tl.types.MessageMediaPoll):
            return self._make_poll(msg)
        elif self.config["download_media"] and isinstance(msg.media, (telethon.tl.types.MessageMediaPhoto, telethon.tl.types.MessageMediaDocument)):
            if self.config["media_mime_types"] and hasattr(msg, "file") and msg.file.mime_type not in self.config["media_mime_types"]:
                logging.info(f"Skipping media {msg.file.name} due to MIME type {msg.file.mime_type}.")
                return None
            try:
                logging.info(f"Downloading media for message ID {msg.id}.")
                basename, fname, thumb = self._download_media(msg)
                return {"id": msg.id, "type": "photo", "url": fname, "title": basename, "description": None, "thumb": thumb}
            except Exception as e:
                logging.error(f"Error downloading media for message ID {msg.id}: {e}")
        return None

    def _download_media(self, msg: TelethonMessage) -> Tuple[str, str, Optional[str]]:
        """Downloads media and optional thumbnail to the media directory."""
        fpath = self.client.client.download_media(msg, file=tempfile.gettempdir())
        basename = os.path.basename(fpath)
        newname = f"{msg.id}.{self._get_file_ext(basename)}"
        os.makedirs(self.config["media_dir"], exist_ok=True)
        shutil.move(fpath, os.path.join(self.config["media_dir"], newname))
        tname = None
        if isinstance(msg.media, telethon.tl.types.MessageMediaPhoto):
            tpath = self.client.client.download_media(msg, file=tempfile.gettempdir(), thumb=1)
            tname = f"thumb_{msg.id}.{self._get_file_ext(os.path.basename(tpath))}"
            shutil.move(tpath, os.path.join(self.config["media_dir"], tname))
        return basename, newname, tname

    def _get_file_ext(self, fname: str) -> str:
        """Extracts file extension or returns a default."""
        return fname.split(".")[-1] if "." in fname and len(fname.split(".")[-1]) < 6 else "file"

    def _make_poll(self, msg: TelethonMessage) -> Dict:
        """Processes a poll media type into a structured format."""
        if not msg.media.results or not msg.media.results.results:
            return None
        options = [{"label": a.text, "count": 0, "correct": False} for a in msg.media.poll.answers]
        total = msg.media.results.total_voters
        if msg.media.results.results:
            for i, r in enumerate(msg.media.results.results):
                options[i]["count"] = r.v "description": json.dumps(options),
            "thumb": None
        }

    def _determine_message_type(self, msg: TelethonMessage) -> str:
        """Determines the type of message (normal, user join, etc.)."""
        if msg.action:
            if isinstance(msg.action, telethon.tl.types.MessageActionChatAddUser):
                return "user_joined"
            elif isinstance(msg.action, telethon.tl.types.MessageActionChatJoinedByLink):
                return "user_joined_by_link"
            elif isinstance(msg.action, telethon.tl.types.MessageActionChatDeleteUser):
                return "user_left"
        return "message"

    def _extract_content(self, msg: TelethonMessage) -> str:
        """Extracts content from a message, prioritizing sticker alt if available."""
        if isinstance(msg.media, telethon.tl.types.MessageMediaDocument) and msg.media.document.mime_type == "application/x-tgsticker":
            alt = [a.alt for a in msg.media.document.attributes if isinstance(a, telethon.tl.types.DocumentAttributeSticker)]
            return alt[0] if alt else msg.raw_text
        return msg.raw_text

    def display_gui(self) -> None:
        """Displays a terminal-based GUI using rich for user interaction."""
        self.console.clear()
        self.console.print(Panel("[bold cyan]SPECTRA-002: Telegram Archiving Tool[/bold cyan]", title="Welcome", border_style="green"))
        table = Table(title="Options")
        table.add_column("Option", style="cyan")
        table.add_column("Description", style="green")
        table.add_row("1", "Sync All Messages (from last known ID)")
        table.add_row("2", "Sync Specific Message IDs")
        table.add_row("3", "Sync from Specific Message ID")
        table.add_row("4", "Configure Settings")
        table.add_row("5", "Exit")
        self.console.print(table)

    async def run(self) -> None:
        """Runs the main application loop with GUI and user input handling."""
        await self.client.start_client()
        while True:
            self.display_gui()
            choice = Prompt.ask("Select an option (1-5)", choices=["1", "2", "3", "4", "5"], default="1")
            if choice == "1":
                await self.sync()
            elif choice == "2":
                ids = Prompt.ask("Enter message IDs (comma-separated)").split(",")
                await self.sync(ids=[int(id.strip()) for id in ids if id.strip().isdigit()])
            elif choice == "3":
                from_id = int(Prompt.ask("Enter starting message ID"))
                await self.sync(from_id=from_id)
            elif choice == "4":
                self.console.print("[yellow]Configuration editing not implemented yet.[/yellow]")
            elif choice == "5":
                if Confirm.ask("Are you sure you want to exit?"):
                    self.db.close()
                    logging.info("SPECTRA-002 Sync terminated by user.")
                    break


if __name__ == "__main__":
    import asyncio
    config = Config().settings
    sync = SpectraSync(config)
    asyncio.run(sync.run())
