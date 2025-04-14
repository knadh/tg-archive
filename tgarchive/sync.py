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
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set
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
            "avatar_size": (128, 128),
            "collect_usernames": True  # New option for username collection
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
        # Setup username collection tables
        self.setup_username_tables()
        self.conn.commit()
        logging.info("Database initialized successfully.")

    def setup_username_tables(self) -> None:
        """Set up tables for username collection."""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS username_mentions (
                id INTEGER PRIMARY KEY,
                username TEXT,
                message_id INTEGER,
                date TEXT,
                source_type TEXT,
                FOREIGN KEY(message_id) REFERENCES messages(id)
            )
        """)
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_username_mentions_username
            ON username_mentions(username)
        """)
        self.conn.commit()
        logging.info("Username collection tables initialized.")

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

    def store_username(self, username: str, message_id: int, date: str = None, source_type: str = "mention") -> None:
        """Store a username mention in the database."""
        if not date:
            # Get the date from the message
            self.cursor.execute("SELECT date FROM messages WHERE id = ?", (message_id,))
            result = self.cursor.fetchone()
            date = result[0] if result else datetime.now().isoformat()
            
        try:
            self.cursor.execute(
                "INSERT INTO username_mentions (username, message_id, date, source_type) VALUES (?, ?, ?, ?)",
                (username, message_id, date, source_type)
            )
        except Exception as e:
            logging.error(f"Error storing username {username}: {e}")

    def get_usernames(self) -> List[Tuple[str, int]]:
        """Get all collected usernames with mention counts."""
        self.cursor.execute("""
            SELECT username, COUNT(*) as mention_count 
            FROM username_mentions 
            GROUP BY username 
            ORDER BY mention_count DESC
        """)
        return self.cursor.fetchall()

    def get_username_mentions(self, username: str) -> List[Dict]:
        """Get all mentions of a specific username."""
        self.cursor.execute("""
            SELECT um.id, um.username, um.message_id, um.date, um.source_type, m.content
            FROM username_mentions um
            JOIN messages m ON um.message_id = m.id
            WHERE um.username = ?
            ORDER BY um.date DESC
        """, (username,))
        
        results = []
        for row in self.cursor.fetchall():
            results.append({
                "id": row[0],
                "username": row[1],
                "message_id": row[2],
                "date": row[3],
                "source_type": row[4],
                "message_content": row[5]
            })
        return results

    def get_pending_user_id_resolutions(self) -> List[Tuple[int, int]]:
        """Get all user IDs that need to be resolved to usernames."""
        self.cursor.execute("""
            SELECT id, REPLACE(username, 'user_id:', '') as user_id
            FROM username_mentions 
            WHERE username LIKE 'user_id:%'
        """)
        return [(row[0], int(row[1])) for row in self.cursor.fetchall()]

    def update_resolved_username(self, mention_id: int, username: str) -> None:
        """Update a username mention with the resolved username."""
        try:
            self.cursor.execute(
                "UPDATE username_mentions SET username = ? WHERE id = ?",
                (username, mention_id)
            )
        except Exception as e:
            logging.error(f"Error updating resolved username for mention {mention_id}: {e}")

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


class UsernameCollector:
    """
    Handles collection of Telegram usernames from various sources:
    - Message senders
    - @mentions in message text
    - Forwarded messages
    - Message entities
    """
    def __init__(self, db_handler: DBHandler):
        self.db = db_handler
        # Match valid Telegram usernames (5-32 chars, letters, numbers, underscores)
        self.username_pattern = re.compile(r'@([a-zA-Z0-9_]{5,32})')
        
    def extract_from_text(self, text: str) -> List[str]:
        """Extract usernames from text content using regex."""
        if not text:
            return []
        return self.username_pattern.findall(text)
    
    def extract_from_entities(self, message: TelethonMessage) -> List[str]:
        """Extract usernames from message entities (mentions)."""
        usernames = []
        if hasattr(message, 'entities') and message.entities:
            for entity in message.entities:
                if isinstance(entity, telethon.tl.types.MessageEntityMention):
                    # Extract the mention from the message text
                    start = entity.offset
                    end = start + entity.length
                    mention = message.text[start:end]
                    if mention.startswith('@'):
                        usernames.append(mention[1:])  # Remove the @ symbol
                elif isinstance(entity, telethon.tl.types.MessageEntityMentionName):
                    # This entity contains the user_id directly
                    user_id = entity.user_id
                    # We'll need to resolve this ID to a username later
                    usernames.append(f"user_id:{user_id}")
        return usernames
    
    def extract_from_forward(self, message: TelethonMessage) -> List[str]:
        """Extract username from forwarded message."""
        usernames = []
        if hasattr(message, 'forward') and message.forward:
            if hasattr(message.forward.from_id, 'user_id'):
                # This is a user ID that needs to be resolved to a username
                user_id = message.forward.from_id.user_id
                usernames.append(f"user_id:{user_id}")
            elif hasattr(message.forward, 'sender_id'):
                user_id = message.forward.sender_id
                usernames.append(f"user_id:{user_id}")
        return usernames
    
    def process_message(self, message: TelethonMessage) -> Dict[str, List[str]]:
        """Process a message to extract all usernames."""
        result = {
            "text_mentions": [],
            "entity_mentions": [],
            "forward_mentions": []
        }
        
        # Extract from text content
        if hasattr(message, 'text') and message.text:
            result["text_mentions"] = self.extract_from_text(message.text)
        
        # Extract from message entities
        result["entity_mentions"] = self.extract_from_entities(message)
        
        # Extract from forwarded messages
        result["forward_mentions"] = self.extract_from_forward(message)
        
        return result
    
    def store_usernames(self, username_data: Dict[str, List[str]], message_id: int, date: str) -> None:
        """Store collected usernames in the database."""
        # Store text mentions
        for username in username_data["text_mentions"]:
            self.db.store_username(username, message_id, date, "text_mention")
                
        # Store entity mentions
        for username in username_data["entity_mentions"]:
            self.db.store_username(username, message_id, date, "entity_mention")
                
        # Store forward mentions
        for username in username_data["forward_mentions"]:
            self.db.store_username(username, message_id, date, "forward_mention")

    async def resolve_user_ids(self, client_wrapper) -> int:
        """Resolve user IDs to usernames."""
        # Get all user IDs that need to be resolved
        pending_resolutions = self.db.get_pending_user_id_resolutions()
        resolved_count = 0
        
        for mention_id, user_id in pending_resolutions:
            username = await client_wrapper.resolve_user_id(user_id)
            if username:
                # Update the mention with the resolved username
                self.db.update_resolved_username(mention_id, username)
                resolved_count += 1
        
        self.db.commit()
        return resolved_count


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

    async def resolve_user_id(self, user_id: int) -> Optional[str]:
        """Resolves a user ID to a username."""
        try:
            entity = await self.client.get_entity(user_id)
            if hasattr(entity, 'username') and entity.username:
                return entity.username
            return None
        except Exception as e:
            logging.error(f"Error resolving user ID {user_id}: {e}")
            return None


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
        self.username_collector = UsernameCollector(self.db)
        self.total_messages = 0
        self.processed_messages = 0
        self.collected_usernames = 0
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

        # Resolve user IDs to usernames
        if self.config.get("collect_usernames", True):
            logging.info("Resolving user IDs to usernames...")
            resolved_count = await self.username_collector.resolve_user_ids(self.client)
            logging.info(f"Resolved {resolved_count} user IDs to usernames.")

        self.db.commit()
        logging.info(f"Sync completed. Total messages processed: {self.processed_messages}.")
        if self.config.get("collect_usernames", True):
            logging.info(f"Total usernames collected: {self.collected_usernames}")

    def process_message(self, msg: TelethonMessage) -> None:
        """Processes a single Telegram message, extracting user, media, and content data."""
        try:
            user_data = self._extract_user(msg.sender, msg.chat)
            self.db.insert_user(user_data)

            # Collect usernames if enabled in config
            if self.config.get("collect_usernames", True):
                username_data = self.username_collector.process_message(msg)
                self.username_collector.store_usernames(username_data, msg.id, msg.date.isoformat())
                
                # Count collected usernames
                username_count = (
                    len(username_data["text_mentions"]) + 
                    len(username_data["entity_mentions"]) + 
                    len(username_data["forward_mentions"])
                )
                self.collected_usernames += username_count
                
                if username_count > 0:
                    logging.debug(f"Collected {username_count} usernames from message {msg.id}")

            media_data = None
            if msg.media:
                media_data = self._process_media(msg)

            message_data = {
                "id": msg.id,
                "type": self._determine_message_type(msg),
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
        # Implementation would go here
        # This is a placeholder as the original code was truncated
        return None

    def _determine_message_type(self, msg: TelethonMessage) -> str:
        """Determines the type of a message based on its content."""
        # Implementation would go here
        # This is a placeholder as the original code was truncated
        return "text"

    def _extract_content(self, msg: TelethonMessage) -> str:
        """Extracts the content from a message."""
        # Implementation would go here
        # This is a placeholder as the original code was truncated
        return msg.text if hasattr(msg, "text") else ""

    async def list_usernames(self) -> None:
        """Lists all collected usernames."""
        usernames = self.db.get_usernames()
        
        table = Table(title="Collected Usernames")
        table.add_column("Username", style="cyan")
        table.add_column("Mention Count", style="green")
        
        for username, count in usernames:
            if not username.startswith("user_id:"):  # Skip unresolved user IDs
                table.add_row(username, str(count))
        
        self.console.print(table)
        
        total_usernames = len([u for u, _ in usernames if not u.startswith("user_id:")])
        self.console.print(f"Total unique usernames collected: {total_usernames}")

    async def username_details(self, username: str) -> None:
        """Shows details about a specific username."""
        mentions = self.db.get_username_mentions(username)
        
        if not mentions:
            self.console.print(f"No mentions found for username: {username}")
            return
        
        table = Table(title=f"Mentions of @{username}")
        table.add_column("Date", style="cyan")
        table.add_column("Source Type", style="green")
        table.add_column("Message Content", style="white")
        
        for mention in mentions[:20]:  # Limit to 20 mentions to avoid overwhelming output
            date = datetime.fromisoformat(mention["date"]).strftime("%Y-%m-%d %H:%M:%S")
            content = mention["message_content"]
            if len(content) > 50:
                content = content[:47] + "..."
            table.add_row(date, mention["source_type"], content)
        
        self.console.print(table)
        
        if len(mentions) > 20:
            self.console.print(f"Showing 20 of {len(mentions)} mentions. Use database queries for more detailed analysis.")


async def main():
    """Main entry point for the script."""
    config = Config().settings
    sync = SpectraSync(config)
    
    await sync.client.start_client()
    
    # Command line argument parsing would go here
    # For demonstration, we'll just sync messages
    await sync.sync()
    
    # List collected usernames
    if config.get("collect_usernames", True):
        await sync.list_usernames()
    
    sync.db.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
