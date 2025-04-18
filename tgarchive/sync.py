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
        """
        )
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
        """
        )
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS media (
                id INTEGER PRIMARY KEY,
                type TEXT,
                url TEXT,
                title TEXT,
                description TEXT,
                thumb TEXT
            )
        """
        )
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
        """
        )
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_username_mentions_username
            ON username_mentions(username)
        """
        )
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
        """
        )
        return self.cursor.fetchall()

    def get_username_mentions(self, username: str) -> List[Dict]:
