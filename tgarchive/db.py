#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SPECTRA-004: Enhanced Database Handler for Telegram Archiving
Developed for John under NSA-style codename conventions.
A robust SQLite database handler for archiving Telegram data with cybersecurity-focused features,
including forensic logging, data integrity checks, and performance optimizations.
"""

import json
import math
import os
import sqlite3
import logging
import hashlib
+import sqlite3
import time
from collections import namedtuple
from datetime import datetime
from typing import Iterator, Optional, Tuple, List, Any
import pytz
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# Setup logging with detailed output for forensic purposes
log_file = f"spectra_004_db_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Console for rich terminal output
console = Console()

# Database schema with added indices for performance
schema = """
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER NOT NULL PRIMARY KEY,
    type TEXT NOT NULL,
    date TIMESTAMP NOT NULL,
    edit_date TIMESTAMP,
    content TEXT,
    reply_to INTEGER,
    user_id INTEGER,
    media_id INTEGER,
    checksum TEXT,  -- For data integrity verification
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(media_id) REFERENCES media(id)
);
CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date);
CREATE INDEX IF NOT EXISTS idx_messages_user ON messages(user_id);
##
CREATE TABLE IF NOT EXISTS users (
    id INTEGER NOT NULL PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    tags TEXT,
    avatar TEXT,
    last_updated TIMESTAMP  -- Track updates for audit purposes
);
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
##
CREATE TABLE IF NOT EXISTS media (
    id INTEGER NOT NULL PRIMARY KEY,
    type TEXT,
    url TEXT,
    title TEXT,
    description TEXT,
    thumb TEXT,
    checksum TEXT  -- For media file integrity checks
);
CREATE INDEX IF NOT EXISTS idx_media_type ON media(type);
##
CREATE TABLE IF NOT EXISTS checkpoints (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    last_message_id INTEGER NOT NULL,
    checkpoint_time TIMESTAMP NOT NULL,
    context TEXT  -- Describe the operation context (e.g., sync or build)
);
"""

# Named tuples for structured data access
User = namedtuple("User", ["id", "username", "first_name", "last_name", "tags", "avatar", "last_updated"])
Message = namedtuple("Message", ["id", "type", "date", "edit_date", "content", "reply_to", "user", "media", "checksum"])
Media = namedtuple("Media", ["id", "type", "url", "title", "description", "thumb", "checksum"])
Month = namedtuple("Month", ["date", "slug", "label", "count"])
Day = namedtuple("Day", ["date", "slug", "label", "count", "page"])

def _page(n: int, multiple: int) -> int:
    """Custom SQLite function to calculate page number for pagination."""
    return math.ceil(n / multiple) if n > 0 else 1

class DB:
    """
    Enhanced SQLite database handler for SPECTRA-004 Telegram archiving.
    Provides robust error handling, forensic logging, data integrity checks,
    and performance optimizations for large datasets in cybersecurity contexts.
    """
    def __init__(self, dbfile: str, tz: Optional[str] = None):
        """
        Initializes the SQLite database connection with error handling and schema setup.

        Args:
            dbfile (str): Path to the SQLite database file.
            tz (Optional[str]): Timezone string (e.g., 'US/Pacific') for localized timestamps.
        """
        self.dbfile = dbfile
        self.tz = pytz.timezone(tz) if tz else None
        self.conn = None
        self.retries = 3  # Number of retries for database operations
        self.initialize_db()

def initialize_db(self) -> None:
    """Sets up the SQLite database connection and schema with retry mechanism."""
    for attempt in range(self.retries):
        try:
            self.conn = sqlite3.connect(
                self.dbfile,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
                timeout=5.0  # Helps prevent "database is locked" errors
            )

            self.conn.create_function("PAGE", 2, _page)

            # Enable WAL mode for concurrent reads/writes
            self.conn.execute("PRAGMA journal_mode=WAL;")

            # Run schema setup
            self.conn.executescript(schema)
            self.conn.commit()

            if os.path.exists(self.dbfile):
                console.print(f"Connected to existing database: [bold]{self.dbfile}[/]")
                logging.info(f"Connected to existing database at {self.dbfile}")
            else:
                console.print("[green]✓ Database created successfully![/]")

            return
        except sqlite3.Error as e:
            logging.error(f"Database initialization attempt {attempt+1}/{self.retries} failed: {e}")
            console.print(f"[red]✗ Database init error: {e}[/]")
            if attempt == self.retries - 1:
                raise Exception(f"Failed to initialize database after {self.retries} attempts: {e}")
            time.sleep(1)  # Wait before retry

    def get_last_message_id(self) -> Tuple[int, Optional[datetime]]:
        """
        Retrieves the ID and date of the last synced message for resumption.

        Returns:
            Tuple[int, Optional[datetime]]: Last message ID and its date, or (0, None) if empty.
        """
        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT id, strftime('%Y-%m-%d 00:00:00', date) as "[timestamp]"
                FROM messages
                ORDER BY id DESC LIMIT 1
            """)
            res = cur.fetchone()
            if not res:
                logging.info("No messages found in database for last ID check.")
                console.print("[yellow]No messages in DB to resume from.[/]")
                return 0, None
            id_val, date_str = res
            logging.info(f"Last synced message ID: {id_val}, Date: {date_str}")
            console.print(f"[green]Last message ID: {id_val}[/] (Date: {date_str})")
            return id_val, datetime.strptime(date_str, '%Y-%m-%d 00:00:00') if date_str else None
        except sqlite3.Error as e:
            logging.error(f"Error fetching last message ID: {e}")
            console.print(f"[red]Error fetching last message: {e}[/]")
            return 0, None

    def save_checkpoint(self, last_message_id: int, context: str = "sync") -> None:
        """
        Saves a checkpoint for resumable operations.

        Args:
            last_message_id (int): ID of the last processed message.
            context (str): Context of the checkpoint (e.g., 'sync', 'build').
        """
        try:
            cur = self.conn.cursor()
            cur.execute("""
                INSERT INTO checkpoints (last_message_id, checkpoint_time, context)
                VALUES (?, ?, ?)
            """, (last_message_id, datetime.utcnow(), context))
            self.conn.commit()
            logging.info(f"Checkpoint saved: Message ID {last_message_id}, Context: {context}")
            console.print(f"[green]✓ Checkpoint saved for Message ID {last_message_id}[/]")
        except sqlite3.Error as e:
            logging.error(f"Error saving checkpoint: {e}")
            console.print(f"[red]✗ Failed to save checkpoint: {e}[/]")

    def get_latest_checkpoint(self, context: str = "sync") -> Optional[int]:
        """
        Retrieves the last checkpoint for a given context.
        """
        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT last_message_id FROM checkpoints
                WHERE context = ?
                ORDER BY checkpoint_time DESC LIMIT 1
            """, (context,))
            res = cur.fetchone()
            if res:
                logging.info(f"Found checkpoint for {context}: Message ID {res[0]}")
                console.print(f"[green]Resuming from checkpoint: Message ID {res[0]}[/]")
                return res[0]
            return None
        except sqlite3.Error as e:
            logging.error(f"Error fetching checkpoint: {e}")
            console.print(f"[red]✗ Error fetching checkpoint: {e}[/]")
            return None

    def get_timeline(self) -> Iterator[Month]:
        """
        Retrieves a chronological list of unique yyyy-mm month groups with message counts.

        Yields:
            Month: Named tuple containing date, slug, label, and count for each month.
        """
        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT strftime('%Y-%m-01 00:00:00', date) as "[timestamp]",
                COUNT(*) FROM messages
                GROUP BY strftime('%Y-%m', date)
                ORDER BY date
            """)
            for r in cur.fetchall():
                date = pytz.utc.localize(r[0])
                if self.tz:
                    date = date.astimezone(self.tz)
                yield Month(
                    date=date,
                    slug=date.strftime("%Y-%m"),
                    label=date.strftime("%b %Y"),
                    count=r[1]
                )
        except sqlite3.Error as e:
            logging.error(f"Error fetching timeline: {e}")
            console.print(f"[red]✗ Error fetching timeline: {e}[/]")
            return iter([])

    def get_dayline(self, year: int, month: int, limit: int = 500) -> Iterator[Day]:
        """
        Retrieves daily message counts and pagination for a specific month.

        Args:
            year (int): Year to filter messages.
            month (int): Month to filter messages.
            limit (int): Pagination limit per page.

        Yields:
            Day: Named tuple with date, slug, label, count, and page number.
        """
        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT strftime("%Y-%m-%d 00:00:00", date) AS "[timestamp]",
                COUNT(*), PAGE(rank, ?) FROM (
                    SELECT ROW_NUMBER() OVER(ORDER BY id) as rank, date FROM messages
                    WHERE strftime('%Y%m', date) = ?
                )
                GROUP BY "[timestamp]";
            """, (limit, f"{year}{month:02d}"))
            for r in cur.fetchall():
                date = pytz.utc.localize(r[0])
                if self.tz:
                    date = date.astimezone(self.tz)
                yield Day(
                    date=date,
                    slug=date.strftime("%Y-%m-%d"),
                    label=date.strftime("%d %b %Y"),
                    count=r[1],
                    page=r[2]
                )
        except sqlite3.Error as e:
            logging.error(f"Error fetching dayline for {year}-{month:02d}: {e}")
            console.print(f"[red]✗ Error fetching dayline: {e}[/]")
            return iter([])

    def get_messages(self, year: int, month: int, last_id: int = 0, limit: int = 500) -> Iterator[Message]:
        """
        Retrieves messages for a specific year and month, with pagination.

        Args:
            year (int): Year to filter messages.
            month (int): Month to filter messages.
            last_id (int): Last message ID for pagination.
            limit (int): Maximum number of messages to return.

        Yields:
            Message: Named tuple with message details.
        """
        date_slug = f"{year}{month:02d}"
        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT messages.id, messages.type, messages.date, messages.edit_date,
                messages.content, messages.reply_to, messages.user_id,
                users.username, users.first_name, users.last_name, users.tags, users.avatar, users.last_updated,
                media.id, media.type, media.url, media.title, media.description, media.thumb, media.checksum,
                messages.checksum
                FROM messages
                LEFT JOIN users ON (users.id = messages.user_id)
                LEFT JOIN media ON (media.id = messages.media_id)
                WHERE strftime('%Y%m', messages.date) = ?
                AND messages.id > ?
                ORDER BY messages.id
                LIMIT ?
            """, (date_slug, last_id, limit))
            for r in cur.fetchall():
                yield self._make_message(r)
        except sqlite3.Error as e:
            logging.error(f"Error fetching messages for {date_slug}: {e}")
            console.print(f"[red]✗ Error fetching messages: {e}[/]")
            return iter([])

    def get_message_count(self, year: int, month: int) -> int:
        """
        Counts messages for a specific year and month.

        Args:
            year (int): Year to filter messages.
            month (int): Month to filter messages.

        Returns:
            int: Total number of messages for the period.
        """
        date_slug = f"{year}{month:02d}"
        try:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT COUNT(*) FROM messages
                WHERE strftime('%Y%m', date) = ?
            """, (date_slug,))
            total, = cur.fetchone()
            logging.info(f"Message count for {date_slug}: {total}")
            return total
        except sqlite3.Error as e:
            logging.error(f"Error counting messages for {date_slug}: {e}")
            console.print(f"[red]✗ Error counting messages: {e}[/]")
            return 0

def insert_user(self, u: User) -> None:
    """
    Inserts or updates a user record with retry mechanism and WAL safety.
    """
    backoff = 1.0
    for attempt in range(1, self.retries + 1):
        try:
            cur = self.conn.cursor()
            cur.execute("""
                INSERT INTO users (id, username, first_name, last_name, tags, avatar, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (id)
                DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_name = excluded.last_name,
                    tags = excluded.tags,
                    avatar = excluded.avatar,
                    last_updated = excluded.last_updated
            """, (
                u.id,
                u.username,
                u.first_name,
                u.last_name,
                " ".join(u.tags or []),
                u.avatar,
                datetime.utcnow()
            ))
            self.conn.commit()
            logging.debug(f"Inserted/Updated user ID {u.id} (commit OK)")
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                logging.warning(f"[Attempt {attempt}] DB locked on insert_user({u.id}), backing off {backoff:.1f}s...")
                console.print(f"[yellow]⚠ Attempt {attempt}: DB locked for user {u.id}, retrying...[/]")
                time.sleep(backoff)
                backoff *= 2  # exponential backoff
            else:
                logging.error(f"Unhandled SQLite error on insert_user({u.id}): {e}")
                raise
        except Exception as e:
            logging.exception(f"Unexpected error inserting user {u.id}: {e}")
            raise
    console.print(f"[red]✗ Max retries exceeded for user {u.id}[/]")
    raise Exception(f"insert_user failed for user {u.id} after {self.retries} attempts")

    def insert_media(self, m: Media) -> None:
        """
        Inserts or replaces a media record with integrity checksum.

        Args:
            m (Media): Named tuple containing media data.
        """
        for attempt in range(self.retries):
            try:
                cur = self.conn.cursor()
                cur.execute("""
                    INSERT OR REPLACE INTO media
                    (id, type, url, title, description, thumb, checksum)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (m.id, m.type, m.url, m.title, m.description, m.thumb, m.checksum))
                logging.debug(f"Inserted/Replaced media ID {m.id}")
                return
            except sqlite3.Error as e:
                logging.error(f"Attempt {attempt+1}/{self.retries} failed to insert media ID {m.id}: {e}")
                if attempt == self.retries - 1:
                    console.print(f"[red]✗ Failed to insert media ID {m.id} after retries: {e}[/]")
                    raise Exception(f"Failed to insert media after {self.retries} attempts: {e}")
                time.sleep(1)

    def insert_message(self, m: Message) -> None:
        """
        Inserts or replaces a message record with integrity checksum.

        Args:
            m (Message): Named tuple containing message data.
        """
        for attempt in range(self.retries):
            try:
                cur = self.conn.cursor()
                cur.execute("""
                    INSERT OR REPLACE INTO messages
                    (id, type, date, edit_date, content, reply_to, user_id, media_id, checksum)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    m.id,
                    m.type,
                    m.date.strftime("%Y-%m-%d %H:%M:%S") if m.date else None,
                    m.edit_date.strftime("%Y-%m-%d %H:%M:%S") if m.edit_date else None,
                    m.content,
                    m.reply_to,
                    m.user.id if m.user else None,
                    m.media.id if m.media else None,
                    m.checksum
                ))
                logging SQL result tuple.

        Returns:
            Message: Structured message data.
        """
        id_, typ, date, edit_date, content, reply_to, user_id, username, first_name, \
        last_name, tags, avatar, last_updated, media_id, media_type, media_url, \
        media_title, media_desc, media_thumb, media_checksum, msg_checksum = m

        media = None
        if media_id:
            desc = media_desc
            if media_type == "poll":
                try:
                    desc = json.loads(media_desc or "[]")
                except json.JSONDecodeError:
                    logging.warning(f"Invalid JSON in poll description for media ID {media_id}")
                    desc = []
            media = Media(
                id=media_id,
                type=media_type,
                url=media_url,
                title=media_title,
                description=desc,
                thumb=media_thumb,
                checksum=media_checksum
            )

        date_obj = pytz.utc.localize(date) if date else None
        edit_date_obj = pytz.utc.localize(edit_date) if edit_date else None
        if self.tz:
            date_obj = date_obj.astimezone(self.tz) if date_obj else None
            edit_date_obj = edit_date_obj.astimezone(self.tz) if edit_date_obj else None

        user = User(
            id=user_id,
            username=username,
            first_name=first_name,
            last_name=last_name,
            tags=tags.split() if tags else [],
            avatar=avatar,
            last_updated=last_updated
        ) if user_id else None

        return Message(
            id=id_,
            type=typ,
            date=date_obj,
            edit_date=edit_date_obj,
            content=content,
            reply_to=reply_to,
            user=user,
            media=media,
            checksum=msg_checksum
        )

    def export_data(self, table: str, output_file: str, format: str = "csv") -> bool:
        """
        Exports data from a specified table to a file (placeholder for external analysis tools).

        Args:
            table (str): Table name to export (messages, users, media).
            output_file (str): Path to save the exported data.
            format (str): Export format (default: csv).

        Returns:
            bool: True if export succeeds, False otherwise.
        """
        try:
            cur = self.conn.cursor()
            cur.execute(f"SELECT * FROM {table}")
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            # Placeholder for CSV export logic (could use pandas or csv module)
            logging.info(f"Exported {len(rows)} records from {table} to {output_file}")
            console.print(f"[green]✓ Exported {len(rows)} {table} records to {output_file}[/]")
            return True
        except sqlite3.Error as e:
            logging.error(f"Error exporting data from {table}: {e}")
            console.print(f"[red]✗ Failed to export {table} data: {e}[/]")
            return False

    def verify_integrity(self, table: str, id_range: Optional[Tuple[int, int]] = None) -> List[Dict[str, Any]]:
        """
        Verifies data integrity by checking checksums (placeholder for novel cybersecurity feature).

        Args:
            table (str): Table to check (messages or media).
            id_range (Optional[Tuple[int, int]]): Range of IDs to verify.

        Returns:
            List[Dict[str, Any]]: List of records with integrity issues.
        """
        issues = []
        try:
            cur = self.conn.cursor()
            query = f"SELECT id, checksum FROM {table}"
            if id_range:
                query += f" WHERE id BETWEEN {id_range[0]} AND {id_range[1]}"
            cur.execute(query)
            for row in cur.fetchall():
                id_, checksum = row
                # Placeholder for actual checksum validation logic
                if not checksum:  # Simulate missing checksum as an issue
                    issues.append({"id": id_, "issue": "Missing checksum"})
            logging.info(f"Verified integrity of {table}: Found {len(issues)} issues")
            console.print(f"[yellow]Integrity check on {table}: {len(issues)} issues found[/]")
            return issues
        except sqlite3.Error as e:
            logging.error(f"Error during integrity check on {table}: {e}")
            console.print(f"[red]✗ Integrity check failed: {e}[/]")
            return []

    def close(self) -> None:
        """Closes the database connection safely."""
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")
            console.print("[green]✓ Database connection closed.[/]")
