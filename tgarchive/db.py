import json
import logging
import math
import os
import re
import sqlite3
from collections import namedtuple
from datetime import datetime, timezone
from typing import Iterator
from urllib.parse import urlparse, parse_qs, unquote
from zoneinfo import ZoneInfo
from markupsafe import Markup

_IV_URL_RE = re.compile(r'https?://(?:t\.me|telegram\.me)/iv\?[^\s<>"\')]+')


def clean_iv_url(url: str | None) -> str | None:
    """Unwrap Telegram Instant View proxy links to their target URL."""
    if not url or "t.me/iv?" not in url:
        return url
    try:
        parsed = urlparse(url)
        if parsed.netloc in ("t.me", "telegram.me") and parsed.path == "/iv":
            qs = parse_qs(parsed.query)
            if "url" in qs and qs["url"]:
                return unquote(qs["url"][0])
    except Exception:
        pass
    return url


def clean_iv_text(text: str | None) -> str | None:
    """Unwrap any Telegram Instant View proxy links embedded in message text."""
    if not text or "t.me/iv?" not in text:
        return text

    def _sub(m):
        return clean_iv_url(m.group(0))

    return _IV_URL_RE.sub(_sub, text)

schema = """
CREATE table IF NOT EXISTS messages (
    id INTEGER NOT NULL PRIMARY KEY,
    type TEXT NOT NULL,
    date TIMESTAMP NOT NULL,
    edit_date TIMESTAMP,
    content TEXT,
    reply_to INTEGER,
    user_id INTEGER,
    media_id INTEGER,
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(media_id) REFERENCES media(id)
);
##
CREATE table IF NOT EXISTS users (
    id INTEGER NOT NULL PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    tags TEXT,
    avatar TEXT
);
##
CREATE table IF NOT EXISTS media (
    id INTEGER NOT NULL PRIMARY KEY,
    type TEXT,
    url TEXT,
    title TEXT,
    description TEXT,
    thumb TEXT
);
##
CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date);
##
CREATE INDEX IF NOT EXISTS idx_messages_reply_to ON messages(reply_to);
##
CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(user_id);
"""

User = namedtuple(
    "User", ["id", "username", "first_name", "last_name", "tags", "avatar"])

Message = namedtuple(
    "Message", ["id", "type", "date", "edit_date", "content", "reply_to", "user", "media"])

Media = namedtuple(
    "Media", ["id", "type", "url", "title", "description", "thumb"])

Month = namedtuple("Month", ["date", "slug", "label", "count"])

Day = namedtuple("Day", ["date", "slug", "label", "count", "page"])


def _page(n, multiple):
    return math.ceil(n / multiple)


class DB:
    conn = None
    tz = None

    def __init__(self, dbfile, tz=None):
        self.conn = sqlite3.connect(dbfile)

        # Add the custom PAGE() function to get the page number of a row
        # by its row number and a limit multiple.
        self.conn.create_function("PAGE", 2, _page)

        if tz:
            if tz.upper() == "UTC":
                self.tz = timezone.utc
            else:
                try:
                    self.tz = ZoneInfo(tz)
                except Exception as e:
                    logging.warning(f"could not load timezone '{tz}': {e}. Falling back to UTC.")
                    self.tz = timezone.utc

        # Run schema / ensure tables and indexes exist
        for s in schema.strip().split("##"):
            stmt = s.strip()
            if stmt:
                self.conn.execute(stmt)
        self.conn.commit()

    def _to_local_tz(self, dt: datetime | None) -> datetime | None:
        if not dt:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if self.tz:
            return dt.astimezone(self.tz)
        return dt

    def _parse_date(self, d: str) -> datetime:
        return datetime.fromisoformat(d)

    def get_last_message_id(self) -> tuple[int, datetime | None]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT id, strftime('%Y-%m-%d 00:00:00', date) as "[timestamp]" FROM messages
            ORDER BY id DESC LIMIT 1
        """)
        res = cur.fetchone()
        if not res:
            return 0, None

        mid, date_str = res
        dt = self._to_local_tz(datetime.fromisoformat(date_str)) if date_str else None
        return mid, dt

    def get_timeline(self) -> Iterator[Month]:
        """
        Get the list of all unique yyyy-mm month groups and
        the corresponding message counts per period in chronological order.
        """
        cur = self.conn.cursor()
        cur.execute("""
            SELECT strftime('%Y-%m-%d 00:00:00', date) as "[timestamp]",
            COUNT(*) FROM messages AS count
            GROUP BY strftime('%Y-%m', date) ORDER BY date
        """)

        for r in cur.fetchall():
            date = self._to_local_tz(datetime.fromisoformat(r[0]))

            yield Month(date=date,
                        slug=date.strftime("%Y-%m"),
                        label=date.strftime("%b %Y"),
                        count=r[1])

    def get_dayline(self, year, month, limit=500) -> Iterator[Day]:
        """
        Get the list of all unique yyyy-mm-dd days corresponding
        message counts and the page number of the first occurrence of 
        the date in the pool of messages for the whole month.
        """
        cur = self.conn.cursor()
        cur.execute("""
            SELECT strftime("%Y-%m-%d 00:00:00", date) AS "[timestamp]",
            COUNT(*), PAGE(rank, ?) FROM (
                SELECT ROW_NUMBER() OVER() as rank, date FROM messages
                WHERE strftime('%Y%m', date) = ? ORDER BY id
            )
            GROUP BY "[timestamp]";
        """, (limit, f"{year}{month:02d}"))

        for r in cur.fetchall():
            date = self._to_local_tz(datetime.fromisoformat(r[0]))

            yield Day(date=date,
                      slug=date.strftime("%Y-%m-%d"),
                      label=date.strftime("%d %b %Y"),
                      count=r[1],
                      page=r[2])

    def get_messages(self, year, month, last_id=0, limit=500) -> Iterator[Message]:
        date = f"{year}{month:02d}"

        cur = self.conn.cursor()
        cur.execute("""
            SELECT messages.id, messages.type, messages.date, messages.edit_date,
            messages.content, messages.reply_to, messages.user_id,
            users.username, users.first_name, users.last_name, users.tags, users.avatar,
            media.id, media.type, media.url, media.title, media.description, media.thumb
            FROM messages
            LEFT JOIN users ON (users.id = messages.user_id)
            LEFT JOIN media ON (media.id = messages.media_id)
            WHERE strftime('%Y%m', date) = ?
            AND messages.id > ? ORDER by messages.id LIMIT ?
            """, (date, last_id, limit))

        for r in cur.fetchall():
            yield self._make_message(r)

    def get_message_count(self, year, month) -> int:
        date = f"{year}{month:02d}"

        cur = self.conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM messages WHERE strftime('%Y%m', date) = ?
            """, (date,))

        total, = cur.fetchone()
        return total

    def insert_user(self, u: User):
        """Insert a user and if they exist, update the fields."""
        cur = self.conn.cursor()
        cur.execute("""INSERT INTO users (id, username, first_name, last_name, tags, avatar)
            VALUES(?, ?, ?, ?, ?, ?) ON CONFLICT (id)
            DO UPDATE SET username=excluded.username, first_name=excluded.first_name,
                last_name=excluded.last_name, tags=excluded.tags, avatar=excluded.avatar
            """, (u.id, u.username, u.first_name, u.last_name, " ".join(u.tags) if u.tags else "", u.avatar))

    def insert_media(self, m: Media):
        cur = self.conn.cursor()
        clean_url = clean_iv_url(m.url) if m.url else None
        cur.execute("""INSERT OR REPLACE INTO media
            (id, type, url, title, description, thumb)
            VALUES(?, ?, ?, ?, ?, ?)""",
                    (m.id,
                     m.type,
                     clean_url,
                     m.title,
                     m.description,
                     m.thumb)
                    )

    def insert_message(self, m: Message):
        cur = self.conn.cursor()
        clean_content = clean_iv_text(m.content) if m.content else None
        cur.execute("""INSERT OR REPLACE INTO messages
            (id, type, date, edit_date, content, reply_to, user_id, media_id)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)""",
                    (m.id,
                     m.type,
                     m.date.strftime("%Y-%m-%d %H:%M:%S"),
                     m.edit_date.strftime(
                         "%Y-%m-%d %H:%M:%S") if m.edit_date else None,
                     clean_content,
                     m.reply_to,
                     m.user.id,
                     m.media.id if m.media else None)
                    )

    def commit(self):
        """Commit pending writes to the DB."""
        self.conn.commit()

    def close(self):
        """Close the DB connection."""
        if self.conn:
            self.conn.close()
    def _make_message(self, m) -> Message:
        """Makes a Message() object from an SQL result tuple."""
        mid, typ, date, edit_date, content, reply_to, \
            user_id, username, first_name, last_name, tags, avatar, \
            media_id, media_type, media_url, media_title, media_description, media_thumb = m

        md = None
        if media_id:
            desc = media_description
            if media_type == "poll":
                desc = json.loads(media_description)

            clean_url = clean_iv_url(media_url) if media_url else None
            md = Media(id=media_id,
                       type=media_type,
                       url=clean_url,
                       title=media_title,
                       description=desc,
                       thumb=media_thumb)

        if isinstance(date, str):
            date = datetime.fromisoformat(date)
        if isinstance(edit_date, str):
            edit_date = datetime.fromisoformat(edit_date)

        date = self._to_local_tz(date)
        edit_date = self._to_local_tz(edit_date)

        tag_list = tags.split() if isinstance(tags, str) else (tags or [])

        clean_content = Markup(clean_iv_text(content)) if content else None
        return Message(id=mid,
                       type=typ,
                       date=date,
                       edit_date=edit_date,
                       content=clean_content,
                       reply_to=reply_to,
                       user=User(id=user_id,
                                 username=username,
                                 first_name=first_name,
                                 last_name=last_name,
                                 tags=tag_list,
                                 avatar=avatar),
                       media=md)
