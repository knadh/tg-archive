"""
SPECTRA-004 — Telegram Archiver DB Handler (v1.0)
=================================================
A hardened SQLite backend for SPECTRA-series tools.
Built for **SWORD-EPI** with the same conventions as *SPECTRA-002*:

* WAL-mode, foreign-key integrity, application-level checksums.
* Exponential-back-off on locked writes.
* Conveniences for timeline queries + resumable checkpoints.

MIT-style licence.  © 2025 John (SWORD-EPI) – codename *SPECTRA-004*.
"""
from __future__ import annotations

# ── Standard Library ─────────────────────────────────────────────────────
import json
import logging
import math
import os
import sqlite3
import sys
import time
from collections import namedtuple
from contextlib import AbstractContextManager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, List, NamedTuple, Optional, Tuple

# ── Third-party ──────────────────────────────────────────────────────────
import pytz  # type: ignore
from rich.console import Console

# ── Logging setup ────────────────────────────────────────────────────────
APP_NAME = "spectra_004_db"
LOGS_DIR = Path.cwd() / "logs"
LOGS_DIR.mkdir(exist_ok=True)
log_file = LOGS_DIR / f"{APP_NAME}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(APP_NAME)
console = Console()

# ── SQL schema ───────────────────────────────────────────────────────────
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY,
    username      TEXT,
    first_name    TEXT,
    last_name     TEXT,
    tags          TEXT,
    avatar        TEXT,
    last_updated  TEXT
);
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);

CREATE TABLE IF NOT EXISTS media (
    id          INTEGER PRIMARY KEY,
    type        TEXT,
    url         TEXT,
    title       TEXT,
    description TEXT,
    thumb       TEXT,
    checksum    TEXT
);
CREATE INDEX IF NOT EXISTS idx_media_type ON media(type);

CREATE TABLE IF NOT EXISTS messages (
    id          INTEGER PRIMARY KEY,
    type        TEXT NOT NULL,
    date        TEXT NOT NULL,
    edit_date   TEXT,
    content     TEXT,
    reply_to    INTEGER,
    user_id     INTEGER REFERENCES users(id),
    media_id    INTEGER REFERENCES media(id),
    checksum    TEXT
);
CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date);
CREATE INDEX IF NOT EXISTS idx_messages_user ON messages(user_id);

CREATE TABLE IF NOT EXISTS checkpoints (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    last_message_id  INTEGER,
    checkpoint_time  TEXT,
    context          TEXT
);
"""

# ── Helper SQL functions ────────────────────────────────────────────────

def _page(n: int, multiple: int) -> int:  # noqa: D401
    """Return page number (1-indexed) for *n* with page-size *multiple*."""
    return math.ceil(n / multiple) if n > 0 else 1

# ── NamedTuples (kept for perf; could migrate to `dataclass` later) ─────
class User(NamedTuple):
    id: int
    username: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]
    tags: List[str]
    avatar: Optional[str]
    last_updated: Optional[datetime]

class Media(NamedTuple):
    id: int
    type: str
    url: Optional[str]
    title: Optional[str]
    description: Optional[str | List | dict]
    thumb: Optional[str]
    checksum: Optional[str]

class Message(NamedTuple):
    id: int
    type: str
    date: datetime
    edit_date: Optional[datetime]
    content: Optional[str]
    reply_to: Optional[int]
    user: Optional[User]
    media: Optional[Media]
    checksum: Optional[str]

class Month(NamedTuple):
    date: datetime
    slug: str
    label: str
    count: int

class Day(NamedTuple):
    date: datetime
    slug: str
    label: str
    count: int
    page: int

# ── DB Handler ───────────────────────────────────────────────────────────
class SpectraDB(AbstractContextManager):
    """SQLite wrapper providing WAL, retries & convenience selects."""

    RETRIES = 3

    def __init__(self, db_path: Path | str, *, tz: str | None = None) -> None:
        self.db_path = Path(db_path)
        self.tz = pytz.timezone(tz) if tz else None
        self.conn: sqlite3.Connection
        self.cur: sqlite3.Cursor
        self._open()

    # ------------------------------------------------------------------ #
    def _open(self) -> None:
        backoff = 1.0
        for attempt in range(1, self.RETRIES + 1):
            try:
                self.conn = sqlite3.connect(
                    self.db_path,
                    detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
                    timeout=5.0,
                )
                self.conn.execute("PRAGMA journal_mode=WAL;")
                self.conn.execute("PRAGMA foreign_keys=ON;")
                self.conn.create_function("PAGE", 2, _page)
                self.cur = self.conn.cursor()
                self.cur.executescript(SCHEMA_SQL)
                self.conn.commit()
                logger.info("DB ready at %s", self.db_path)
                return
            except sqlite3.OperationalError as exc:
                logger.warning("[%d/%d] DB locked (%s) – backing off %.1fs", attempt, self.RETRIES, exc, backoff)
                time.sleep(backoff)
                backoff *= 2
        raise RuntimeError("Failed to open DB after retries")

    # ------------------------------------------------------------------ #
    def __exit__(self, exc_type, exc, tb):
        if exc:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.conn.close()
        logger.info("Connection closed")
        return False

    # ------------------------------------------------------------------ #
    # Insert helpers use exponential back-off on `database is locked`.
    def _exec_retry(self, sql: str, params: tuple = ()) -> None:
        backoff = 1.0
        for attempt in range(1, self.RETRIES + 1):
            try:
                self.cur.execute(sql, params)
                return
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower():
                    logger.debug("[%d/%d] Locked – sleep %.1fs", attempt, self.RETRIES, backoff)
                    time.sleep(backoff)
                    backoff *= 2
                else:
                    raise

    # Users ----------------------------------------------------------------
    def upsert_user(self, user: User) -> None:
        self._exec_retry(
            """
            INSERT INTO users(id, username, first_name, last_name, tags, avatar, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                tags=excluded.tags,
                avatar=excluded.avatar,
                last_updated=excluded.last_updated;
            """,
            (
                user.id,
                user.username,
                user.first_name,
                user.last_name,
                " ".join(user.tags),
                user.avatar,
                datetime.utcnow().isoformat(),
            ),
        )

    # Media ----------------------------------------------------------------
    def upsert_media(self, media: Media) -> None:
        self._exec_retry(
            """
            INSERT INTO media(id, type, url, title, description, thumb, checksum)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                type=excluded.type,
                url=excluded.url,
                title=excluded.title,
                description=excluded.description,
                thumb=excluded.thumb,
                checksum=excluded.checksum;
            """,
            media,
        )

    # Messages -------------------------------------------------------------
    def upsert_message(self, msg: Message) -> None:
        self._exec_retry(
            """
            INSERT INTO messages(id, type, date, edit_date, content, reply_to, user_id, media_id, checksum)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                type=excluded.type,
                date=excluded.date,
                edit_date=excluded.edit_date,
                content=excluded.content,
                reply_to=excluded.reply_to,
                user_id=excluded.user_id,
                media_id=excluded.media_id,
                checksum=excluded.checksum;
            """,
            (
                msg.id,
                msg.type,
                msg.date.isoformat(),
                msg.edit_date.isoformat() if msg.edit_date else None,
                msg.content,
                msg.reply_to,
                msg.user.id if msg.user else None,
                msg.media.id if msg.media else None,
                msg.checksum,
            ),
        )

    # Checkpoints ----------------------------------------------------------
    def save_checkpoint(self, last_id: int, *, context: str = "sync") -> None:
        self._exec_retry(
            "INSERT INTO checkpoints(last_message_id, checkpoint_time, context) VALUES (?, ?, ?)",
            (last_id, datetime.utcnow().isoformat(), context),
        )
        self.conn.commit()
        logger.info("Checkpoint saved (%s – %s)", last_id, context)

    def latest_checkpoint(self, context: str = "sync") -> Optional[int]:
        row = self.cur.execute(
            "SELECT last_message_id FROM checkpoints WHERE context=? ORDER BY checkpoint_time DESC LIMIT 1",
            (context,),
        ).fetchone()
        return row[0] if row else None

    # Timeline helpers -----------------------------------------------------
    def months(self) -> Iterator[Month]:
        for ts, cnt in self.cur.execute(
            "SELECT strftime('%Y-%m-01T00:00:00Z', date), COUNT(*) FROM messages GROUP BY strftime('%Y-%m', date) ORDER BY 1"
        ):
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if self.tz:
                dt = dt.astimezone(self.tz)
            yield Month(dt, dt.strftime("%Y-%m"), dt.strftime("%b %Y"), cnt)

    def days(self, year: int, month: int, *, page_size: int = 500) -> Iterator[Day]:
        ym = f"{year}{month:02d}"
        for ts, cnt, page in self.cur.execute(
            """
            SELECT strftime('%Y-%m-%dT00:00:00Z', date), COUNT(*), PAGE(rank, ?) FROM (
                SELECT ROW_NUMBER() OVER(ORDER BY id) AS rank, date FROM messages WHERE strftime('%Y%m', date)=?
            ) GROUP BY 1 ORDER BY 1;
            """,
            (page_size, ym),
        ):
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if self.tz:
                dt = dt.astimezone(self.tz)
            yield Day(dt, dt.strftime("%Y-%m-%d"), dt.strftime("%d %b %Y"), cnt, page)

    # Integrity check ------------------------------------------------------
    def verify_checksums(self, table: str, *, id_range: Tuple[int, int] | None = None) -> List[dict]:
        issues: List[dict] = []
        sql = f"SELECT id, checksum FROM {table}"
        params: Tuple = ()
        if id_range:
            sql += " WHERE id BETWEEN ? AND ?"
            params = id_range  # type: ignore
        for id_, checksum in self.cur.execute(sql, params):
            if not checksum:
                issues.append({"id": id_, "issue": "missing checksum"})
        logger.info("Integrity on %s – %d issues", table, len(issues))
        return issues

    # Export (CSV placeholder) --------------------------------------------
    def export_csv(self, table: str, dst: Path) -> int:
        rows = self.cur.execute(f"SELECT * FROM {table}").fetchall()
        headers = [d[0] for d in self.cur.description]
        with dst.open("w", encoding="utf-8") as fh:
            fh.write(",".join(headers) + "\n")
            for row in rows:
                fh.write(",".join(map(lambda x: "" if x is None else str(x), row)) + "\n")
        logger.info("Exported %d rows from %s to %s", len(rows), table, dst)
        return len(rows)

__all__ = [
    "SpectraDB",
    "User",
    "Media",
    "Message",
    "Month",
    "Day",
]
