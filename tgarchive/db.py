import json
import math
import os
import sqlite3
from collections import namedtuple
from datetime import datetime
import pytz
from typing import Iterator



                     
schema = """
CREATE table messages (
    id INTEGER NOT NULL PRIMARY KEY,
    type TEXT NOT NULL,
    date TIMESTAMP NOT NULL,
    edit_date TIMESTAMP,
    content TEXT,
    reply_to INTEGER,
    from_id INTEGER,
    user_id INTEGER,
    media_id INTEGER,
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(media_id) REFERENCES media(id)
);
##
CREATE table users (
    id INTEGER NOT NULL PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    tags TEXT,
    avatar TEXT
);
##
CREATE table message_fwd_header (
    id INTEGER NOT NULL PRIMARY KEY,
    date TIMESTAMP NOT NULL,
    imported INTEGER,
    from_id_channel_id TEXT,
    from_id_user_id TEXT,
    from_name TEXT,
    channel_post TEXT,
    post_author TEXT,
    saved_from_peer TEXT,
    saved_from_msg_id TEXT,
    psa_type TEXT
);


##
CREATE table media (
    id INTEGER NOT NULL PRIMARY KEY,
    type TEXT,
    url TEXT,
    title TEXT,
    description TEXT,
    thumb TEXT
);
"""

User = namedtuple(
    "User", ["id", "username", "first_name", "last_name", "tags", "avatar"])

Message = namedtuple(
    "Message", ["id", "type", "date", "edit_date", "content", "reply_to","from_id", "user", "media","message_fwd_header"])

Media = namedtuple(
    "Media", ["id", "type", "url", "title", "description", "thumb"])

MessageFwdHeader = namedtuple(
	"MessageFwdHeader", ["date","imported","from_id","from_name","channel_post","post_author","saved_from_peer","saved_from_msg_id","psa_type"])
 
Month = namedtuple("Month", ["date", "slug", "label", "count"])

Day = namedtuple("Day", ["date", "slug", "label", "count", "page"])


def _page(n, multiple):
    return math.ceil(n / multiple)


class DB:
    conn = None
    tz = None

    def __init__(self, dbfile, tz=None):
        # Initialize the SQLite DB. If it's new, create the table schema.
        is_new = not os.path.isfile(dbfile)

        self.conn = sqlite3.Connection(
            dbfile, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)

        # Add the custom PAGE() function to get the page number of a row
        # by its row number and a limit multiple.
        self.conn.create_function("PAGE", 2, _page)

        if tz:
            self.tz = pytz.timezone(tz)

        if is_new:
            for s in schema.split("##"):
                self.conn.cursor().execute(s)
                self.conn.commit()

    def _parse_date(self, d) -> str:
        return datetime.strptime(d, "%Y-%m-%dT%H:%M:%S%z")

    def get_last_message_id(self) -> [int, datetime]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT id, strftime('%Y-%m-%d 00:00:00', date) as "[timestamp]" FROM messages
            ORDER BY id DESC LIMIT 1
        """)
        res = cur.fetchone()
        if not res:
            return 0, None

        id, date = res
        return id, date

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
            date = pytz.utc.localize(r[0])
            if self.tz:
                date = date.astimezone(self.tz)

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
        """, (limit, "{}{:02d}".format(year, month)))

        for r in cur.fetchall():
            date = pytz.utc.localize(r[0])
            if self.tz:
                date = date.astimezone(self.tz)

            yield Day(date=date,
                      slug=date.strftime("%Y-%m-%d"),
                      label=date.strftime("%d %b %Y"),
                      count=r[1],
                      page=r[2])

    def get_messages(self, year, month, last_id=0, limit=500) -> Iterator[Message]:
        date = "{}{:02d}".format(year, month)

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
        date = "{}{:02d}".format(year, month)

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
            """, (u.id, u.username, u.first_name, u.last_name, " ".join(u.tags), u.avatar))

    def insert_media(self, m: Media):
        cur = self.conn.cursor()
        cur.execute("""INSERT OR REPLACE INTO media
            (id, type, url, title, description, thumb)
            VALUES(?, ?, ?, ?, ?, ?)""",
                    (m.id,
                     m.type,
                     m.url,
                     m.title,
                     m.description,
                     m.thumb)
                    )

    def insert_message_fwd_header(self, m: MessageFwdHeader):
        # print(m)
        from_id_channel_id = None
        from_id_user_id = None
        
        if hasattr(m.from_id,'channel_id'):
        	from_id_channel_id = m.from_id.channel_id
        if hasattr(m.from_id,'user_id'):
        	from_id_user_id = m.from_id.user_id
        	
        cur = self.conn.cursor()
        cur.execute("""INSERT OR REPLACE INTO message_fwd_header
            (date, imported, from_id_channel_id,from_id_user_id, from_name, channel_post, post_author, saved_from_peer, saved_from_msg_id, psa_type)
            VALUES(?, ?, ?, ?, ?, ?, ?,?, ?, ?)""",
                    (m.date,
                     m.imported,
                     from_id_channel_id,
                     from_id_user_id,
                     m.from_name,
                     m.channel_post,
                     m.post_author,
                     m.saved_from_peer,
                     m.saved_from_msg_id,
                     m.psa_type)
                    )

    def insert_message(self, m: Message):
        cur = self.conn.cursor()
        cur.execute("""INSERT OR REPLACE INTO messages
            (id, type, date, edit_date, content, from_id, reply_to, user_id, media_id)
            VALUES(?, ?, ?, ?, ?, ?,  ?, ?,? )""",
                    (m.id,
                     m.type,
                     m.date.strftime("%Y-%m-%d %H:%M:%S"),
                     m.edit_date.strftime(
                         "%Y-%m-%d %H:%M:%S") if m.edit_date else None,
                     m.content,
                     m.from_id,
                     m.reply_to,
                     m.user.id,
                     m.media.id if m.media else None)
                    )

    def commit(self):
        """Commit pending writes to the DB."""
        self.conn.commit()

    def _make_message(self, m) -> Message:
        """Makes a Message() object from an SQL result tuple."""
        id, typ, date, edit_date, content, reply_to, \
            user_id, username, first_name, last_name, tags, avatar, \
            media_id, media_type, media_url, media_title, media_description, from_id,  media_thumb = m

        fwd = None
        if fwd_from:
   
            	
            imported = pytz.utc.localize(imported) if imported else None
            fwd = MessageFwdHeader(id=id,
                     imported=imported,
                     from_id=from_id,
                     post_author=post_author,
                     saved_from_peer=saved_from_peer,
                     saved_from_msg_id=saved_from_msg_id,
                     psa_type=psa_type)
        
        md = None
        if media_id:
            desc = media_description
            if media_type == "poll":
                desc = json.loads(media_description)

            md = Media(id=media_id,
                       type=media_type,
                       url=media_url,
                       title=media_title,
                       description=desc,
                       thumb=media_thumb)

        date = pytz.utc.localize(date) if date else None
        edit_date = pytz.utc.localize(edit_date) if edit_date else None

        if self.tz:
            date = date.astimezone(self.tz) if date else None
            edit_date = edit_date.astimezone(self.tz) if edit_date else None

        return Message(id=id,
                       type=typ,
                       date=date,
                       edit_date=edit_date,
                       content=content,
                       reply_to=reply_to,
                       from_id=from_id,
                       user=User(id=user_id,
                                 username=username,
                                 first_name=first_name,
                                 last_name=last_name,
                                 tags=tags,
                                 avatar=avatar),
                       media=md,
                       message_fwd_header=fwd)
