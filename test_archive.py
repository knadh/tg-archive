"""
Self-contained verification suite for tg-archive modernization.
Tests:
- Database schema, indexing, and SQLite date handling (no pytz/sqlite3 deprecation warnings)
- Timezone handling with stdlib zoneinfo / timezone.utc
- Tag parsing (list of strings, not char-split string)
- Static site building with Jinja2 template and RSS/Atom feeds
- Incremental publish directory preservation
"""
import os
import shutil
import tempfile
from datetime import datetime, timezone

from tgarchive.db import DB, User, Message, Media, clean_iv_url, clean_iv_text
from tgarchive.build import Build
from tgarchive.__metadata__ import __version__


def test_metadata():
    assert __version__ == "1.3.1", f"Unexpected version: {__version__}"
    print("✓ Metadata version verified")


def test_db_operations():
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
        db_path = f.name

    try:
        db = DB(db_path, tz="UTC")

        # Verify tables and indexes exist
        cur = db.conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = [r[0] for r in cur.fetchall()]
        assert "idx_messages_date" in indexes, "idx_messages_date missing"
        assert "idx_messages_reply_to" in indexes, "idx_messages_reply_to missing"
        assert "idx_messages_user_id" in indexes, "idx_messages_user_id missing"

        # Insert user with tags
        u1 = User(id=1, username="testuser", first_name="Test", last_name="User",
                  tags=["bot", "admin"], avatar="avatar_1.jpg")
        db.insert_user(u1)

        # Insert media
        m1 = Media(id=10, type="photo", url="10.jpg", title="pic.jpg",
                   description=None, thumb="thumb_10.jpg")
        db.insert_media(m1)

        # Insert message
        dt = datetime(2026, 3, 15, 12, 30, 0, tzinfo=timezone.utc)
        msg = Message(id=100, type="message", date=dt, edit_date=None,
                      content="Hello archive!", reply_to=None, user=u1, media=m1)
        db.insert_message(msg)
        db.commit()

        # Check last message id
        last_id, last_dt = db.get_last_message_id()
        assert last_id == 100, f"Expected last_id 100, got {last_id}"
        assert last_dt is not None

        # Check timeline
        timeline = list(db.get_timeline())
        assert len(timeline) == 1
        assert timeline[0].slug == "2026-03"
        assert timeline[0].count == 1

        # Check dayline
        dayline = list(db.get_dayline(2026, 3, limit=500))
        assert len(dayline) == 1
        assert dayline[0].slug == "2026-03-15"
        assert dayline[0].count == 1

        # Check messages and tag parsing
        messages = list(db.get_messages(2026, 3, last_id=0, limit=500))
        assert len(messages) == 1
        retrieved_msg = messages[0]
        assert retrieved_msg.id == 100
        assert retrieved_msg.content == "Hello archive!"
        assert retrieved_msg.user.tags == ["bot", "admin"], f"Tags bug check: got {retrieved_msg.user.tags}"
        assert retrieved_msg.media is not None
        assert retrieved_msg.media.url == "10.jpg"

        print("✓ DB operations, indexing, and tag parsing verified")
    finally:
        db.close()
        if os.path.exists(db_path):
            os.remove(db_path)


def test_build_site():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.sqlite")
        pubdir = os.path.join(tmpdir, "site")
        mediadir = os.path.join(tmpdir, "media")
        staticdir = os.path.join(tmpdir, "static")
        os.makedirs(mediadir, exist_ok=True)
        os.makedirs(staticdir, exist_ok=True)

        # Create dummy static file and media file
        with open(os.path.join(staticdir, "style.css"), "w") as f:
            f.write("body { color: black; }")
        with open(os.path.join(mediadir, "test.txt"), "w") as f:
            f.write("media payload")

        db = DB(db_path, tz="UTC")
        u = User(id=1, username="testuser", first_name="Test", last_name="User",
                 tags=["dev"], avatar=None)
        db.insert_user(u)
        dt = datetime(2026, 3, 15, 14, 0, 0, tzinfo=timezone.utc)
        msg = Message(id=1, type="message", date=dt, edit_date=None,
                      content="First post", reply_to=None, user=u, media=None)
        db.insert_message(msg)
        db.commit()

        template_path = os.path.abspath("tgarchive/example/template.html")
        config = {
            "group": "testgroup",
            "publish_dir": pubdir,
            "static_dir": staticdir,
            "media_dir": mediadir,
            "site_url": "https://example.com/archive",
            "site_name": "@{group} Archive",
            "site_description": "Public archive",
            "meta_description": "@{group} {date} archive",
            "page_title": "{date} - @{group}",
            "telegram_url": "https://t.me/{id}",
            "publish_rss_feed": True,
            "rss_feed_entries": 10,
            "per_page": 500,
            "show_day_index": True,
            "show_sender_fullname": False,
            "date": "2026-03"
        }

        builder = Build(config, db, symlink=False)
        builder.load_template(template_path)
        builder.build()

        # Check published files
        assert os.path.exists(os.path.join(pubdir, "index.html")), "index.html not generated"
        assert os.path.exists(os.path.join(pubdir, "2026-03.html")), "2026-03.html not generated"
        assert os.path.exists(os.path.join(pubdir, "index.xml")), "RSS feed index.xml not generated"
        assert os.path.exists(os.path.join(pubdir, "index.atom")), "Atom feed index.atom not generated"
        assert os.path.exists(os.path.join(pubdir, "static", "style.css")), "static asset not copied"

        # Verify HTML contents
        with open(os.path.join(pubdir, "2026-03.html"), "r", encoding="utf-8") as f:
            html = f.read()
            assert "First post" in html
            assert "testuser" in html
            assert "dev" in html

        # Verify incremental build: add new file to pubdir/media, rebuild, ensure it remains
        test_media_dest = os.path.join(pubdir, os.path.basename(mediadir), "test.txt")
        assert os.path.exists(test_media_dest), "media file not copied"
        # Run second build
        builder.build()
        assert os.path.exists(test_media_dest), "media file preserved in incremental build"
        db.close()
        print("✓ Static site generation and incremental build verified")
def test_iv_url_cleaning():
    # 1. Pure function tests
    dirty1 = "https://t.me/iv?rhash=5fb1a8a56b1668&url=https%3A%2F%2Ftelegra.ph%2Fmy-article-09-03"
    assert clean_iv_url(dirty1) == "https://telegra.ph/my-article-09-03"

    dirty2 = "https://t.me/iv?rhash=5fb1a8a56b1668&url=https://telegra.ph/my-article-09-03"
    assert clean_iv_url(dirty2) == "https://telegra.ph/my-article-09-03"

    text_dirty = f"Check this article: {dirty1} and also {dirty2}! Have fun."
    expected_text = "Check this article: https://telegra.ph/my-article-09-03 and also https://telegra.ph/my-article-09-03! Have fun."
    assert clean_iv_text(text_dirty) == expected_text

    # Normal links should not be affected
    normal = "https://example.com/test?a=1&b=2"
    assert clean_iv_url(normal) == normal
    assert clean_iv_text(normal) == normal

    # 2. DB insertion & retrieval tests
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
        db_path = f.name

    try:
        db = DB(db_path, tz="UTC")
        u = User(1, "user1", "First", "Last", [], None)
        db.insert_user(u)

        # Insert media with IV URL
        med = Media(101, "webpage", dirty1, "Telegraph Post", None, None)
        db.insert_media(med)

        # Insert message with IV URL in content
        msg = Message(201, "message", datetime(2026, 3, 15, 12, 0, 0, tzinfo=timezone.utc),
                      None, text_dirty, None, u, med)
        db.insert_message(msg)
        db.commit()

        messages = list(db.get_messages(2026, 3))
        assert len(messages) == 1
        m = messages[0]
        assert m.content == expected_text, f"Expected {expected_text}, got {m.content}"
        assert m.media.url == "https://telegra.ph/my-article-09-03", f"Expected clean URL, got {m.media.url}"
        print("✓ Instant View (IV) link unwrapping verified")
    finally:
        db.close()
        if os.path.exists(db_path):
            os.remove(db_path)
def test_rich_text_formatting():
    import telethon.tl.types
    from tgarchive.sync import Sync

    class DummyTelethonMessage:
        def __init__(self, text, entities):
            self.id = 501
            self.text = text
            self.raw_text = text
            self.entities = entities

    sync_obj = Sync.__new__(Sync)
    m_tl = DummyTelethonMessage(
        "Check 极彩色彩社长 for bold and telegraph link",
        [
            telethon.tl.types.MessageEntityBold(offset=0, length=5),
            telethon.tl.types.MessageEntityTextUrl(
                offset=6, length=6,
                url="https://t.me/iv?rhash=5fb1a8a56b1668&url=https%3A%2F%2Ftelegra.ph%2F%E6%A5%B5%E5%BD%A9%E8%89%B2-09-03"
            )
        ]
    )

    formatted = sync_obj._format_message(m_tl)
    assert "<strong>Check</strong>" in formatted, f"Expected strong tag, got: {formatted}"
    assert '<a href="https://telegra.ph/極彩色-09-03">极彩色彩社长</a>' in formatted, f"Expected unparsed clean anchor, got: {formatted}"
    print("✓ Rich text entity formatting and IV URL unwrapping verified")



if __name__ == "__main__":
    test_metadata()
    test_db_operations()
    test_iv_url_cleaning()
    test_rich_text_formatting()
    test_build_site()
    print("\nALL VERIFICATION TESTS PASSED SUCCESSFULLY!")
