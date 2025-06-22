# SPECTRA Database Schema

This document outlines the schema for the SQLite database used by SPECTRA.

## Table Index

- [`users`](#users)
- [`media`](#media)
- [`messages`](#messages)
- [`checkpoints`](#checkpoints)
- [`account_channel_access`](#account_channel_access)

---

## `users`

Stores information about Telegram users encountered by the archiver.

| Column         | Type    | Constraints         | Description                                      |
|----------------|---------|---------------------|--------------------------------------------------|
| `id`           | INTEGER | PRIMARY KEY         | Unique Telegram User ID.                         |
| `username`     | TEXT    |                     | User's Telegram username (can be NULL).          |
| `first_name`   | TEXT    |                     | User's first name (can be NULL).                 |
| `last_name`    | TEXT    |                     | User's last name (can be NULL).                  |
| `tags`         | TEXT    |                     | Space-separated list of tags (internal use).     |
| `avatar`       | TEXT    |                     | Path or reference to the user's avatar (internal use). |
| `last_updated` | TEXT    |                     | ISO 8601 timestamp of when this record was last updated. |

**Indexes:**
- `idx_users_username` ON `users(username)`

---

## `media`

Stores information about media files (photos, videos, documents, etc.) attached to messages.

| Column        | Type    | Constraints   | Description                                      |
|---------------|---------|---------------|--------------------------------------------------|
| `id`          | INTEGER | PRIMARY KEY   | Unique Media ID (often from Telegram).           |
| `type`        | TEXT    |               | Type of media (e.g., 'photo', 'video', 'document'). |
| `url`         | TEXT    |               | URL of the media, if applicable (e.g., web link). |
| `title`       | TEXT    |               | Title of the media (e.g., document filename).    |
| `description` | TEXT    |               | Description or caption of the media.             |
| `thumb`       | TEXT    |               | Path or reference to a media thumbnail.          |
| `checksum`    | TEXT    |               | Checksum of the downloaded media file (e.g., SHA256). |

**Indexes:**
- `idx_media_type` ON `media(type)`

---

## `messages`

Stores individual messages archived from Telegram channels or groups.

| Column     | Type    | Constraints                      | Description                                      |
|------------|---------|----------------------------------|--------------------------------------------------|
| `id`       | INTEGER | PRIMARY KEY                      | Unique Telegram Message ID for the channel/group. |
| `type`     | TEXT    | NOT NULL                         | Type of message (e.g., 'message', 'service').    |
| `date`     | TEXT    | NOT NULL                         | ISO 8601 timestamp of when the message was sent. |
| `edit_date`| TEXT    |                                  | ISO 8601 timestamp of when the message was last edited (can be NULL). |
| `content`  | TEXT    |                                  | Text content of the message.                     |
| `reply_to` | INTEGER |                                  | Message ID this message is a reply to (can be NULL). |
| `user_id`  | INTEGER | REFERENCES `users(id)`           | Foreign key to the `users` table (sender).       |
| `media_id` | INTEGER | REFERENCES `media(id)`           | Foreign key to the `media` table (attached media).|
| `checksum` | TEXT    |                                  | Checksum of the message content/metadata (internal use). |

**Indexes:**
- `idx_messages_date` ON `messages(date)`
- `idx_messages_user` ON `messages(user_id)`

---

## `checkpoints`

Stores checkpoint information for resumable archiving operations.

| Column            | Type    | Constraints                 | Description                                      |
|-------------------|---------|-----------------------------|--------------------------------------------------|
| `id`              | INTEGER | PRIMARY KEY AUTOINCREMENT   | Unique identifier for the checkpoint record.     |
| `last_message_id` | INTEGER |                             | The ID of the last successfully archived message. |
| `checkpoint_time` | TEXT    |                             | ISO 8601 timestamp of when the checkpoint was saved. |
| `context`         | TEXT    |                             | Context for the checkpoint (e.g., 'sync', specific channel ID). |

---

## `account_channel_access`

Stores information about which Telegram channels or groups are accessible by which configured accounts. This helps in understanding the reach of each account and potentially for routing tasks.

| Column                 | Type   | Constraints                         | Description                                      |
|------------------------|--------|-------------------------------------|--------------------------------------------------|
| `account_phone_number` | TEXT   | NOT NULL                            | Phone number of the Telegram account.            |
| `channel_id`           | BIGINT | NOT NULL                            | Unique Telegram ID of the channel or group.      |
| `channel_name`         | TEXT   |                                     | Name/title of the channel or group (can be NULL).|
| `access_hash`          | BIGINT |                                     | Access hash for the channel/group, if applicable (can be NULL). |
| `last_seen`            | TEXT   |                                     | ISO 8601 timestamp of when this channel was last seen/updated for this account. |
|                        |        | PRIMARY KEY (`account_phone_number`, `channel_id`) |                                          |
