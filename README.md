![favicon](https://user-images.githubusercontent.com/547147/111869334-eb48f100-89a4-11eb-9c0c-bc74cdee197a.png)

# tg-archive

[English](README.md) | [中文说明](README_CN.md)

**tg-archive** is a tool for exporting Telegram group and channel chats into static websites, preserving chat history like mailing list archives.

## Preview
The [@fossunited](https://tg.fossunited.org) Telegram group archive.

![image](https://user-images.githubusercontent.com/547147/111869398-44188980-89a5-11eb-936f-01d98276ba6a.png)

## How it works
tg-archive uses the [Telethon](https://github.com/LonamiWebs/Telethon) Telegram API client to periodically sync messages from a group or channel into a local SQLite database, downloading only new messages since the last sync. It then generates a fast, static archive website of messages to be published anywhere.

## Features
- **Rich text formatting**: Preserves Telegram message formatting (bold, italic, underline, strikethrough, inline code, syntax-highlighted code blocks, blockquotes, spoilers, and hyperlinks) using UTF-16 alignment.
- **Telegram Instant View (IV) unwrapping**: Automatically unpackages protected channel proxy URLs (`https://t.me/iv?rhash=...&url=...`) back into their original Telegraph or web links.
- **Fast incremental site builds**: Rebuilding only syncs updated content without deleting and re-copying downloaded media files.
- **Optimized SQLite database**: Uses indexed queries and Python 3.12+ compliant ISO 8601 timestamps (zero deprecation warnings).
- **Modern Python dependencies**: Powered by standard library `zoneinfo` and `mimetypes`. Eliminates legacy dependencies (`pytz`, `python-magic`, `six`, `python-dateutil`). Works out-of-the-box on Windows, Linux, and macOS.
- **Media & Avatar archiving**: Downloads user avatars, photos, documents, and renders poll results locally.
- **Navigation & Indexing**: Year / Month / Day timeline indexes with deep linking across pages.
- **Threading**: "In reply to" links to parent messages across arbitrary paginated pages.
- **Syndication**: RSS and Atom feeds for recent messages.
- **Modern packaging**: PEP 621 compliant `pyproject.toml` supporting Python 3.9 through 3.14+.

## Install

### Prerequisites
- Python 3.9+ (tested up to Python 3.14)
- Telegram API credentials from [my.telegram.org](https://my.telegram.org/auth?to=apps) (User account API, not Bot API).

### Installation
```bash
git clone https://github.com/mhdy2233/tg-archive.git
cd tg-archive

# Install dependencies
pip install .
```

## Usage

### 1. Initialize a new archive site
```bash
tg-archive --new --path=mysite
cd mysite
```
Edit `config.yaml` with your `api_id`, `api_hash`, and target `group` (channel/group username or ID).

### 2. Sync messages from Telegram
```bash
tg-archive --sync
```
*Tip*: You can also invoke it directly via `python -m tgarchive --sync`.
- First-time connection will prompt for your phone number and verification code (and 2FA password if enabled).
- Authentication is saved locally into `session.session` (do **not** share this file publicly).
- To sync only recent messages (e.g. from a specific message ID):
  ```bash
  tg-archive --sync --from-id 3876
  ```

### 3. Build the static site
```bash
tg-archive --build
```
Outputs the static HTML site into the `site/` directory, ready to deploy to GitHub Pages, Cloudflare Pages, Nginx, or any web server.

## Customization
- Edit `template.html` to customize the Jinja layout.
- Customize assets in `static/style.css` (custom code block, quote, and spoiler styles included).

## License
Licensed under the MIT license.
