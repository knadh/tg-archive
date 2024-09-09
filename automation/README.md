## How it works

tg-archive uses the [Telethon](https://github.com/LonamiWebs/Telethon) Telegram API client to periodically sync messages from ALL your non-archived groups to a local SQLite database (file), downloading only new messages since the last sync. It then generates a static archive website of messages to be published anywhere.

## Features

-   📁 Extra feature: scan all your non-archived groups and archive them.
-   🔄 Periodically sync Telegram group messages to a local DB.
-   🖼️ Download user avatars locally.
-   📥 Download and embed media (files, documents, photos).
-   📊 Renders poll results.
-   😀 Use emoji alternatives in place of stickers.
-   📝 Single file Jinja HTML template for generating the static site.
-   📅 Year / Month / Day indexes with deep linking across pages.
-   🔗 "In reply to" on replies with links to parent messages across pages.
-   📰 RSS / Atom feed of recent messages.

## Install

-   Get [Telegram API credentials](https://my.telegram.org/auth?to=apps). Normal user account API and not the Bot API.
    -   If this page produces an alert stating only "ERROR", disconnect from any proxy/vpn and try again in a different browser.
-   Copy `example.env` to .env
-   Create the directionr `session` in same compose dir.
-   Copy any generated session generated to `./session/session.session`. If you don't have a session, just run the container and the enter it and run tg-archive to generate a new one.

    -   Inside the container: `python /usr/local/bin/tg-archive --new --path=session`
    -   Ensure that the `session.session` file is generated in the as `/session/session.session` directory. This file contains the API authorization for your account.
    -   Then, after 30 seconds the script should auto-start.

### Customization

Edit the generated `./data/*/template.html` and static assets in the `./data/*/static` directory to customize the site group.

### Note

-   The sync can be stopped (Ctrl+C) any time to be resumed later.
-   Setup a cron job to periodically sync messages and re-publish the archive.
-   Downloading large media files and long message history from large groups continuously may run into Telegram API's rate limits. Watch the debug output.

Licensed under the MIT license.
