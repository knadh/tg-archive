# SPECTRA

![SPECTRA logo](https://user-images.githubusercontent.com/547147/111869334-eb48f100-89a4-11eb-9c0c-bc74cdee197a.png)

> **SPECTRA** (Secure **P**ersistent **E**vent **C**ollection & **T**elegram **R**ecord **A**rchive) is a battle-hardened toolkit for exporting Telegram channels & groups into forensic-grade SQLite databases and publishing them as modern, searchable static websites.

---

## Why SPECTRA?

* Built for investigators & threat-intel teams who need **verifiable, offline copies** of volatile chat data.
* End-to-end workflow: **fetch → store → analyse → publish** — all from the CLI or an ncurses TUI.
* Modular codebase: `spectra-002` (archiver) · `spectra-004` (DB) · `spectra-site` (builder) · `spectra-003` (orchestrator).

> **Note:** SPECTRA is actively maintained by **SWORD-EPI**. Pull requests are welcome – please keep them focused & well-documented.

---

## Live demo

Archive of the [@fossunited](https://tg.fossunited.org) Telegram group generated with SPECTRA.

![Screenshot](https://user-images.githubusercontent.com/547147/111869398-44188980-89a5-11eb-936f-01d98276ba6a.png)

---

## How it works

1. `spectra-archiver` uses [Telethon](https://github.com/LonamiWebs/Telethon) to incrementally pull messages, media & user avatars into a **single WAL-enabled SQLite database** (one DB can contain multiple channels).
2. Checksums + foreign-key constraints guarantee integrity; checkpoints make every run fully **resumable**.
3. `spectra-site-build` converts the DB into a **Tailwind-styled**, Chart.js-powered static site (one sub-site per channel).
4. `spectra-orchestrator` ties it all together with a **multi-channel queue** and a curses UI.

---

## Features

* 🔄 Incremental sync with resumable checkpoints
* 🛡 WAL-mode SQLite, per-row checksums for forensic integrity
* 👥 Avatar harvesting & username frequency analytics
* 🖼 Media download with MIME whitelist & rotating proxy support
* 📈 Responsive dashboard (stat cards, trend line, doughnut chart)
* 🔍 Deep-link replies, full date hierarchy, RSS/Atom feed generator
* 🖥 Interactive ncurses TUI **or** rich CLI flags
* 🧩 Extensible Jinja2 template & Tailwind CSS assets

---

## Installation

```bash
# Recommended: Python 3.11+
python -m pip install spectra-archive
```

This installs the three entry-point commands:

| Command                | Purpose                                  |
| ---------------------- | ---------------------------------------- |
| `spectra-orchestrator` | End-to-end queue runner (TUI by default) |
| `spectra-archiver`     | Low-level single-channel fetcher         |
| `spectra-site-build`   | Static-site generator for an existing DB |

> You’ll need a **Telegram API ID & hash** from [https://my.telegram.org/apps](https://my.telegram.org/apps) (user account, not bot).

---

## Quick start

```bash
# 1. Create skeleton site & config
spectra-orchestrator --new

# 2. Launch TUI, add channels, hit Esc to start queue
spectra-orchestrator

# 3. Or non-interactive, two channels in parallel
spectra-orchestrator --queue @group1 @group2 --concurrent

# Outputs
#   spectra.sqlite3        ← forensic database
#   site/<channel>/index.html  ← static archive ready to host
```

---

## Customisation

* Edit `template.html` inside each `site/<channel>/` folder to change layout.
* Tailwind CDN is used by default — swap to local build if desired.
* Modify `requirements.txt` & `setup.py` for additional plugins.

---

## Roadmap

* 🔎 Full-text search (SQLite FTS5)
* 🐳 Docker & OCI images
* 📜 Signed snapshot export (WORM media)

---

### Licence

MIT © 2025 John (SWORD-EPI) – see `LICENSE` file.
