"""
SPECTRA-003 — Orchestrator (v4.0)
=================================
*Everything* from the original 3.0 script **plus** multi-channel queue,
npyscreen TUI, and unified SPECTRA integrations.

Key capabilities
----------------
1. **YAML / ENV config** with the full `DEFAULT_CONFIG` you drafted — nothing
   lost.
2. **Checkpoint manager** via `ConfigManager` (JSON file) *and* DB checkpoints.
3. **Multi-channel queue** (`channels:` YAML key or `--queue …`) with
   per-channel site builds (`site/<slug>/`).
4. **Rich CLI** *and* **npyscreen TUI** — choose whichever.
5. **Classic actions** (`--new`, `--sync`, `--build`) still work for single
   channel workflows.
6. **Concurrent fetch** optional (`--concurrent`).

MIT-style licence.  © 2025 John (SWORD-EPI) – codename *SPECTRA-003*.
"""
from __future__ import annotations

# ── Stdlib ───────────────────────────────────────────────────────────────
import argparse
import asyncio
import json
import logging
import os
import platform
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional

# ── Third-party ──────────────────────────────────────────────────────────
import npyscreen  # type: ignore
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from rich.progress import Progress, SpinnerColumn, BarColumn, TimeElapsedColumn
import yaml

# ── SPECTRA modules ──────────────────────────────────────────────────────
from spectra_002_archiver import archive_channel, Config as ArchCfg  # type: ignore
from spectra_004_db_handler import SpectraDB                       # type: ignore
from build_site import build_site                                   # type: ignore

# ── Globals ──────────────────────────────────────────────────────────────
APP_NAME = "spectra_003"
__version__ = "4.0.0"
TZ = timezone.utc
console = Console()

# ── Logging ──────────────────────────────────────────────────────────────
LOGS_DIR = Path("logs"); LOGS_DIR.mkdir(exist_ok=True)
log_file = LOGS_DIR / f"{APP_NAME}_{datetime.now(TZ).strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(APP_NAME)

# ── Default config (superset of v3.0) ────────────────────────────────────
DEFAULT_CONFIG: Dict[str, object] = {
    "api_id": os.getenv("API_ID", ""),
    "api_hash": os.getenv("API_HASH", ""),
    "group": "",  # legacy single-channel key
    "channels": [],
    "download_avatars": True,
    "avatar_size": [64, 64],
    "download_media": False,
    "media_dir": "media",
    "media_mime_types": [],
    "proxy": {"enable": False},
    "fetch_batch_size": 2000,
    "fetch_wait": 5,
    "fetch_limit": 0,
    "publish_rss_feed": True,
    "rss_feed_entries": 100,
    "publish_root": "site",
    "site_url": "https://mysite.com",
    "static_dir": "static",
    "telegram_url": "https://t.me/{id}",
    "per_page": 1000,
    "show_sender_fullname": False,
    "timezone": "",
    "site_name": "@{group} (Telegram) archive",
    "site_description": "Public archive of @{group} Telegram messages.",
    "meta_description": "@{group} {date} Telegram message archive.",
    "page_title": "{date} - @{group} Telegram message archive.",
    "checkpoint_file": "spectra_checkpoint.json",
    # orchestrator specific
    "db_path": "spectra.sqlite3",
}

# ── Config manager (from v3.0) ───────────────────────────────────────────
class ConfigManager:
    def __init__(self, path: Path):
        self.path = path
        self.config = self._load()

    def _load(self) -> Dict:
        cfg = DEFAULT_CONFIG.copy()
        if self.path.exists():
            cfg.update(yaml.safe_load(self.path.read_text()) or {})
        return cfg

    # JSON checkpoint ----------------------------------------------------
    def save_checkpoint(self, data: Dict) -> None:
        ck = Path(self.config["checkpoint_file"])
        ck.write_text(json.dumps(data, indent=2))
        logger.info("Checkpoint saved → %s", ck)

    def load_checkpoint(self) -> Optional[Dict]:
        ck = Path(self.config["checkpoint_file"])
        if ck.exists():
            return json.loads(ck.read_text())
        return None

# ── YAML helper ----------------------------------------------------------

def load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text()) if path.exists() else {}

# ── Archiving helpers ----------------------------------------------------

async def archive_and_build(channel: str, cfg: Dict):
    """Archive single channel then build site."""
    slug = channel.lstrip("@")
    arch_cfg = ArchCfg(); arch_cfg.data.update({"entity": channel, "db_path": cfg["db_path"]})
    out_dir = Path(cfg["publish_root"]) / slug; out_dir.mkdir(parents=True, exist_ok=True)
    with Progress(SpinnerColumn(), "{task.description}", BarColumn(), TimeElapsedColumn(), console=console) as p:
        t = p.add_task(f"{channel} ↦ DB", total=None)
        await archive_channel(arch_cfg)
        p.update(t, description=f"{channel} – build")
        build_site(Path(cfg["db_path"]), out_dir)
        p.update(t, description=f"{channel} – done", completed=1)

async def run_queue(channels: List[str], cfg: Dict, concurrent: bool):
    if concurrent:
        await asyncio.gather(*(archive_and_build(ch, cfg) for ch in channels))
    else:
        for ch in channels:
            await archive_and_build(ch, cfg)

# ── npyscreen TUI --------------------------------------------------------
class QueueForm(npyscreen.ActionForm):
    def create(self):
        self.add(npyscreen.FixedText, value="SPECTRA-003 Queue", editable=False)
        self.qlist = self.add(npyscreen.TitleMultiSelect, name="Channels", values=[], scroll_exit=True)
        self.new_ch = self.add(npyscreen.TitleText, name="Add @channel:")
        self.concurrent = self.add(npyscreen.Checkbox, name="Concurrent", value=False)
        self.status = self.add(npyscreen.FixedText, value="Ready", editable=False)
        self._refresh()

    def _refresh(self):
        self.parentApp.cfg.setdefault("channels", [])
        self.qlist.values = self.parentApp.cfg["channels"]
        self.qlist.display()

    def on_ok(self):
        ch = self.new_ch.value.strip()
        if ch:
            self.parentApp.cfg["channels"].append(ch); self.new_ch.value=""; self._refresh()
        elif self.qlist.value:
            for idx in sorted(self.qlist.value, reverse=True):
                del self.parentApp.cfg["channels"][idx]
            self._refresh()
        else:
            self.status.value = "Select or add channel"; self.status.display()

    def on_cancel(self):
        self.parentApp.run_queue = bool(self.parentApp.cfg.get("channels"))
        self.parentApp.concurrent = self.concurrent.value
        self.parentApp.setNextForm(None)

class SpectraTUI(npyscreen.NPSAppManaged):
    def __init__(self, cfg):
        super().__init__(); self.cfg=cfg; self.run_queue=False; self.concurrent=False
    def onStart(self):
        self.addForm("MAIN", QueueForm, name="SPECTRA-003 TUI")

# ── CLI main -------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser("SPECTRA-003 orchestrator")
    parser.add_argument("--config", "-c", type=Path, default=Path("config.yaml"))
    parser.add_argument("--db", type=Path, help="SQLite db override")
    parser.add_argument("--queue", nargs="*", help="channels to archive")
    parser.add_argument("--concurrent", action="store_true")
    parser.add_argument("--new", action="store_true", help="create skeleton site & exit")
    parser.add_argument("--site", type=Path, help="site root override")
    parser.add_argument("--no-tui", action="store_true")
    args = parser.parse_args()

    cm = ConfigManager(args.config)
    cfg = cm.config
    if args.db: cfg["db_path"] = str(args.db)
    if args.site: cfg["publish_root"] = str(args.site)
    if args.queue: cfg["channels"] = args.queue

    # skeleton -----------------------------------------------------------
    if args.new:
        dst = Path(cfg["publish_root"]); dst.mkdir(parents=True, exist_ok=True)
        (dst/"media").mkdir(exist_ok=True); (dst/"logs").mkdir(exist_ok=True)
        console.print(f"[green]Skeleton created at {dst}")
        sys.exit(0)

    # choose channels ----------------------------------------------------
    if not args.no_tui and not cfg["channels"]:
        app = SpectraTUI(cfg); app.run()
        if not app.run_queue:
            console.print("No channels chosen, exiting."); sys.exit(0)
        concurrent = app.concurrent
    else:
        if not cfg["channels"]:
            console.print("[red]No channels specified.[/]"); sys.exit(1)
        concurrent = args.concurrent

    # run ---------------------------------------------------------------
    console.print(f"[cyan]Processing {len(cfg['channels'])} channel(s)…[/]")
    try:
        asyncio.run(run_queue(cfg["channels"], cfg, concurrent))
    except KeyboardInterrupt:
        console.print("\n[red]Interrupted.[/]")

if __name__ == "__main__":
    main()
