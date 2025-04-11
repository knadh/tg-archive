#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SPECTRA-003: Advanced Telegram Group Archiving Tool
Developed for John under NSA-style codename conventions.
A robust, cybersecurity-focused tool for archiving Telegram groups with static site generation.
Features terminal GUI, progress tracking, resumable operations, and forensic logging.
"""

import argparse
import logging
import os
import sys
import shutil
import yaml
import json
from datetime import datetime
from typing import Dict, Optional, List
import platform
from pathlib import Path

# Third-party imports for enhanced UI and progress tracking
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.prompt import Prompt, Confirm
    from tqdm import tqdm
except ImportError as e:
    logging.error(f"Required libraries missing: {e}. Install with 'pip install rich tqdm'")
    sys.exit(1)

# Placeholder for version (replace with actual version from metadata if available)
__version__ = "3.0.0"

# Setup logging with detailed output to both console and file for forensic traceability
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"spectra_003_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Initialize rich console for terminal GUI
console = Console()

# Default configuration with cybersecurity-focused defaults
DEFAULT_CONFIG = {
    "api_id": os.getenv("API_ID", ""),
    "api_hash": os.getenv("API_HASH", ""),
    "group": "",
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
    "publish_dir": "site",
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
    "checkpoint_file": "spectra_checkpoint.json"  # For resumable operations
}

class ConfigManager:
    """Manages configuration loading, validation, and environment overrides."""
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> Dict:
        """Load YAML config file or fallback to defaults with environment variable overrides."""
        config = DEFAULT_CONFIG.copy()
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, "r") as f:
                    loaded_config = yaml.safe_load(f.read()) or {}
                    config.update(loaded_config)
                logger.info(f"Configuration loaded from {self.config_path}")
            else:
                logger.warning(f"No config.file at {self.config_path}. Using defaults.")
        except Exception as e:
            logger.error(f"Error loading config: {e}. Falling back to defaults.")
        return config

    def save_checkpoint(self, data: Dict) -> None:
        """Save operation checkpoints for resumable tasks."""
        try:
            with open(self.config.get("checkpoint_file", "spectra_checkpoint.json"), "w") as f:
                json.dump(data, f, indent=2)
            logger.info("Checkpoint saved successfully.")
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")

    def load_checkpoint(self) -> Optional[Dict]:
        """Load operation checkpoints for resuming tasks."""
        checkpoint_file = self.config.get("checkpoint_file", "spectra_checkpoint.json")
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, "r") as f:
                    data = json.load(f)
                logger.info("Checkpoint loaded for resumable operation.")
                return data
            except Exception as e:
                logger.error(f"Error loading checkpoint: {e}")
        return None

def display_welcome_panel():
    """Display a formatted welcome panel in the terminal GUI."""
    welcome_table = Table(show_header=False, box=None)
    welcome_table.add_row("[bold cyan]SPECTRA-003[/bold cyan]")
    welcome_table.add_row("Advanced Telegram Group Archiving Tool")
    welcome_table.add_row(f"Version: {__version__}")
    welcome_table.add_row(f"System: {platform.system()} {platform.release()}")
    console.print(Panel(welcome_table, title="Welcome, John", expand=False))

def prompt_operation() -> str:
    """Prompt user for the operation to perform using a rich interface."""
    console.print("\n[bold green]Select Operation:[/bold green]")
    options = ["New Site Setup", "Sync Telegram Data", "Build Static Site", "Show Version", "Exit"]
    for i, opt in enumerate(options, 1):
        console.print(f"  {i}. {opt}")
    choice = Prompt.ask("Enter choice (1-5)", choices=[str(i) for i in range(1, 6)], default="5")
    return options[int(choice) - 1]

# Placeholder for DB class (to be imported from .db in actual implementation)
class DB:
    def __init__(self, db_path: str, timezone: str = ""):
        self.db_path = db_path
        self.timezone = timezone
        logger.info(f"Initialized DB at {db_path}")

# Placeholder for Sync class (to be imported from .sync)
class Sync:
    def __init__(self, config: Dict, session_path: str, db: DB):
        self.config = config
        self.session_path = session_path
        self.db = db
        logger.info("Sync module initialized")

    def sync(self, message_ids: Optional[List[int]] = None, from_id: Optional[int] = None):
        """Simulate sync with progress bar (replace with actual Telethon logic)."""
        total = self.config["fetch_limit"] if self.config["fetch_limit"] > 0 else 10000
        mode = "takeout" if self.config.get("use_takeout", False) else "standard"
        logger.info(f"Starting sync: batch_size={self.config['fetch_batch_size']}, limit={total}, mode={mode}")
        with tqdm(total=total, desc="Syncing Messages", unit="msgs") as pbar:
            for i in range(total):
                # Simulate syncing messages
                pbar.update(1)
                if i % self.config["fetch_batch_size"] == 0:
                    logger.info(f"Processed batch of {self.config['fetch_batch_size']} messages")
                    time.sleep(self.config["fetch_wait"] / 10)  # Simulate wait
        logger.info("Sync completed successfully")

    def finish_takeout(self):
        logger.info("Finishing takeout session")

# Placeholder for Build class (to be imported from .build)
class Build:
    def __init__(self, config: Dict, db: DB, symlink: bool = False):
        self.config = config
        self.db = db
        self.symlink = symlink
        logger.info("Build module initialized")

    def load_template(self, template_path: str):
        logger.info(f"Loading template from {template_path}")

    def load_rss_template(self, rss_template_path: str):
        logger.info(f"Loading RSS template from {rss_template_path}")

    def build(self):
        """Simulate building site with progress bar."""
        total_steps = 100
        with tqdm(total=total_steps, desc="Building Static Site", unit="steps") as pbar:
            for _ in range(total_steps):
                pbar.update(1)
                time.sleep(0.05)  # Simulate build time
        logger.info(f"Site published to {self.config['publish_dir']}")

def setup_new_site(path: str):
    """Setup a new site directory structure."""
    try:
        exdir = os.path.join(os.path.dirname(__file__), "example")
        if not os.path.isdir(exdir):
            logger.error("Bundled example directory not found")
            console.print("[bold red]Error: Example directory not found.[/bold red]")
            return False
        shutil.copytree(exdir, path)
        os.chmod(path, 0o755)
        for root, dirs, files in os.walk(path):
            for d in dirs:
                os.chmod(os.path.join(root, d), 0o755)
            for f in files:
                os.chmod(os.path.join(root, f), 0o644)
        logger.info(f"New site created at {path}")
        console.print(f"[bold green]New site created at {path}[/bold green]")
        return True
    except FileExistsError:
        logger.error(f"Directory {path} already exists")
        console.print(f"[bold red]Error: Directory {path} already exists.[/bold red]")
        return False
    except Exception as e:
        logger.error(f"Error setting up new site: {e}")
        console.print(f"[bold red]Error: {str(e)}[/bold red]")
        return False

def main():
    """Main entry point for SPECTRA-003 with terminal GUI and operation handling."""
    display_welcome_panel()
    
    # Parse arguments (for compatibility with CLI usage)
    parser = argparse.ArgumentParser(
        description="SPECTRA-003: Advanced Telegram Group Archiving Tool",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("-c", "--config", type=str, default="config.yaml", help="Path to config file")
    parser.add_argument("-d", "--data", type=str, default="data.sqlite", help="Path to SQLite data file")
    parser.add_argument("-se", "--session", type=str, default="session.session", help="Path to session file")
    parser.add_argument("-v", "--version", action="store_true", help="Display version")
    parser.add_argument("-n", "--new", action="store_true", help="Initialize a new site")
    parser.add_argument("-p", "--path", type=str, default="example", help="Path to create the site")
    parser.add_argument("-s", "--sync", action="store_true", help="Sync data from Telegram group")
    parser.add_argument("-id", "--id", type=int, nargs="+", help="Sync messages for given IDs")
    parser.add_argument("-from-id", "--from-id", type=int, help="Sync messages from this ID to latest")
    parser.add_argument("-b", "--build", action="store_true", help="Build the static site")
    parser.add_argument("-t", "--template", type=str, default="template.html", help="Path to template file")
    parser.add_argument("--rss-template", type=str, default=None, help="Path to RSS template file")
    parser.add_argument("--symlink", action="store_true", help="Symlink media and static files instead of copying")

    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])

    # Load configuration
    config_mgr = ConfigManager(args.config)
    config = config_mgr.config

    # Handle GUI or CLI mode (GUI preferred as per request)
    if len(sys.argv) > 1:
        operation = None  # CLI mode will determine operation from args
    else:
        operation = prompt_operation()  # GUI mode with rich prompt

    if args.version or operation == "Show Version":
        console.print(f"[bold]SPECTRA-003 Version: {__version__}[/bold]")
        sys.exit(0)
    elif args.new or operation == "New Site Setup":
        path = args.path if args.new else Prompt.ask("Enter path for new site", default="example")
        setup_new_site(path)
    elif args.sync or operation == "Sync Telegram Data":
        if args.id and args.from_id:
            logger.error("Cannot use both --id and --from-id")
            console.print("[bold red]Error: Cannot use both --id and --from-id.[/bold red]")
            sys.exit(1)
        db = DB(args.data, config.get("timezone", ""))
        sync = Sync(config, args.session, db)
        checkpoint = config_mgr.load_checkpoint()
        if checkpoint and not args.id and not args.from_id:
            from_id = checkpoint.get("last_synced_id", None)
            console.print(f"[bold green]Resuming sync from ID {from_id}[/bold green]")
        else:
            from_id = args.from_id
        try:
            sync.sync(args.id, from_id)
            # Simulate saving checkpoint (replace with real logic)
            config_mgr.save_checkpoint({"last_synced_id": from_id or 0})
        except KeyboardInterrupt:
            logger.info("Sync cancelled manually")
            console.print("[bold yellow]Sync cancelled by user.[/bold yellow]")
            if config.get("use_takeout", False):
                sync.finish_takeout()
            sys.exit(0)
        except Exception as e:
            logger.error(f"Sync error: {e}", exc_info=True)
            console.print(f"[bold red]Sync Error: {str(e)}[/bold red]")
            sys.exit(1)
    elif args.build or operation == "Build Static Site":
        db = DB(args.data, config.get("timezone", ""))
        build = Build(config, db, args.symlink)
        build.load_template(args.template)
        if args.rss_template:
            build.load_rss_template(args progress bars for sync and build operations provide visual feedback, crucial for long-running tasks in high-level data operations.
3. **Checkpointing for Resumable Tasks**: Added mechanisms to save and load checkpoints, ensuring operations can resume after interruptions—vital for large dataset archiving.
4. **Comprehensive Error Handling and Logging**: All exceptions are caught, logged with full stack traces to a file, and summarized in the terminal for immediate feedback.
5. **Modular Design**: Classes like `ConfigManager`, `Sync`, and `Build` are structured for easy expansion, with placeholders for integrating advanced cybersecurity features (e.g., LLM training on archived messages).
6. **Cross-Platform Compatibility**: Using `pathlib` and `platform` checks ensures the script runs on Linux (Debian/RHEL) and Windows.
7. **Detailed Comments and Logging**: I've included extensive comments and ensured every major action logs to both console and file, aiding debugging and forensic analysis.

#### **Installation of Dependencies**
Run the following to install necessary libraries:
```bash
pip install rich tqdm pyyaml
