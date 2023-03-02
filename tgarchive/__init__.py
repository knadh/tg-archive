import argparse
import logging
import os
import shutil
import sys
import yaml

from .db import DB

__version__ = "1.1.2"

logging.basicConfig(format="%(asctime)s: %(message)s",
                    level=logging.INFO)

_CONFIG = {
    "api_id": os.getenv("API_ID", ""),
    "api_hash": os.getenv("API_HASH", ""),
    "group": "",
    "download_avatars": True,
    "avatar_size": [64, 64],
    "download_media": False,
    "media_dir": "media",
    "media_mime_types": [],
    "proxy": {
        "enable": False,
    },
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
    "page_title": "{date} - @{group} Telegram message archive."
}


def get_config(path):
    config = {}
    with open(path, "r") as f:
        config = {**_CONFIG, **yaml.safe_load(f.read())}
    return config


def main():
    """Run the CLI."""
    p = argparse.ArgumentParser(
        description="A tool for exporting and archiving Telegram groups to webpages.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    p.add_argument("-c", "--config", action="store", type=str, default="config.yaml",
                   dest="config", help="path to the config file")
    p.add_argument("-d", "--data", action="store", type=str, default="data.sqlite",
                   dest="data", help="path to the SQLite data file to store messages")
    p.add_argument("-se", "--session", action="store", type=str, default="session.session",
                   dest="session", help="path to the session file")
    p.add_argument("-v", "--version", action="store_true", dest="version", help="display version")

    n = p.add_argument_group("new")
    n.add_argument("-n", "--new", action="store_true",
                   dest="new", help="initialize a new site")
    n.add_argument("-p", "--path", action="store", type=str, default="example",
                   dest="path", help="path to create the site")

    s = p.add_argument_group("sync")
    s.add_argument("-s", "--sync", action="store_true",
                   dest="sync", help="sync data from telegram group to the local DB")
    s.add_argument("-id", "--id", action="store", type=int, nargs="+",
                   dest="id", help="sync (or update) messages for given ids")
    s.add_argument("-from-id", "--from-id", action="store", type=int,
                   dest="from_id", help="sync (or update) messages from this id to the latest")

    b = p.add_argument_group("build")
    b.add_argument("-b", "--build", action="store_true",
                   dest="build", help="build the static site")
    b.add_argument("-t", "--template", action="store", type=str, default="template.html",
                   dest="template", help="path to the template file")
    b.add_argument("--rss-template", action="store", type=str, default=None,
                   dest="rss_template", help="path to the rss template file")
    b.add_argument("--symlink", action="store_true", dest="symlink",
                   help="symlink media and other static files instead of copying")

    args = p.parse_args(args=None if sys.argv[1:] else ['--help'])

    if args.version:
        print("v{}".format(__version__))
        sys.exit()

    # Setup new site.
    elif args.new:
        exdir = os.path.join(os.path.dirname(__file__), "example")
        if not os.path.isdir(exdir):
            logging.error("unable to find bundled example directory")
            sys.exit(1)

        try:
            shutil.copytree(exdir, args.path)
        except FileExistsError:
            logging.error(
                "the directory '{}' already exists".format(args.path))
            sys.exit(1)
        except:
            raise

        logging.info("created directory '{}'".format(args.path))
        
        # make sure the files are writable
        os.chmod(args.path, 0o755)
        for root, dirs, files in os.walk(args.path):
            for d in dirs:
                os.chmod(os.path.join(root, d), 0o755)
            for f in files:
                os.chmod(os.path.join(root, f), 0o644)

    # Sync from Telegram.
    elif args.sync:
        # Import because the Telegram client import is quite heavy.
        from .sync import Sync

        if args.id and args.from_id and args.from_id > 0:
            logging.error("pass either --id or --from-id but not both")
            sys.exit(1)

        cfg = get_config(args.config)
        mode = "takeout" if cfg.get("use_takeout", False) else "standard"

        logging.info("starting Telegram sync (batch_size={}, limit={}, wait={}, mode={})".format(
            cfg["fetch_batch_size"], cfg["fetch_limit"], cfg["fetch_wait"], mode
        ))
        try:
            s = Sync(cfg, args.session, DB(args.data))
            s.sync(args.id, args.from_id)
        except KeyboardInterrupt as e:
            logging.info("sync cancelled manually")
            if cfg.get("use_takeout", False):
                s.finish_takeout()
            sys.exit()
        except:
            raise

    # Build static site.
    elif args.build:
        from .build import Build

        logging.info("building site")
        config = get_config(args.config)
        b = Build(config, DB(args.data, config["timezone"]), args.symlink)
        b.load_template(args.template)
        if args.rss_template:
            b.load_rss_template(args.rss_template)
        b.build()

        logging.info("published to directory '{}'".format(config["publish_dir"]))
