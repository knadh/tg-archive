import argparse
import logging
import os
import shutil
import sys
import yaml

from .db import DB

__version__ = "0.3.8"

logging.basicConfig(format="%(asctime)s: %(message)s",
                    level=logging.INFO)

_CONFIG = {
    "api_id": "",
    "api_hash": "",
    "group": "",
    "download_avatars": True,
    "avatar_size": [64, 64],
    "download_media": False,
    "media_dir": "media",
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
                   dest="id", help="sync (or update) data for specific message ids")

    b = p.add_argument_group("build")
    b.add_argument("-b", "--build", action="store_true",
                   dest="build", help="build the static site")
    b.add_argument("-t", "--template", action="store", type=str, default="template.html",
                   dest="template", help="path to the template file")
    b.add_argument("-o", "--output", action="store", type=str, default="site",
                   dest="output", help="path to the output directory")

    args = p.parse_args(args=None if sys.argv[1:] else ['--help'])

    if args.version:
        print("v{}".format(__version__))
        quit()

    # Setup new site.
    elif args.new:
        exdir = os.path.join(os.path.dirname(__file__), "example")
        if not os.path.isdir(exdir):
            logging.error("unable to find bundled example directory")
            quit(1)

        try:
            shutil.copytree(exdir, args.path)
        except FileExistsError:
            logging.error(
                "the directory '{}' already exists".format(args.path))
            quit(1)
        except:
            raise

        logging.info("created directory '{}'".format(args.path))

    # Sync from Telegram.
    elif args.sync:
        # Import because the Telegram client import is quite heavy.
        from .sync import Sync

        cfg = get_config(args.config)
        logging.info("starting Telegram sync (batch_size={}, limit={}, wait={})".format(
            cfg["fetch_batch_size"], cfg["fetch_limit"], cfg["fetch_wait"]
        ))

        try:
            Sync(cfg, args.session, DB(args.data)).sync(args.id)
        except KeyboardInterrupt as e:
            logging.info("sync cancelled manually")
            quit()
        except:
            raise

    # Build static site.
    elif args.build:
        from .build import Build

        logging.info("building site")
        b = Build(get_config(args.config), DB(args.data))
        b.load_template(args.template)
        b.build()

        logging.info("published to directory '{}'".format(args.output))
