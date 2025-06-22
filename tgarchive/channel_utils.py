"""
Utilities for channel enumeration and access logging.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

# Third-party imports
from telethon import TelegramClient
from telethon.tl.types import Dialog, Channel, ChatForbidden # Import ChatForbidden for handling restricted chats
from telethon.errors import ChannelPrivateError, RPCError, ChatAdminRequiredError, UserDeactivatedError, AuthKeyError, AuthKeyUnregisteredError, UserBannedInChannelError

# Local application imports
from tgarchive.db import SpectraDB
from tgarchive.sync import Config # Config class handles accounts and API details

logger = logging.getLogger("tgarchive.channel_utils") # Using a more specific logger name


async def populate_account_channel_access(db: SpectraDB, config: Config):
    """
    Populates the account_channel_access table with channels/groups
    accessible by each configured account.
    """
    if not config.accounts:
        logger.warning("No accounts configured. Skipping channel access population.")
        return

    for account_details in config.accounts:
        session_name = account_details.get("session_name")
        api_id = account_details.get("api_id")
        api_hash = account_details.get("api_hash")
        phone_number = account_details.get("phone_number", session_name) # Use phone_number if available, else session_name

        if not all([session_name, api_id, api_hash]):
            logger.warning(f"Account {session_name or phone_number} is missing session_name, api_id, or api_hash. Skipping.")
            continue

        logger.info(f"Processing account: {phone_number} (Session: {session_name})")

        # Proxy configuration (simplified, adapt from sync.py if complex proxy logic is needed)
        proxy_conf = config.data.get("proxy")
        proxy = None
        if proxy_conf and proxy_conf.get("enabled"):
            # This is a simplified proxy setup. For rotating proxies, more logic from sync.ProxyCycler would be needed.
            # For now, let's assume a single proxy configuration or direct connection if not detailed.
            # This part might need enhancement if complex proxy rotation per account is required here.
            try:
                # PySocks proxy format: (socks.SOCKS5, 'host', port, True, 'user', 'pass')
                # This example assumes SOCKS5 and may need adjustment based on actual proxy types used.
                import socks # type: ignore
                proxy_type_map = {"socks5": socks.SOCKS5, "socks4": socks.SOCKS4, "http": socks.HTTP}
                p_type = proxy_conf.get("type", "socks5").lower()
                if p_type in proxy_type_map:
                    proxy = (
                        proxy_type_map[p_type],
                        proxy_conf["host"],
                        proxy_conf["port"],
                        True, # rdns, typically True
                        proxy_conf.get("username"),
                        proxy_conf.get("password"),
                    )
                else:
                    logger.warning(f"Unsupported proxy type: {p_type} for account {phone_number}")
            except KeyError as e:
                logger.warning(f"Proxy configuration for {phone_number} is incomplete (missing {e}). Skipping proxy.")
            except ImportError:
                logger.warning("PySocks library not found, cannot use proxy. Continuing without proxy.")


        client = TelegramClient(str(Config().path.parent / session_name), api_id, api_hash, proxy=proxy)

        try:
            await client.connect()
            if not await client.is_user_authorized():
                logger.warning(f"Account {phone_number} is not authorized. Skipping.")
                await client.disconnect()
                continue

            logger.info(f"Fetching dialogs for account: {phone_number}")
            dialog_count = 0
            async for dialog in client.iter_dialogs():
                dialog_count += 1
                entity = dialog.entity
                channel_id: Optional[int] = None
                channel_name: Optional[str] = None
                access_hash: Optional[int] = None

                if isinstance(entity, (Channel, Dialog)): # Dialog might wrap a Channel
                    if hasattr(entity, 'id'): # Basic check for entity properties
                        channel_id = entity.id
                    if hasattr(entity, 'title'):
                        channel_name = entity.title
                    if hasattr(entity, 'access_hash'):
                        access_hash = entity.access_hash
                    
                    # For Dialog objects, the entity is what we care about
                    if isinstance(dialog, Dialog) and hasattr(dialog.entity, 'id'):
                         channel_id = dialog.entity.id
                         if hasattr(dialog.entity, 'title'):
                            channel_name = dialog.entity.title
                         if hasattr(dialog.entity, 'access_hash'):
                            access_hash = dialog.entity.access_hash


                if channel_id is None: # Skip if it's not a channel/group we can process
                    # logger.debug(f"Skipping dialog (not a recognized channel/group type or missing ID): {dialog.name}")
                    continue
                
                # Ensure channel_id is positive, as Telethon might use negative IDs for channels/chats
                # and our schema uses BIGINT which typically means positive.
                # However, Telegram IDs are large numbers and can be negative. Let's keep them as is.

                last_seen = datetime.now(timezone.utc).isoformat()

                logger.debug(
                    f"Found for {phone_number}: id={channel_id}, name='{channel_name}', hash={access_hash}"
                )
                db.upsert_account_channel_access(
                    account_phone_number=str(phone_number), # Ensure phone_number is string
                    channel_id=channel_id,
                    channel_name=channel_name,
                    access_hash=access_hash,
                    last_seen=last_seen,
                )
            logger.info(f"Processed {dialog_count} dialogs for account: {phone_number}")

        except (ChannelPrivateError, ChatAdminRequiredError, UserBannedInChannelError) as e:
            logger.warning(f"Telegram API error for account {phone_number} (channel access issue): {e}")
        except (AuthKeyError, AuthKeyUnregisteredError, UserDeactivatedError) as e:
            logger.error(f"Telegram authentication error for account {phone_number}: {e}. This account may need re-authentication or is banned.")
        except RPCError as e:
            logger.error(f"Telegram RPC error for account {phone_number}: {e}")
        except ConnectionError as e:
            logger.error(f"Connection error for account {phone_number}: {e}")
        except Exception as e:
            logger.exception(f"An unexpected error occurred while processing account {phone_number}: {e}")
        finally:
            if client.is_connected():
                await client.disconnect()
            logger.info(f"Disconnected client for account: {phone_number}")

    logger.info("Finished populating account_channel_access table.")
