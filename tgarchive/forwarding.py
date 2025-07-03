"""
Handles forwarding of message attachments between Telegram entities.
"""
from __future__ import annotations

import logging
import asyncio # Added for asyncio.sleep
from typing import Optional, Set
import hashlib # Added for deduplication
from datetime import datetime, timezone # Added for deduplication timestamp

# Third-party imports
from telethon import TelegramClient, errors as telethon_errors # Added telethon_errors
from telethon.tl import types # Added for types.MessageMediaWebPage
from telethon.tl.types import Message as TLMessage, InputPeerChannel, User, Channel, ChatAdminRequiredError # Add more as needed # Added ChatAdminRequiredError
from telethon.errors import RPCError, ChannelPrivateError, UserDeactivatedError, AuthKeyError, UserBannedInChannelError # Added UserBannedInChannelError

# Local application imports
from tgarchive.db import SpectraDB
from tgarchive.sync import Config, DEFAULT_CFG # DEFAULT_CFG might not be directly needed here but good for ref

logger = logging.getLogger("tgarchive.forwarding")

class AttachmentForwarder:
    """
    Manages forwarding of attachments from an origin to a destination Telegram entity.
    """

    def __init__(self, config: Config, db: Optional[SpectraDB] = None,
                 forward_to_all_saved_messages: bool = False,
                 prepend_origin_info: bool = False,
                 destination_topic_id: Optional[int] = None,
                 secondary_unique_destination: Optional[str] = None, # NEW
                 enable_deduplication: bool = True): # NEW
        """
        Initializes the AttachmentForwarder.
        Enhanced with deduplication support.

        Args:
            config: The SPECTRA configuration object.
            db: An optional SpectraDB instance for database interactions.
            forward_to_all_saved_messages: If True, forward to all accounts' Saved Messages.
            prepend_origin_info: If True, prepend origin info to message text.
            destination_topic_id: Optional topic ID for the destination.
            secondary_unique_destination: Channel ID for unique messages only.
            enable_deduplication: Enable duplicate detection.
        """
        self.config = config
        self.db = db
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._client: Optional[TelegramClient] = None
        self.forward_to_all_saved_messages = forward_to_all_saved_messages
        self.prepend_origin_info = prepend_origin_info
        self.destination_topic_id = destination_topic_id

        # Deduplication related attributes
        self.enable_deduplication = enable_deduplication
        self.secondary_unique_destination = secondary_unique_destination
        self.message_hashes: Set[str] = set()  # In-memory cache for hashes

        if self.forward_to_all_saved_messages:
            self.logger.info("Forwarding to 'Saved Messages' of all configured accounts is ENABLED.")
        if self.prepend_origin_info:
            self.logger.info("Prepending origin info to forwarded messages is ENABLED.")
        if self.destination_topic_id:
            self.logger.info(f"Forwarding to destination topic ID: {self.destination_topic_id}")
        if self.enable_deduplication:
            self.logger.info("Deduplication is ENABLED.")
            if self.secondary_unique_destination:
                self.logger.info(f"Unique messages will be forwarded to secondary destination: {self.secondary_unique_destination}")
            if self.db:
                self._init_dedup_table()
                self._load_existing_hashes() # Load hashes from DB into memory
            else:
                self.logger.warning("Deduplication is enabled, but no database is configured. Deduplication will be in-memory only for this session.")
        else:
            self.logger.info("Deduplication is DISABLED.")

    def _init_dedup_table(self):
        """Create deduplication tracking table if it doesn't exist."""
        if not self.db:
            self.logger.error("Database not available, cannot initialize dedup table.")
            return
        try:
            self.db.conn.execute("""
                CREATE TABLE IF NOT EXISTS forwarded_messages (
                    hash TEXT PRIMARY KEY,
                    origin_id TEXT,
                    destination_id TEXT,
                    message_id INTEGER,
                    forwarded_at TEXT,
                    content_preview TEXT
                )
            """)
            self.db.conn.commit()
            self.logger.info("Deduplication table 'forwarded_messages' initialized successfully.")
        except Exception as e:
            self.logger.error(f"Failed to create/ensure dedup table 'forwarded_messages': {e}", exc_info=True)

    def _load_existing_hashes(self):
        """Load existing message hashes from the database into the in-memory set."""
        if not self.db:
            self.logger.warning("Database not available, cannot load existing hashes for deduplication.")
            return
        try:
            cursor = self.db.conn.execute("SELECT hash FROM forwarded_messages")
            count = 0
            for row in cursor:
                self.message_hashes.add(row[0])
                count += 1
            self.logger.info(f"Loaded {count} existing message hashes into memory for deduplication.")
        except Exception as e:
            self.logger.error(f"Failed to load existing message hashes from database: {e}", exc_info=True)

    def _compute_message_hash(self, message: TLMessage) -> str:
        """Compute a unique hash for message content (text and media)."""
        content_parts = []

        # Add message text
        if message.text:
            content_parts.append(message.text)

        # Add media identifiers (if media exists)
        if message.media:
            # General media attributes
            if hasattr(message.media, 'id'): # Common for Photo, Document
                content_parts.append(f"media_id:{message.media.id}")
            if hasattr(message.media, 'access_hash'): # Common for Photo, Document
                content_parts.append(f"media_hash:{message.media.access_hash}")

            # Specific file attributes if message.file exists (for documents, videos, photos etc.)
            if message.file:
                if hasattr(message.file, 'id') and message.file.id is not None: # Ensure ID is not None
                    content_parts.append(f"file_id:{message.file.id}")
                if hasattr(message.file, 'size') and message.file.size is not None:
                    content_parts.append(f"file_size:{message.file.size}")
                # Using message.file.name might be too variable if names change slightly but content is same.
                # However, for some media (like generic web previews), other IDs might be less stable.
                # Let's stick to IDs and size for now.

            # For WebPage media, try to get URL
            if isinstance(message.media, types.MessageMediaWebPage) and hasattr(message.media.webpage, 'url'):
                 content_parts.append(f"webpage_url:{message.media.webpage.url}")


        # Fallback for messages that might only have media but no standard IDs (e.g. some polls, geo points)
        if not content_parts and message.media: # If no specific IDs were found but media exists
            content_parts.append(f"media_type:{type(message.media).__name__}") # Add type as a fallback

        if not content_parts and not message.text: # Message with no text and no identifiable media parts
             # This could be a service message or something unusual. Hash its ID as a last resort.
             content_parts.append(f"message_obj_id:{message.id}")


        content_string = "|".join(sorted(str(p) for p in content_parts)) # Sort to ensure order doesn't change hash
        return hashlib.sha256(content_string.encode('utf-8')).hexdigest()

    async def _is_duplicate(self, message: TLMessage) -> bool:
        """Check if message has been forwarded before using its hash."""
        if not self.enable_deduplication:
            return False

        msg_hash = self._compute_message_hash(message)

        # Check memory cache first
        if msg_hash in self.message_hashes:
            self.logger.debug(f"Message ID {message.id} (hash: {msg_hash[:8]}...) found in memory cache as duplicate.")
            return True

        # Check database if DB is available
        if self.db:
            try:
                result = self.db.conn.execute(
                    "SELECT 1 FROM forwarded_messages WHERE hash = ?",
                    (msg_hash,)
                ).fetchone()

                if result:
                    self.message_hashes.add(msg_hash)  # Update memory cache
                    self.logger.debug(f"Message ID {message.id} (hash: {msg_hash[:8]}...) found in DB as duplicate.")
                    return True
            except Exception as e:
                self.logger.error(f"Error checking duplicate in DB for hash {msg_hash[:8]}...: {e}", exc_info=True)
                # If DB check fails, rely on memory cache (or treat as not duplicate to be safe for forwarding)
                # For now, let's assume if DB check fails, it's not a duplicate to avoid blocking forwards.

        return False # Not found in cache or DB (or dedup disabled/DB error)

    async def _record_forwarded(self, message: TLMessage, origin_id: str, dest_id: str):
        """Record that a message was forwarded by storing its hash in the database."""
        if not self.enable_deduplication or not self.db:
            return # Do nothing if dedup is off or no DB

        msg_hash = self._compute_message_hash(message)
        self.message_hashes.add(msg_hash) # Add to memory cache immediately

        # Create a short preview of the content for the DB log
        content_preview = "Media Message"
        if message.text:
            content_preview = (message.text[:100] + '...') if len(message.text) > 100 else message.text
        elif message.file and message.file.name:
            content_preview = f"File: {message.file.name}"

        try:
            self.db.conn.execute("""
                INSERT OR IGNORE INTO forwarded_messages
                (hash, origin_id, destination_id, message_id, forwarded_at, content_preview)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                msg_hash,
                str(origin_id), # Ensure it's a string
                str(dest_id),   # Ensure it's a string
                message.id,
                datetime.now(timezone.utc).isoformat(),
                content_preview
            ))
            self.db.conn.commit()
            self.logger.debug(f"Recorded forwarded message ID {message.id} (hash: {msg_hash[:8]}...) to DB.")
        except Exception as e:
            self.logger.error(f"Failed to record forwarded message (hash: {msg_hash[:8]}...) to DB: {e}", exc_info=True)


    async def _get_client(self, account_identifier: Optional[str] = None) -> TelegramClient:
        """
        Gets an initialized and connected Telegram client.

        If a client is already initialized and connected for the target account,
        it will be reused. Otherwise, a new client is created and connected.

        Args:
            account_identifier: The phone number or session name of the account to use.
                                If None, the first available account from the config is used.

        Returns:
            An initialized and connected TelegramClient instance.

        Raises:
            ValueError: If no suitable accounts are found or if an account is not authorized.
            ConnectionError: If the client fails to connect.
        """
        selected_account = None
        if account_identifier:
            for acc in self.config.accounts:
                if acc.get("phone_number") == account_identifier or acc.get("session_name") == account_identifier:
                    selected_account = acc
                    break
            if not selected_account:
                raise ValueError(f"Account '{account_identifier}' not found in configuration.")
        elif self.config.accounts:
            selected_account = self.config.accounts[0] # Default to the first account
            self.logger.info(f"No account specified, using the first configured account: {selected_account.get('session_name')}")
        else:
            raise ValueError("No accounts configured.")

        session_name = selected_account.get("session_name")
        api_id = selected_account.get("api_id")
        api_hash = selected_account.get("api_hash")
        
        if not all([session_name, api_id, api_hash]):
            raise ValueError(f"Account {session_name or 'Unknown'} is missing critical configuration (session_name, api_id, or api_hash).")

        # Check if we already have a client for this session and it's connected
        if self._client and self._client.session.filename == str(Config().path.parent / session_name) and self._client.is_connected():
             if await self._client.is_user_authorized():
                self.logger.debug(f"Reusing existing connected client for session: {session_name}")
                return self._client
             else: # If not authorized, we need to re-establish
                self.logger.warning(f"Existing client for {session_name} is no longer authorized. Attempting to reconnect.")
                await self._client.disconnect()
                self._client = None


        # Proxy configuration (simplified from channel_utils.py)
        proxy = None
        proxy_conf = self.config.data.get("proxy")
        if proxy_conf and proxy_conf.get("enabled"):
            try:
                import socks # type: ignore
                proxy_type_map = {"socks5": socks.SOCKS5, "socks4": socks.SOCKS4, "http": socks.HTTP}
                p_type = proxy_conf.get("type", "socks5").lower()
                if p_type in proxy_type_map:
                    proxy = (
                        proxy_type_map[p_type],
                        proxy_conf["host"],
                        proxy_conf["port"],
                        True,
                        proxy_conf.get("username"),
                        proxy_conf.get("password"),
                    )
                else:
                    self.logger.warning(f"Unsupported proxy type: {p_type}")
            except KeyError as e:
                self.logger.warning(f"Proxy configuration is incomplete (missing {e}). Proceeding without proxy.")
            except ImportError:
                self.logger.warning("PySocks library not found. Proceeding without proxy.")
        
        client_path = str(Config().path.parent / session_name) # Ensures session files are in the same dir as config
        self._client = TelegramClient(client_path, api_id, api_hash, proxy=proxy)
        
        self.logger.info(f"Connecting to Telegram with account: {session_name}")
        try:
            await self._client.connect()
        except ConnectionError as e:
            self.logger.error(f"Failed to connect to Telegram for account {session_name}: {e}")
            # Clean up client instance if connection fails
            self._client = None
            raise
            
        if not await self._client.is_user_authorized():
            await self._client.disconnect()
            self._client = None # Clean up
            raise ValueError(f"Account {session_name} is not authorized. Please check credentials or run authorization process.")
        
        self.logger.info(f"Successfully connected and authorized as {session_name}.")
        return self._client

    async def forward_messages(
        self,
        origin_id: int | str,
        destination_id: int | str,
        account_identifier: Optional[str] = None
    ):
        """
        Forwards messages (currently placeholders for attachments) from an origin
        Telegram entity to a destination Telegram entity.

        Args:
            origin_id: The ID or username of the origin channel/chat.
            destination_id: The ID or username of the destination channel/chat.
            account_identifier: Optional identifier for the account to use for forwarding.
        """
        client = None
        try:
            client = await self._get_client(account_identifier)
            
            self.logger.info(f"Attempting to resolve origin: '{origin_id}'")
            origin_entity = await client.get_entity(origin_id)
            self.logger.info(f"Origin '{origin_id}' resolved to: {origin_entity.id if hasattr(origin_entity, 'id') else 'Unknown ID'}")

            self.logger.info(f"Attempting to resolve destination: '{destination_id}'")
            destination_entity = await client.get_entity(destination_id)
            self.logger.info(f"Destination '{destination_id}' resolved to: {destination_entity.id if hasattr(destination_entity, 'id') else 'Unknown ID'}")

            # Basic check if entities could be resolved
            if not origin_entity or not destination_entity:
                raise ValueError("Could not resolve one or both Telegram entities.")

            self.logger.info(f"Starting to iterate messages from origin: {origin_id}")
            # TODO: Add pagination/checkpointing if needed for very large channels
            async for message in client.iter_messages(origin_entity): # type: ignore
                message: TLMessage # Type hint
                self.logger.debug(f"Processing Message ID: {message.id} from {origin_id}. Type: {type(message.media).__name__ if message.media else 'Text'}")

                # 0. Deduplication Check
                if await self._is_duplicate(message):
                    self.logger.info(f"Message ID: {message.id} (from {origin_id}) is a duplicate. Skipping forwarding.")
                    continue # Skip this message entirely

                # 1. Attachment Filtering (as per existing logic)
                if not message.media: # This example forwarder focuses on messages with media
                    self.logger.debug(f"Message {message.id} has no media. Skipping.")
                    continue

                self.logger.info(f"Message {message.id} has media. Attempting to forward to {destination_id}.")

                # 2. Message Forwarding Logic & 3. Rate Limit Handling
                successfully_forwarded_main = False
                # message_to_forward = message # This is what we'll operate on (already just 'message')
                
                main_reply_to_arg = self.destination_topic_id 

                try:
                    # Forward to Primary Destination
                    if self.prepend_origin_info and not self.destination_topic_id:
                        origin_title = getattr(origin_entity, 'title', f"ID: {origin_entity.id}")
                        header = f"[Forwarded from {origin_title} (ID: {origin_entity.id})]\n"
                        message_content = header + (message.text or "")
                        
                        if client.session.filename != str(Config().path.parent / (account_identifier or self.config.accounts[0].get("session_name"))):
                             await self.close()
                             client = await self._get_client(account_identifier)

                        await client.send_message(
                            entity=destination_entity,
                            message=message_content,
                            file=message.media,
                            reply_to=main_reply_to_arg
                        )
                        self.logger.info(f"Successfully sent Message ID: {message.id} with origin info from '{origin_id}' to '{destination_id}'.")
                    else:
                        await client.forward_messages(
                            entity=destination_entity,
                            messages=[message.id],
                            from_peer=origin_entity,
                            reply_to=main_reply_to_arg
                        )
                        log_msg = f"Successfully forwarded Message ID: {message.id} from '{origin_id}' to main destination '{destination_id}'"
                        if main_reply_to_arg:
                            log_msg += f" (Topic/ReplyTo: {main_reply_to_arg})"
                        self.logger.info(log_msg)
                    successfully_forwarded_main = True

                except telethon_errors.FloodWaitError as e_flood:
                    self.logger.warning(f"Rate limit hit (main destination). Waiting for {e_flood.seconds} seconds.")
                    await asyncio.sleep(e_flood.seconds + 1) # Add a small buffer
                    # Re-queue or re-attempt this message? For now, continue to next, effectively skipping current one on flood.
                    # A more robust solution might put it back in a queue.
                    self.logger.info(f"Skipping Message ID: {message.id} for main destination due to FloodWait. Will not be recorded as forwarded unless successfully processed later.")
                    continue
                except (ChannelPrivateError, ChatAdminRequiredError, UserBannedInChannelError) as e_perm:
                    self.logger.error(f"Permission error forwarding Message ID: {message.id} to main destination: {e_perm}")
                    continue 
                except RPCError as rpc_error:
                    self.logger.error(f"RPCError forwarding Message ID: {message.id} to main destination: {rpc_error}")
                    continue 
                except Exception as e_fwd:
                    self.logger.exception(f"Unexpected error forwarding Message ID: {message.id} to main destination: {e_fwd}")
                    continue

                # If successfully forwarded to main, record it and then handle secondary unique destination
                if successfully_forwarded_main:
                    await self._record_forwarded(message, str(origin_entity.id), str(destination_entity.id))

                    # Forward to Secondary Unique Destination (if configured and this is a unique message)
                    if self.secondary_unique_destination:
                        self.logger.info(f"Attempting to forward unique Message ID: {message.id} to secondary destination: {self.secondary_unique_destination}")
                        try:
                            secondary_dest_entity = await client.get_entity(self.secondary_unique_destination)
                            # Using forward_messages to preserve "forwarded from" header, unless prepend_origin_info is also desired for secondary
                            await client.forward_messages(
                                entity=secondary_dest_entity,
                                messages=[message.id],
                                from_peer=origin_entity
                                # reply_to: Not specified for secondary, could be added if needed
                            )
                            self.logger.info(f"Successfully forwarded unique Message ID: {message.id} to secondary destination '{self.secondary_unique_destination}'.")
                        except telethon_errors.FloodWaitError as e_flood_sec:
                            self.logger.warning(f"Rate limit hit (secondary destination: {self.secondary_unique_destination}). Waiting for {e_flood_sec.seconds} seconds.")
                            await asyncio.sleep(e_flood_sec.seconds + 1)
                            self.logger.info(f"Skipping secondary forward for Message ID: {message.id} due to FloodWait.")
                        except Exception as e_sec_fwd:
                            self.logger.error(f"Error forwarding unique Message ID: {message.id} to secondary destination '{self.secondary_unique_destination}': {e_sec_fwd}", exc_info=True)
                    
                    # Forward to Saved Messages (existing logic, applied if main forward was successful)
                    if self.forward_to_all_saved_messages:
                        self.logger.info(f"Forwarding Message ID: {message.id} to 'Saved Messages' of all configured accounts.")
                        original_main_account_id = client.session.filename

                        for acc_config in self.config.accounts:
                            saved_messages_account_id = acc_config.get("session_name") or acc_config.get("phone_number")
                            if not saved_messages_account_id:
                                self.logger.warning("Skipping an account for 'Saved Messages' forwarding due to missing identifier.")
                                continue

                            self.logger.info(f"Attempting to forward Message ID: {message.id} to 'Saved Messages' for account: {saved_messages_account_id}")

                            try:
                                if self._client and self._client.session.filename != str(Config().path.parent / saved_messages_account_id):
                                    await self.close() # Close current client if it's not for the target saved messages account

                                target_client = await self._get_client(saved_messages_account_id) # Get/switch client

                                await target_client.forward_messages(
                                    entity='me',
                                    messages=[message.id],
                                    from_peer=origin_entity
                                )
                                self.logger.info(f"Successfully forwarded Message ID: {message.id} to 'Saved Messages' for account: {saved_messages_account_id}")
                                await asyncio.sleep(1) # Small delay between accounts

                            except telethon_errors.FloodWaitError as e_flood_saved:
                                self.logger.warning(f"Rate limit hit (Saved Messages for {saved_messages_account_id}). Waiting for {e_flood_saved.seconds} seconds.")
                                await asyncio.sleep(e_flood_saved.seconds + 1)
                            except (UserDeactivatedError, AuthKeyError) as e_auth_saved:
                                self.logger.error(f"Auth error for account {saved_messages_account_id} when forwarding to Saved Messages: {e_auth_saved}. Skipping this account.")
                            except RPCError as e_rpc_saved:
                                self.logger.error(f"RPCError for account {saved_messages_account_id} when forwarding to Saved Messages: {e_rpc_saved}. Skipping this account.")
                            except Exception as e_saved:
                                self.logger.exception(f"Unexpected error for account {saved_messages_account_id} when forwarding to Saved Messages: {e_saved}. Skipping this account.")

                        # Restore client for main operation if it was changed
                        if self._client and self._client.session.filename != original_main_account_id:
                             await self.close()
                        # Next loop iteration will call _get_client with original account_identifier for main operations.


            self.logger.info(f"Finished iterating messages from {origin_id}.")

        except ValueError as e:
            self.logger.error(f"Configuration or resolution error: {e}")
            raise
        except (ChannelPrivateError, ChatAdminRequiredError) as e: # Specific Telethon errors
            self.logger.error(f"Telegram channel access error: {e}")
            raise
        except (AuthKeyError, UserDeactivatedError) as e: # Auth errors
            self.logger.error(f"Telegram authentication error with account {account_identifier or 'default'}: {e}. This account might be banned or need re-authentication.")
            # Potentially mark account as bad or require re-auth.
            # Re-raising to ensure the operation stops if auth is compromised.
            raise
        except RPCError as e: # Catch broader RPC errors not handled in the loop (e.g., during get_entity)
            self.logger.error(f"Telegram API RPCError (potentially during entity resolution or initial connection phase): {e}")
            raise
        except ConnectionError as e: # From _get_client if initial connect fails, or general connection loss
            self.logger.error(f"Connection error: {e}")
            raise
        except Exception as e:
            self.logger.exception(f"An unexpected error occurred during forwarding: {e}")
            raise
        finally:
            if client and client.is_connected():
                self.logger.info("Disconnecting Telegram client.")
                await client.disconnect()
            self._client = None # Clear cached client

    async def close(self):
        """Closes any active Telegram client connection."""
        if self._client and self._client.is_connected():
            self.logger.info("Closing client connection in AttachmentForwarder.")
            await self._client.disconnect()
            self._client = None

    async def forward_all_accessible_channels(
        self,
        destination_id: int | str,
        orchestration_account_identifier: Optional[str] = None
    ):
        """
        Forwards messages from all unique channels found in the account_channel_access table
        to the specified destination ID.

        Args:
            destination_id: The ID or username of the destination channel/chat.
            orchestration_account_identifier: Optional account to use for initial operations or if a specific
                                             channel does not have a designated account. (Currently, the
                                             account from DB is prioritized for each channel).
        """
        if not self.db:
            self.logger.error("Database instance (self.db) not available. Cannot proceed with total forward mode.")
            return

        self.logger.info(f"Starting 'Total Forward Mode'. Destination: {destination_id}")
        
        try:
            unique_channels_with_accounts = self.db.get_all_unique_channels()
        except Exception as e_db:
            self.logger.error(f"Failed to retrieve channels from database: {e_db}", exc_info=True)
            return
            
        if not unique_channels_with_accounts:
            self.logger.warning("No channels found in account_channel_access table to process for total forward mode.")
            return

        self.logger.info(f"Found {len(unique_channels_with_accounts)} unique channels to process.")
        # Conceptual: Messages from the same origin channel are processed sequentially by forward_messages.
        # Total mode processes these channels one after another.

        for channel_id, accessing_account_phone in unique_channels_with_accounts:
            self.logger.info(f"--- Processing channel ID: {channel_id} using account: {accessing_account_phone} ---")
            try:
                # Ensure any previous client (if cached and different) is cleared before potentially switching accounts.
                # The _get_client method handles reusing clients if the identifier matches an existing one.
                # If accessing_account_phone is different from the last one, _get_client will create a new client.
                # We call close() here to ensure the previous client is fully disconnected before a new one might be made for a different account.
                await self.close() 

                await self.forward_messages(
                    origin_id=channel_id, # channel_id from DB is BIGINT, compatible with int | str
                    destination_id=destination_id,
                    account_identifier=accessing_account_phone
                )
                self.logger.info(f"--- Finished processing channel ID: {channel_id} ---")
            except Exception as e_fwd_all:
                # Catching exceptions from forward_messages to ensure the loop continues
                self.logger.error(f"Failed to forward messages for channel ID {channel_id} using account {accessing_account_phone}: {e_fwd_all}", exc_info=True)
                # Ensure client is closed/reset if an error occurred mid-operation for one channel
                await self.close() 
                self.logger.info(f"Continuing to the next channel after error with channel ID: {channel_id}.")
                continue # Move to the next channel
        
        self.logger.info("'Total Forward Mode' completed.")
