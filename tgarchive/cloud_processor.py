from __future__ import annotations

import asyncio
import csv # Added for download log
import json # Added for sidecar and invitation state
import logging
from pathlib import Path
import collections
from datetime import datetime, timezone # Added for download log
import random # Added for invitation delays
from typing import List, Tuple, Set, Dict, Any # Added for type hinting

from telethon import TelegramClient, errors, functions, types
from telethon.tl.functions.channels import JoinChannelRequest # Added for joining channels
from telethon.sessions import StringSession


from .sync import Config # Assuming Config is in .sync, adjust if necessary

logger = logging.getLogger(__name__)

class CloudProcessor:
    def __init__(
        self,
        selected_account: dict,
        channels_file: str,
        output_dir: str,
        max_depth: int,
        min_files_gateway: int,
        config: Config,
    ):
        self.selected_account = selected_account
        self.channels_file_path = Path(channels_file)
        self.output_path = Path(output_dir)
        self.max_depth = max_depth
        self.min_files_gateway = min_files_gateway
        self.config = config

        self.client: TelegramClient | None = None
        self.channels_to_visit: collections.deque[tuple[str, int]] = collections.deque()
        self.visited_channels: set[str] = set()

        # Ensure output directory exists
        self.output_path.mkdir(parents=True, exist_ok=True)

        # Account Invitation System Attributes
        self.invite_queue: List[Tuple[str, Dict[str, Any]]] = []  # (channel_identifier, account_dict)
        # Load defaults from config, fallback to hardcoded if not in config for some reason
        cloud_config_settings = self.config.data.get("cloud", {})
        default_delays = { "min_seconds": 120, "max_seconds": 600, "variance": 0.3 }
        self.invite_delays = cloud_config_settings.get("invitation_delays", default_delays)

        self.invitation_state_file = self.output_path / "invitation_state.json"
        self.processed_invites: Set[str] = set()  # Stores "channel_id:account_session_name"
        self._load_invitation_state()
        
        # Define paths for text and archive files
        self.text_files_path = self.output_path / "text_files"
        self.archive_files_path = self.output_path / "archive_files"

        # Initialize download log file
        self.log_file_path = self.output_path / "cloud_download_log.csv"
        try:
            with open(self.log_file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "relative_file_path", "original_file_name", "channel_source_id", "message_id", "file_size_bytes", "mime_type"])
            logger.info(f"Initialized download log: {self.log_file_path}")
        except IOError as e:
            logger.error(f"Failed to initialize download log {self.log_file_path}: {e}")
            # Depending on desired robustness, might raise e or set a flag to disable logging

    async def initialize_client(self):
        """Creates, configures, and connects the TelegramClient."""
        session_name = self.selected_account.get("session_name")
        api_id = self.selected_account.get("api_id")
        api_hash = self.selected_account.get("api_hash")
        session_string = self.selected_account.get("session_string") # For string sessions

        if not session_name or not api_id or not api_hash:
            logger.error(
                "Selected account is missing session_name, api_id, or api_hash."
            )
            raise ValueError(
                "Account details are incomplete for client initialization."
            )

        logger.info(f"Initializing Telegram client for account: {session_name}")

        if session_string:
            self.client = TelegramClient(
                StringSession(session_string),
                api_id,
                api_hash,
                proxy=self.config.proxy_conf if self.config.use_proxy else None,
                # Adjust other parameters as in sync.py if needed
                # e.g., device_model, system_version, app_version, lang_code, system_lang_code
            )
        else: # Fallback to file-based session
             self.client = TelegramClient(
                str(Path(self.config.sessions_dir_path) / session_name), # Ensure sessions_dir_path is part of Config or accessible
                api_id,
                api_hash,
                proxy=self.config.proxy_conf if self.config.use_proxy else None,
            )


        try:
            await self.client.connect()
            if not await self.client.is_user_authorized():
                logger.error(
                    f"Client for account {session_name} is not authorized. "
                    "Please ensure the session is valid and has been authorized."
                )
                raise errors.UserNotParticipantError( # Or a custom exception
                    f"Client for account {session_name} is not authorized."
                )
            logger.info(f"Telegram client initialized and authorized for account: {session_name}")
        except errors.PhoneNumberInvalidError:
            logger.error(f"Phone number for {session_name} is invalid.")
            raise
        except errors.SessionPasswordNeededError:
            logger.error(f"2FA password needed for {session_name}. Automatic login not supported for 2FA-enabled accounts in this mode.")
            raise
        except ConnectionError as e:
            logger.error(f"Connection error for {session_name}: {e}")
            raise
        except Exception as e: # Catch other Telethon or general errors
            logger.error(f"Failed to initialize client for {session_name}: {e}")
            if self.client and self.client.is_connected():
                await self.client.disconnect()
            self.client = None # Ensure client is None if setup fails
            raise

    # --- Invitation System Methods ---
    def _load_invitation_state(self):
        """Loads processed invites from the state file."""
        try:
            if self.invitation_state_file.exists():
                with open(self.invitation_state_file, "r", encoding="utf-8") as f:
                    loaded_invites = json.load(f)
                    if isinstance(loaded_invites, list): # Expecting a list of strings
                        self.processed_invites = set(loaded_invites)
                        logger.info(f"Loaded {len(self.processed_invites)} processed invitations from state file.")
                    else:
                        logger.warning(f"Invitation state file format error: Expected a list. Starting fresh.")
                        self.processed_invites = set()
            else:
                logger.info("No invitation state file found. Starting fresh.")
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON from invitation state file {self.invitation_state_file}. Starting fresh.", exc_info=True)
            self.processed_invites = set()
        except Exception as e:
            logger.error(f"Failed to load invitation state: {e}", exc_info=True)
            self.processed_invites = set() # Default to empty set on other errors

    def _save_invitation_state(self):
        """Saves processed invites to the state file."""
        try:
            with open(self.invitation_state_file, "w", encoding="utf-8") as f:
                json.dump(list(self.processed_invites), f, indent=4) # Save as a list
            logger.debug(f"Saved {len(self.processed_invites)} processed invitations to state file.")
        except Exception as e:
            logger.error(f"Failed to save invitation state: {e}", exc_info=True)

    async def _queue_channel_invitations(self, channel_entity: types.TypeEntity):
        """Queues invitations for other configured accounts to join this channel."""
        if not self.config.data.get("cloud", {}).get("auto_invite_accounts", True):
            logger.debug("Auto-invite accounts is disabled in config. Skipping invitation queuing.")
            return

        channel_identifier = str(channel_entity.id) # Use channel ID as the canonical identifier
        channel_title = getattr(channel_entity, 'title', f"ID: {channel_identifier}")


        # Only queue if channel is public (has username) or appears to be a general accessible channel.
        # This is a heuristic; Telethon's join attempt will be the real test.
        if not hasattr(channel_entity, 'username') or not channel_entity.username:
            if not isinstance(channel_entity, (types.Channel, types.Chat)): # If not a channel or chat, less likely to be joinable by link/ID
                 logger.debug(f"Channel '{channel_title}' (ID: {channel_identifier}) is not a public channel (no username) and not a standard channel/chat type. Skipping invitation queuing.")
                 return
            logger.info(f"Channel '{channel_title}' (ID: {channel_identifier}) is not public by username, but attempting to queue invites based on its ID.")


        current_account_session_name = self.selected_account.get("session_name")

        for account_dict in self.config.accounts:
            target_account_session_name = account_dict.get("session_name")
            if not target_account_session_name:
                logger.warning("Skipping an account for invitation queue due to missing session_name.")
                continue

            if target_account_session_name == current_account_session_name:
                continue  # Skip current account

            invite_key = f"{channel_identifier}:{target_account_session_name}"

            # Check if already processed or already in queue for this specific account
            is_in_queue = any(item[0] == channel_identifier and item[1].get("session_name") == target_account_session_name for item in self.invite_queue)

            if invite_key not in self.processed_invites and not is_in_queue:
                self.invite_queue.append((channel_identifier, account_dict))
                logger.info(f"Queued invitation for account '{target_account_session_name}' to join channel '{channel_title}' (ID: {channel_identifier}).")
            elif invite_key in self.processed_invites:
                 logger.debug(f"Invitation for '{target_account_session_name}' to '{channel_title}' (ID: {channel_identifier}) already processed.")
            elif is_in_queue:
                 logger.debug(f"Invitation for '{target_account_session_name}' to '{channel_title}' (ID: {channel_identifier}) already in queue.")


    async def _process_invitation_queue(self):
        """Gradually processes pending invitations from the queue."""
        if not self.invite_queue:
            logger.info("Invitation queue is empty.")
            return

        if not self.config.data.get("cloud", {}).get("auto_invite_accounts", True):
            logger.info("Auto-invite accounts is disabled. Skipping processing of invitation queue.")
            # Clear queue if auto-invites got disabled mid-run? Or leave for next time?
            # self.invite_queue.clear() # Optional: clear queue if setting changes
            return

        logger.info(f"Processing {len(self.invite_queue)} pending invitations...")

        # Process a copy of the queue to allow modification during iteration
        items_to_process = list(self.invite_queue)

        for channel_id_to_join, account_to_invite in items_to_process:
            target_account_session_name = account_to_invite.get("session_name")
            invite_key = f"{channel_id_to_join}:{target_account_session_name}"

            # Double check if it got processed by another concurrent call or if state changed
            if invite_key in self.processed_invites:
                logger.debug(f"Skipping {invite_key} as it's already processed (checked before delay).")
                try:
                    self.invite_queue.remove((channel_id_to_join, account_to_invite))
                except ValueError:
                    pass # Item might have been removed by another part of the code if processing is concurrent
                continue

            try:
                # Calculate random delay
                min_delay = float(self.invite_delays.get('min_seconds', 120))
                max_delay = float(self.invite_delays.get('max_seconds', 600))
                variance = float(self.invite_delays.get('variance', 0.3))

                base_delay = random.uniform(min_delay, max_delay)
                actual_delay = random.uniform(base_delay * (1 - variance), base_delay * (1 + variance))

                logger.info(f"Waiting {actual_delay:.1f}s before inviting '{target_account_session_name}' to channel ID '{channel_id_to_join}'.")
                await asyncio.sleep(actual_delay)

                # Create temporary client for the inviting account
                invite_client_session_path = str(Path(self.config.sessions_dir_path) / target_account_session_name)
                api_id = account_to_invite.get("api_id")
                api_hash = account_to_invite.get("api_hash")

                if not all([target_account_session_name, api_id, api_hash]):
                    logger.error(f"Account {target_account_session_name or 'Unknown'} is missing critical details for invite. Removing from queue.")
                    self.invite_queue.remove((channel_id_to_join, account_to_invite))
                    # Mark as processed to avoid retrying if details are permanently missing
                    self.processed_invites.add(invite_key)
                    self._save_invitation_state()
                    continue

                invite_client = TelegramClient(
                    invite_client_session_path,
                    api_id,
                    api_hash,
                    proxy=self.config.proxy_conf if self.config.use_proxy else None
                )

                async with invite_client: # Manages connect and disconnect
                    if not await invite_client.is_user_authorized():
                        logger.warning(f"Account '{target_account_session_name}' is not authorized. Skipping invitation to {channel_id_to_join}.")
                        self.invite_queue.remove((channel_id_to_join, account_to_invite))
                        # Consider if this should be marked processed or retried later if auth is fixed.
                        # For now, remove and mark processed to avoid repeated auth failures.
                        self.processed_invites.add(invite_key)
                        self._save_invitation_state()
                        continue

                    logger.info(f"Attempting to join channel ID '{channel_id_to_join}' with account '{target_account_session_name}'.")
                    try:
                        # Attempt to join the channel using its ID or username/link if that's what channel_id_to_join holds
                        # Telethon's JoinChannelRequest typically wants a channel entity or input channel,
                        # but get_entity first then JoinChannelRequest is safer.
                        # However, the spec implies direct join. If channel_id_to_join is a username/link, get_entity first.
                        # For simplicity assuming channel_id_to_join is something JoinChannelRequest can handle (like actual ID or @username)

                        # Robust way: try to get entity first, then join.
                        # target_entity = await invite_client.get_entity(channel_id_to_join) # This might fail if it's a private link etc.
                        # await invite_client(JoinChannelRequest(target_entity))

                        # Direct attempt as per implied spec (might need adjustment based on what channel_id_to_join contains)
                        # If channel_id_to_join is a numerical ID, it must be an InputChannel.
                        # If it's a username, JoinChannelRequest can take it.
                        # This part is tricky because channel_id_to_join might be a public username, a private hash, or a numerical ID.
                        # For now, we assume JoinChannelRequest can handle what's passed.
                        # A common pattern is: entity = await client.get_entity(identifier); await client(JoinChannelRequest(entity))
                        # Let's refine this:
                        try:
                            target_channel_entity = await invite_client.get_entity(channel_id_to_join)
                        except ValueError: # If channel_id_to_join is a hash for a private channel, get_entity might fail directly.
                                           # In such cases, ImportChatInviteRequest is needed.
                                           # This simple implementation will only work for public channels by username/ID.
                            logger.warning(f"Could not resolve entity for '{channel_id_to_join}' with account '{target_account_session_name}'. May need ImportChatInviteRequest for private links. Skipping.")
                            self.invite_queue.remove((channel_id_to_join, account_to_invite))
                            self.processed_invites.add(invite_key) # Mark as processed because this account can't resolve it
                            self._save_invitation_state()
                            continue

                        await invite_client(JoinChannelRequest(target_channel_entity))
                        logger.info(f"Successfully invited account '{target_account_session_name}' to join channel ID '{channel_id_to_join}'.")

                        self.processed_invites.add(invite_key)
                        self.invite_queue.remove((channel_id_to_join, account_to_invite))
                        self._save_invitation_state()

                    except errors.UserAlreadyParticipantError:
                        logger.info(f"Account '{target_account_session_name}' is already a participant in channel ID '{channel_id_to_join}'.")
                        self.processed_invites.add(invite_key)
                        self.invite_queue.remove((channel_id_to_join, account_to_invite))
                        self._save_invitation_state()
                    except errors.FloodWaitError as e_flood:
                        logger.warning(f"Flood wait of {e_flood.seconds}s for account '{target_account_session_name}' when trying to join {channel_id_to_join}. Will retry later.")
                        # Adjust future delays to be more conservative
                        self.invite_delays['min_seconds'] = max(float(self.invite_delays.get('min_seconds', 120)), e_flood.seconds + 60)
                        self.invite_delays['max_seconds'] = max(float(self.invite_delays.get('max_seconds', 600)), e_flood.seconds + 120)
                        logger.info(f"Updated invitation delays: min={self.invite_delays['min_seconds']}s, max={self.invite_delays['max_seconds']}s")
                        # Do not remove from queue, it will be picked up in the next _process_invitation_queue call
                        await asyncio.sleep(e_flood.seconds + 5) # Wait out this specific flood before continuing queue processing
                    except errors.ChannelsTooMuchError:
                        logger.error(f"Account '{target_account_session_name}' is in too many channels. Cannot join {channel_id_to_join}. Marking as processed for this account.")
                        self.processed_invites.add(invite_key)
                        self.invite_queue.remove((channel_id_to_join, account_to_invite))
                        self._save_invitation_state()
                    except (errors.ChannelPrivateError, errors.ChatAdminRequiredError, errors.UserBannedInChannelError, errors.ChatWriteForbiddenError) as e_perm:
                         logger.warning(f"Permission error for '{target_account_session_name}' joining '{channel_id_to_join}': {type(e_perm).__name__}. Marking as processed for this account.")
                         self.processed_invites.add(invite_key)
                         self.invite_queue.remove((channel_id_to_join, account_to_invite))
                         self._save_invitation_state()
                    except Exception as e_join:
                        logger.error(f"Failed to invite '{target_account_session_name}' to channel ID '{channel_id_to_join}': {e_join}", exc_info=True)
                        # Decide if this should be retried or marked processed. For unknown errors, maybe retry once by not removing from queue.
                        # For now, let's leave it in queue for one more retry attempt in a subsequent process_invitation_queue call.
                        # A counter for retries per item could be added for more robustness.

            except Exception as e_outer: # Errors in the delay logic or client creation itself
                logger.error(f"Outer error processing invitation for {target_account_session_name} to {channel_id_to_join}: {e_outer}", exc_info=True)
                # Potentially remove from queue or leave for retry depending on error
                # For now, leave in queue for retry.

        logger.info(f"Finished one pass of processing invitation queue. {len(self.invite_queue)} invitations remaining.")
    # --- End Invitation System Methods ---

    async def close_client(self):
        """Disconnects the TelegramClient if it's active."""
        if self.client and self.client.is_connected():
            logger.info(f"Disconnecting client for account: {self.selected_account.get('session_name')}")
            await self.client.disconnect()
            logger.info(f"Client for account: {self.selected_account.get('session_name')} disconnected.")
        self.client = None

    def _load_initial_channels(self):
        """Loads initial channels from the specified file."""
        if not self.channels_file_path.exists():
            logger.error(f"Channels file not found: {self.channels_file_path}")
            raise FileNotFoundError(f"Channels file not found: {self.channels_file_path}")

        try:
            with open(self.channels_file_path, "r", encoding="utf-8") as f:
                for line in f:
                    channel_url = line.strip()
                    if channel_url: # Basic validation: not empty
                        self.channels_to_visit.append((channel_url, 0))
            logger.info(f"Loaded {len(self.channels_to_visit)} initial channels from {self.channels_file_path}")
        except Exception as e:
            logger.error(f"Error reading channels file {self.channels_file_path}: {e}")
            # Potentially re-raise or handle as appropriate
            raise

    async def process_channels(self):
        """
        Main processing loop for channels.
        Initializes client, loads channels, and processes them.
        """
        try:
            await self.initialize_client()
            if not self.client: # Check if client initialization failed
                logger.error("Client not initialized. Aborting process_channels.")
                return

            self._load_initial_channels()

            while self.channels_to_visit:
                channel_identifier, current_depth = self.channels_to_visit.popleft()

                if channel_identifier in self.visited_channels:
                    logger.debug(f"Skipping already visited channel: {channel_identifier}")
                    continue

                self.visited_channels.add(channel_identifier)
                logger.info(f"Processing channel: {channel_identifier} at depth {current_depth}/{self.max_depth}")

                if current_depth > self.max_depth:
                    logger.info(f"Skipping channel {channel_identifier} because it exceeds max depth ({current_depth} > {self.max_depth}).")
                    continue
                
                entity = None
                try:
                    logger.debug(f"Attempting to get entity for: {channel_identifier}")
                    entity = await self.client.get_entity(channel_identifier)
                    if not (hasattr(entity, 'broadcast') and entity.broadcast) and \
                       not (hasattr(entity, 'megagroup') and entity.megagroup) and \
                       not (isinstance(entity, types.User) and entity.bot): # Allow bots as they can be channels
                        # A more specific check for channels/supergroups:
                        if isinstance(entity, (types.Channel, types.Chat)):
                             if isinstance(entity, types.Chat) and not (entity.megagroup or getattr(entity, 'gigagroup', False)): # type: ignore
                                logger.warning(f"Skipping '{channel_identifier}' (resolved to '{getattr(entity, 'title', 'N/A')}') as it is a small group chat, not a channel/supergroup.")
                                continue
                        elif isinstance(entity, types.User): # If it's a user, it's not a channel unless it's a bot (which can act like a channel sometimes)
                             logger.warning(f"Skipping '{channel_identifier}' (resolved to user '{getattr(entity, 'username', 'N/A')}') as it is a user and not a bot.")
                             continue
                        else:
                            logger.warning(f"Skipping '{channel_identifier}' (resolved to '{getattr(entity, 'title', 'N/A')}') as it does not appear to be a channel, supergroup, or bot.")
                            continue
                    logger.info(f"Successfully accessed entity: {getattr(entity, 'title', channel_identifier)} (ID: {entity.id})")

                except errors.ChannelsTooMuchError:
                    logger.error(f"Account {self.selected_account.get('session_name')} is in too many channels/supergroups. Cannot join '{channel_identifier}'. Skipping.")
                    # Consider re-queueing with a different account if multi-account is implemented for cloud mode later
                    continue
                except (errors.ChannelPrivateError, errors.ChatAdminRequiredError, errors.UserBannedInChannelError) as e:
                    logger.warning(f"Cannot access '{channel_identifier}': {type(e).__name__}. It might be private, require admin rights, or user is banned. Skipping. Error: {e}")
                    continue
                except errors.InviteRequestSentError:
                    logger.info(f"A request to join '{channel_identifier}' has been sent. Will not process further in this run.")
                    # Add to a separate list if we want to track pending requests
                    continue
                except errors.UserAlreadyParticipantError:
                    logger.info(f"Already a participant in '{channel_identifier}'. Proceeding.")
                    # This is not an error, just information. Entity should be valid.
                    if entity is None: # Should have been fetched if this error occurs during a join attempt
                        try:
                            entity = await self.client.get_entity(channel_identifier)
                        except Exception as e_inner:
                            logger.error(f"Failed to re-fetch entity for '{channel_identifier}' after UserAlreadyParticipantError: {e_inner}. Skipping.")
                            continue
                except errors.FloodWaitError as e:
                    logger.warning(f"Flood wait error when trying to access '{channel_identifier}'. Sleeping for {e.seconds}s.")
                    await asyncio.sleep(e.seconds + 5) # Add buffer
                    self.channels_to_visit.append((channel_identifier, current_depth)) # Re-queue
                    self.visited_channels.remove(channel_identifier) # Allow re-processing by removing from visited
                    continue
                except (ValueError, TypeError) as e: # Handles invalid channel URLs/IDs (e.g. from malformed links)
                    logger.error(f"Invalid channel identifier '{channel_identifier}': {e}. Skipping.")
                    continue
                except errors.RPCError as e: # Catch other Telethon RPC errors
                    logger.error(f"RPC error when trying to access '{channel_identifier}': {e} (Code: {e.code}, Message: {e.message}). Skipping.")
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error when trying to access '{channel_identifier}': {type(e).__name__} - {e}. Skipping.")
                    continue

                if not entity: # If entity could not be fetched for any reason
                    logger.debug(f"Entity for {channel_identifier} was not successfully obtained. Skipping further processing for it.")
                    continue

                # Successfully accessed an entity, queue invitations for other accounts
                await self._queue_channel_invitations(entity)
                
                # Link Discovery
                if current_depth < self.max_depth:
                    logger.info(f"Starting link discovery in '{getattr(entity, 'title', channel_identifier)}' (depth {current_depth + 1}).")
                    messages_to_scan_for_links = 100 # Placeholder
                    try:
                        async for message in self.client.iter_messages(entity, limit=messages_to_scan_for_links):
                            if not message: continue
                            
                            found_urls_in_message = set()

                            if message.entities:
                                for msg_entity in message.entities:
                                    found_url = None
                                    if isinstance(msg_entity, types.MessageEntityTextUrl):
                                        found_url = msg_entity.url
                                    elif isinstance(msg_entity, types.MessageEntityUrl):
                                        try:
                                            found_url = message.text[msg_entity.offset : msg_entity.offset + msg_entity.length]
                                        except (TypeError, IndexError) as e:
                                            logger.debug(f"Error extracting URL from MessageEntityUrl for message {message.id} in {getattr(entity, 'title', 'N/A')}: {e}")
                                            continue
                                    
                                    if found_url:
                                        found_urls_in_message.add(found_url)

                            # Also consider message.text for raw @mentions and t.me links if not caught by entities
                            if message.text:
                                import re # Import moved here to be specific to this block
                                # Regex for t.me links (public, joinchat, "+") and @mentions
                                potential_links = re.findall(r"(?:https?://)?t\.me/(?:joinchat/|\+|resolve\?domain=)?([a-zA-Z0-9_./+-]+)|@([a-zA-Z0-9_]{5,32})", message.text)
                                for link_match in potential_links:
                                    if link_match[0]: # t.me link
                                        # Construct full t.me URL if only path is found for some regex cases (though current regex aims for full)
                                        full_url = link_match[0] if link_match[0].startswith("t.me/") else "t.me/" + link_match[0]
                                        found_urls_in_message.add(full_url)
                                    elif link_match[1]: # @mention
                                        found_urls_in_message.add("@" + link_match[1])
                            
                            for raw_url in found_urls_in_message:
                                normalized_identifier = None
                                if "t.me/joinchat/" in raw_url or "t.me/+" in raw_url:
                                    logger.debug(f"Found potential private join link: {raw_url}. These are handled by Telethon's get_entity if valid. Adding as is.")
                                    normalized_identifier = raw_url # Use as is, get_entity can handle them
                                elif "t.me/" in raw_url:
                                    # Extract username or public channel ID
                                    # Handles t.me/username, t.me/c/channel_id/message_id, t.me/username/message_id
                                    match = re.search(r"t\.me/(?:c/)?([a-zA-Z0-9_]+)", raw_url)
                                    if match:
                                        normalized_identifier = "@" + match.group(1) # Prepend @ for consistency
                                    else:
                                        logger.debug(f"Could not parse a standard username/channel ID from t.me link: {raw_url}")
                                elif raw_url.startswith("@"):
                                    normalized_identifier = raw_url
                                
                                if normalized_identifier:
                                    # Check if it's already visited or in queue to avoid redundant logging/processing
                                    is_new = normalized_identifier not in self.visited_channels
                                    is_not_in_queue = not any(item[0] == normalized_identifier for item in self.channels_to_visit)

                                    if is_new and is_not_in_queue:
                                        logger.info(f"Discovered new potential entity: {normalized_identifier} from '{getattr(entity, 'title', channel_identifier)}'. Adding to queue with depth {current_depth + 1}.")
                                        self.channels_to_visit.append((normalized_identifier, current_depth + 1))
                                    elif not is_new:
                                        logger.debug(f"Skipping already visited discovered link: {normalized_identifier}")
                                    elif not is_not_in_queue:
                                         logger.debug(f"Skipping already queued discovered link: {normalized_identifier}")

                    except errors.ChannelPrivateError:
                        logger.warning(f"Cannot iterate messages in '{getattr(entity, 'title', channel_identifier)}', channel is private or inaccessible after joining. Skipping link discovery for it.")
                    except errors.SlowModeWaitError as e:
                        logger.warning(f"Slow mode wait error in '{getattr(entity, 'title', channel_identifier)}'. Sleeping for {e.seconds}s.")
                        await asyncio.sleep(e.seconds + 2)
                    except errors.FloodWaitError as e: # Flood wait during message iteration
                        logger.warning(f"Flood wait error during message iteration in '{getattr(entity, 'title', channel_identifier)}'. Sleeping for {e.seconds}s.")
                        await asyncio.sleep(e.seconds + 5)
                        # Re-add current channel to process its messages again after flood wait
                        # But ensure it's not added if it was already processed or if it's the source of flood.
                        # For simplicity now, we just pause and continue; the channel won't be re-queued for message iteration from scratch in this version.
                    except Exception as e:
                        logger.error(f"Error during link discovery in '{getattr(entity, 'title', channel_identifier)}': {type(e).__name__} - {e}")

                # Placeholder for File Processing (Step 4) - Now to be replaced with actual logic
                # logger.info(f"Channel '{getattr(entity, 'title', channel_identifier)}' (ID: {entity.id}) is accessible and ready for file processing (Step 4).")

                # Create specific output directories for this channel's files
                self.text_files_path.mkdir(parents=True, exist_ok=True)
                self.archive_files_path.mkdir(parents=True, exist_ok=True)

                logger.info(f"Starting file download process for channel: {getattr(entity, 'title', channel_identifier)}...")
                message_count = 0
                download_count = 0
                archive_extensions = ['.zip', '.rar', '.7z', '.tar', '.gz', '.bz2']

                try:
                    async for message in self.client.iter_messages(entity, limit=None): # Process all messages
                        message_count += 1
                        if message_count % 100 == 0: # Log progress periodically
                            logger.info(f"Scanned {message_count} messages in {getattr(entity, 'title', channel_identifier)}...")

                        if not message.file or not message.file.name: # Ensure there's a file and it has a name
                            continue
                        
                        original_file_name = message.file.name
                        mime_type = message.file.mime_type.lower() if message.file.mime_type else ''

                        if mime_type == 'text/plain':
                            filepath = self.text_files_path / original_file_name
                            try:
                                logger.info(f"Downloading text file: {original_file_name} to {filepath}")
                                await self.client.download_media(message.media, file=filepath)
                                download_count += 1
                                self._append_to_download_log(
                                    original_file_name=original_file_name,
                                    saved_filepath=filepath,
                                    channel_id=str(getattr(entity, 'username', entity.id)),
                                    message_id=message.id,
                                    file_size=message.file.size,
                                    mime_type=message.file.mime_type
                                )
                            except errors.FloodWaitError as e_flood:
                                logger.warning(f"Flood wait during text file download '{original_file_name}': {e_flood.seconds}s. Sleeping.")
                                await asyncio.sleep(e_flood.seconds + 5)
                                # Optionally re-try or skip this specific file
                            except Exception as e_download:
                                logger.error(f"Error downloading text file {original_file_name}: {e_download}")
                        
                        elif original_file_name.lower().endswith(tuple(archive_extensions)):
                            filepath = self.archive_files_path / original_file_name
                            try:
                                logger.info(f"Downloading archive file: {original_file_name} to {filepath}")
                                await self.client.download_media(message.media, file=filepath)
                                download_count += 1
                                logger.info(f"Writing sidecar for {original_file_name}")
                                await self._write_archive_sidecar(message, filepath)
                                self._append_to_download_log(
                                    original_file_name=original_file_name,
                                    saved_filepath=filepath,
                                    channel_id=str(getattr(entity, 'username', entity.id)),
                                    message_id=message.id,
                                    file_size=message.file.size,
                                    mime_type=message.file.mime_type
                                )
                            except errors.FloodWaitError as e_flood:
                                logger.warning(f"Flood wait during archive download '{original_file_name}': {e_flood.seconds}s. Sleeping.")
                                await asyncio.sleep(e_flood.seconds + 5)
                            except Exception as e_download:
                                logger.error(f"Error downloading archive file {original_file_name}: {e_download}")
                    
                    logger.info(f"Finished scanning {message_count} messages in {getattr(entity, 'title', channel_identifier)}. Downloaded {download_count} files.")

                except errors.ChannelPrivateError: # Error during message iteration itself
                    logger.warning(f"Cannot iterate messages in '{getattr(entity, 'title', channel_identifier)}' for file download, channel became private or inaccessible. Skipping file download for it.")
                except errors.SlowModeWaitError as e:
                    logger.warning(f"Slow mode wait error in '{getattr(entity, 'title', channel_identifier)}' during file downloads. Sleeping for {e.seconds}s.")
                    await asyncio.sleep(e.seconds + 2)
                except errors.FloodWaitError as e: 
                    logger.warning(f"Flood wait error during message iteration for file downloads in '{getattr(entity, 'title', channel_identifier)}'. Sleeping for {e.seconds}s.")
                    await asyncio.sleep(e.seconds + 5)
                except Exception as e:
                    logger.error(f"Error during file download process in '{getattr(entity, 'title', channel_identifier)}': {type(e).__name__} - {e}")


        except FileNotFoundError:
            logger.error("Cannot start processing, channels file was not found.")
            return 
        except errors.UserNotParticipantError as e:
             logger.error(f"Client authorization error: {e}. This might indicate the session is no longer valid. Aborting.")
        except Exception as e:
            logger.error(f"An critical error occurred during the main channel processing loop: {e}", exc_info=True)
        finally:
            # After processing all channels in the current run, process any pending invitations
            logger.info("Main channel processing loop finished. Processing invitation queue...")
            await self._process_invitation_queue()
            await self.close_client()
            logger.info("Cloud processing finished.")

    async def _write_archive_sidecar(self, msg: types.Message, file_path: Path):
        """Writes a JSON sidecar file for a downloaded archive."""
        meta = {
            "message_id": msg.id,
            "date": msg.date.isoformat() if msg.date else None,
            "sender_id": msg.sender_id,
            "sender_username": msg.sender.username if msg.sender and hasattr(msg.sender, 'username') else None,
            "reply_to_msg_id": msg.reply_to_msg_id if msg.reply_to else None,
            "message_text": msg.message, # Raw text content of the message
            "file_name": msg.file.name if msg.file else None,
            "file_size_bytes": msg.file.size if msg.file else None,
            "mime_type": msg.file.mime_type if msg.file else None,
            "original_url": f"tg://channel/{msg.chat_id}/message/{msg.id}" if msg.chat_id else None # Basic TG link
        }
        sidecar_path = file_path.with_suffix(file_path.suffix + ".json")
        try:
            with open(sidecar_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=4, ensure_ascii=False)
            logger.debug(f"Successfully wrote sidecar file: {sidecar_path}")
        except Exception as e:
            logger.error(f"Error writing sidecar file {sidecar_path}: {e}")

    def _append_to_download_log(self, original_file_name: str, saved_filepath: Path, channel_id: str, message_id: int, file_size: int, mime_type: str | None):
        """Appends a record to the download log CSV file."""
        timestamp = datetime.now(timezone.utc).isoformat()
        try:
            # Ensure saved_filepath is absolute before making it relative to self.output_path
            if not saved_filepath.is_absolute():
                 # This case should ideally not happen if filepath is constructed from self.text_files_path etc.
                logger.warning(f"Saved filepath for log is not absolute: {saved_filepath}. Storing as is.")
                relative_path = str(saved_filepath)
            else:
                relative_path = str(saved_filepath.relative_to(self.output_path))
        except ValueError:
            # If saved_filepath is not under self.output_path for some unexpected reason
            logger.warning(f"Saved filepath {saved_filepath} is not relative to output path {self.output_path}. Storing absolute path.")
            relative_path = str(saved_filepath)

        row = [
            timestamp,
            relative_path,
            original_file_name,
            str(channel_id), # Ensure channel_id is a string
            message_id,
            file_size,
            mime_type if mime_type else "" # Handle None mime_type
        ]
        
        try:
            # Check if log file was initialized
            if not hasattr(self, 'log_file_path') or not self.log_file_path.exists():
                logger.error(f"Download log file {getattr(self, 'log_file_path', 'Not Defined')} not initialized or found. Cannot append record.")
                # Re-initialize header if file gone? Or just error out?
                # For now, just error out.
                return

            with open(self.log_file_path, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(row)
        except IOError as e:
            logger.error(f"Failed to append to download log {self.log_file_path}: {e}")
        except Exception as e: # Catch any other unexpected error during logging
            logger.error(f"Unexpected error when appending to download log: {e}")


# Example of how it might be called (for testing, not part of the class itself)
async def main_test():
    # This is a mock config and account for testing purposes
    class MockConfig:
        def __init__(self):
            self.proxy_conf = None
            self.use_proxy = False
            self.sessions_dir_path = Path("./sessions") # Ensure this dir exists or adjust
            # Add other necessary config attributes if your Config class has them
            self.data = {"accounts": []} # if Config expects this

    mock_config = MockConfig()
    
    # IMPORTANT: Replace with actual API ID, HASH, and SESSION NAME for testing
    # It's better to load these from a test config file or environment variables
    mock_selected_account = {
        "session_name": "test_session", # This will create 'test_session.session'
        "api_id": 12345,  # Replace with your API ID
        "api_hash": "0123456789abcdef0123456789abcdef",  # Replace with your API Hash
        # "session_string": "YOUR_SESSION_STRING" # Optional: if using string sessions
    }
    
    # Create a dummy channels file
    channels_file = Path("test_channels.txt")
    with open(channels_file, "w") as f:
        f.write("https://t.me/somechannel\n")
        f.write("anotherchannelusername\n")

    output_dir = Path("test_cloud_output")
    output_dir.mkdir(exist_ok=True)

    # Setup basic logging for the test
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    processor = CloudProcessor(
        selected_account=mock_selected_account,
        channels_file=str(channels_file),
        output_dir=str(output_dir),
        max_depth=2,
        min_files_gateway=100,
        config=mock_config # Pass the mock config
    )
    
    try:
        # For file-based sessions, Telethon will prompt for phone number/code interactively
        # if the session file doesn't exist or is invalid.
        # For string sessions, ensure the string is valid.
        logger.info("Attempting to run CloudProcessor. If this is the first run for a file session, Telethon might ask for login details.")
        await processor.process_channels()
    except Exception as e:
        logger.error(f"Error during test run: {e}")
    finally:
        # Clean up dummy files
        if channels_file.exists():
            channels_file.unlink()
        # You might want to inspect output_dir before removing it
        # import shutil
        # if output_dir.exists():
        # shutil.rmtree(output_dir)
        pass

if __name__ == "__main__":
    # This is for manual testing of the CloudProcessor structure.
    # Ensure you have a valid Telethon session or are ready to create one.
    # asyncio.run(main_test())
    logger.info("CloudProcessor structure defined. To test, uncomment asyncio.run(main_test()) and configure mock_selected_account.")
    logger.info("Remember to replace mock API ID and Hash, and be ready for interactive login if session is new.")
    pass
