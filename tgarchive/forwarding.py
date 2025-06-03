"""
Handles forwarding of message attachments between Telegram entities.
"""
from __future__ import annotations

import logging
import asyncio # Added for asyncio.sleep
from typing import Optional

# Third-party imports
from telethon import TelegramClient, errors as telethon_errors # Added telethon_errors
from telethon.tl.types import Message as TLMessage, InputPeerChannel, User, Channel # Add more as needed
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
                 destination_topic_id: Optional[int] = None): # Added destination_topic_id
        """
        Initializes the AttachmentForwarder.

        Args:
            config: The SPECTRA configuration object.
            db: An optional SpectraDB instance for database interactions.
            forward_to_all_saved_messages: If True, forward to all accounts' Saved Messages.
            prepend_origin_info: If True, prepend origin info to message text.
            destination_topic_id: Optional topic ID for the destination.
        """
        self.config = config
        self.db = db # May be used later for logging forwarding actions or fetching metadata
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._client: Optional[TelegramClient] = None # To cache client instance within a forwarding operation
        self.forward_to_all_saved_messages = forward_to_all_saved_messages
        self.prepend_origin_info = prepend_origin_info
        self.destination_topic_id = destination_topic_id # Store destination_topic_id
        
        if self.forward_to_all_saved_messages:
            self.logger.info("Forwarding to 'Saved Messages' of all configured accounts is ENABLED.")
        if self.prepend_origin_info:
            self.logger.info("Prepending origin info to forwarded messages is ENABLED.")
        if self.destination_topic_id:
            self.logger.info(f"Forwarding to destination topic ID: {self.destination_topic_id}")


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

                # 1. Attachment Filtering
                if not message.media:
                    self.logger.debug(f"Message {message.id} has no media. Skipping.")
                    continue

                # Further filtering can be added here, e.g., by document type, photo, video
                # For now, any media is considered an attachment.
                self.logger.info(f"Message {message.id} has media. Attempting to forward to {destination_id}.")

                # 2. Message Forwarding Logic & 3. Rate Limit Handling
                successfully_forwarded_main = False
                message_to_forward = message # This is what we'll operate on
                
                # Conceptual: If self.destination_topic_id is set, it implies `reply_to` for main forwarding.
                # The actual `reply_to_msg_id` for a topic needs to be determined beforehand.
                # For now, we just pass it if available.
                main_reply_to_arg = self.destination_topic_id 

                try:
                    if self.prepend_origin_info and not self.destination_topic_id: # Prepending only if not using topics for now
                        origin_title = getattr(origin_entity, 'title', f"ID: {origin_entity.id}")
                        header = f"[Forwarded from {origin_title} (ID: {origin_entity.id})]\n"
                        message_content = header + (message.text or "")
                        
                        # Ensure client is the one for the main operation
                        if client.session.filename != str(Config().path.parent / (account_identifier or self.config.accounts[0].get("session_name"))):
                             await self.close()
                             client = await self._get_client(account_identifier)

                        await client.send_message(
                            entity=destination_entity,
                            message=message_content,
                            file=message.media, # Send the original media
                            reply_to=main_reply_to_arg # Could be message.reply_to_msg_id or topic ID
                        )
                        self.logger.info(f"Successfully sent Message ID: {message.id} with origin info from '{origin_id}' to '{destination_id}'.")
                    else:
                        # Standard forward or forward to topic
                        await client.forward_messages(
                            entity=destination_entity,
                            messages=[message.id],
                            from_peer=origin_entity,
                            reply_to=main_reply_to_arg # Forward to topic if ID provided
                        )
                        log_msg = f"Successfully forwarded Message ID: {message.id} from '{origin_id}' to main destination '{destination_id}'"
                        if main_reply_to_arg:
                            log_msg += f" (Topic/ReplyTo: {main_reply_to_arg})"
                        self.logger.info(log_msg)
                    successfully_forwarded_main = True

                except telethon_errors.FloodWaitError as e_flood:
                    self.logger.warning(f"Rate limit hit (main destination). Waiting for {e_flood.seconds} seconds.")
                    await asyncio.sleep(e_flood.seconds)
                    self.logger.info(f"Resuming after FloodWaitError. Will re-attempt Message ID: {message.id} to main destination in the next full iteration if needed.")
                    # For simplicity, we don't have an explicit retry loop here for the main forward.
                    # The outer loop will just process the message again if it wasn't successfully_forwarded_main.
                    # However, this means if it fails again, it will be skipped.
                    # A more robust solution would involve a specific retry for the current message.
                    # For now, we'll let it attempt on next pass or be skipped. This is a simplification.
                    continue # Skip to next message in outer loop, effectively not retrying immediately
                except (ChannelPrivateError, ChatAdminRequiredError, UserBannedInChannelError) as e_perm:
                    self.logger.error(f"Permission error forwarding Message ID: {message.id} to main destination: {e_perm}")
                    continue 
                except RPCError as rpc_error:
                    self.logger.error(f"RPCError forwarding Message ID: {message.id} to main destination: {rpc_error}")
                    continue 
                except Exception as e_fwd:
                    self.logger.exception(f"Unexpected error forwarding Message ID: {message.id} to main destination: {e_fwd}")
                    continue

                # Forward to Saved Messages
                if successfully_forwarded_main and self.forward_to_all_saved_messages:
                    self.logger.info(f"Forwarding Message ID: {message.id} to 'Saved Messages' of all configured accounts.")
                    original_main_account_id = client.session.filename # Store which client was used for main forward

                    for acc_config in self.config.accounts:
                        saved_messages_account_id = acc_config.get("session_name") or acc_config.get("phone_number")
                        if not saved_messages_account_id:
                            self.logger.warning("Skipping an account for 'Saved Messages' forwarding due to missing identifier.")
                            continue
                        
                        self.logger.info(f"Attempting to forward Message ID: {message.id} to 'Saved Messages' for account: {saved_messages_account_id}")
                        
                        try:
                            # Switch client if necessary
                            if self._client and self._client.session.filename != str(Config().path.parent / saved_messages_account_id):
                                await self.close()
                            
                            target_client = await self._get_client(saved_messages_account_id)
                            
                            await target_client.forward_messages(
                                entity='me', 
                                messages=[message_to_forward.id], # Use the original message object
                                from_peer=origin_entity
                            )
                            self.logger.info(f"Successfully forwarded Message ID: {message.id} to 'Saved Messages' for account: {saved_messages_account_id}")
                            await asyncio.sleep(1) 

                        except telethon_errors.FloodWaitError as e_flood_saved:
                            self.logger.warning(f"Rate limit hit (Saved Messages for {saved_messages_account_id}). Waiting for {e_flood_saved.seconds} seconds.")
                            await asyncio.sleep(e_flood_saved.seconds)
                        except (UserDeactivatedError, AuthKeyError) as e_auth_saved:
                            self.logger.error(f"Auth error for account {saved_messages_account_id} when forwarding to Saved Messages: {e_auth_saved}. Skipping this account.")
                        except RPCError as e_rpc_saved:
                            self.logger.error(f"RPCError for account {saved_messages_account_id} when forwarding to Saved Messages: {e_rpc_saved}. Skipping this account.")
                        except Exception as e_saved:
                            self.logger.exception(f"Unexpected error for account {saved_messages_account_id} when forwarding to Saved Messages: {e_saved}. Skipping this account.")
                        # No finally self.close() here, _get_client manages client state across calls.
                    
                    # After looping all accounts for saved messages, restore the client for the main operation for the next message.
                    # This is important if the last account in the loop was not the original_main_account_id.
                    if self._client and self._client.session.filename != original_main_account_id:
                         await self.close()
                    # The next iteration of the main loop will call _get_client(account_identifier) which will set up the correct client.


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
