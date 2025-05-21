"""
SPECTRA-003 — Telegram Network Discovery + Archiving
=====================================================
*Group Crawler* · *Network Analysis* · *Auto-Join* · *Batch Archive*

Integration layer between SPECTRA's archiving capabilities and
telegram-groups-crawler's discovery functionality.
"""
from __future__ import annotations

# ── Standard Library ──────────────────────────────────────────────────────
import asyncio
import itertools
import json
import logging
import os
import re
import time
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

# ── Third-party ───────────────────────────────────────────────────────────
import networkx as nx
import pandas as pd
from matplotlib import pyplot as plt
from rich.console import Console
from telethon import TelegramClient, errors
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import (
    CheckChatInviteRequest,
    ImportChatInviteRequest,
)

# ── Local Imports ──────────────────────────────────────────────────────────
from .sync import Config, runner, logger

# ── Globals ───────────────────────────────────────────────────────────────
TZ = timezone.utc
console = Console()

# ── Link Patterns ──────────────────────────────────────────────────────────
USERNAME_PATTERN = re.compile(r"@([A-Za-z0-9_]{5,32})")
INVITE_LINK_PATTERN = re.compile(r'(?:https?://)?t\.me/(?:joinchat/|[^/]+\?|\\+)([a-zA-Z0-9_-]+)')

# ── Group Discovery ─────────────────────────────────────────────────────────
class GroupDiscovery:
    """Parse and extract Telegram group links from messages"""
    
    def __init__(self, client: TelegramClient = None, data_dir: Path = None):
        self.client = client
        self.data_dir = data_dir or Path("spectra_data")
        self.data_dir.mkdir(exist_ok=True)
        self.groups_db = self.data_dir / "discovered_groups.json"
        self.discovered_groups: Set[str] = set()
        self.load_cached_groups()
        
    def load_cached_groups(self) -> Set[str]:
        """Load previously discovered groups from cache"""
        if self.groups_db.exists():
            try:
                data = json.loads(self.groups_db.read_text())
                self.discovered_groups = set(data.get("groups", []))
                logger.info(f"Loaded {len(self.discovered_groups)} groups from cache")
            except Exception as e:
                logger.error(f"Failed to load cached groups: {e}")
                self.discovered_groups = set()
        return self.discovered_groups
        
    def save_discovered_groups(self):
        """Save discovered groups to cache"""
        try:
            self.groups_db.write_text(json.dumps({
                "groups": list(self.discovered_groups),
                "last_updated": datetime.now(TZ).isoformat()
            }, indent=2))
            logger.info(f"Saved {len(self.discovered_groups)} groups to cache")
        except Exception as e:
            logger.error(f"Failed to save discovered groups: {e}")
    
    async def extract_from_entity(self, entity_id, limit=1000) -> Set[str]:
        """Extract all Telegram links from a channel/group"""
        if not self.client:
            logger.error("No client available for link extraction")
            return set()
            
        new_links = set()
        try:
            async for message in self.client.iter_messages(entity_id, limit=limit):
                if not message.text:
                    continue
                    
                # Extract @usernames
                usernames = USERNAME_PATTERN.findall(message.text)
                for username in usernames:
                    new_links.add(f"@{username}")
                    
                # Extract t.me links
                invite_matches = INVITE_LINK_PATTERN.findall(message.text)
                for invite_hash in invite_matches:
                    new_links.add(f"https://t.me/joinchat/{invite_hash}")
            
            # Add to discovered groups
            self.discovered_groups.update(new_links)
            logger.info(f"Extracted {len(new_links)} links from entity {entity_id}")
            self.save_discovered_groups()
            return new_links
            
        except Exception as e:
            logger.error(f"Failed to extract links from {entity_id}: {e}")
            return set()
            
    async def load_crawler_data(self, crawler_dir: Path = None) -> Set[str]:
        """Load groups discovered by telegram-groups-crawler"""
        if not crawler_dir:
            crawler_dir = Path.cwd() / "telegram-groups-crawler"
            
        try:
            # Try to load crawler dataframes
            df_groups = pd.read_pickle(crawler_dir / 'groups')
            df_tbp = pd.read_pickle(crawler_dir / 'to_be_processed')
            
            # Extract groups from dataframe
            for row in df_groups.values.tolist():
                group_id = str(row[0])
                username = row[2]
                
                if username:
                    self.discovered_groups.add(f"@{username}")
                else:
                    self.discovered_groups.add(group_id)
                    
            # Add to-be-processed groups from crawler
            for row in df_tbp.values.tolist():
                if row and row[0]:
                    # These are usually invite hashes
                    self.discovered_groups.add(f"https://t.me/joinchat/{row[0]}")
                    
            logger.info(f"Loaded {len(self.discovered_groups)} groups from crawler data")
            self.save_discovered_groups()
            return self.discovered_groups
            
        except Exception as e:
            logger.error(f"Failed to load crawler data: {e}")
            return set()
            
    def export_groups_to_file(self, output_path: str = "spectra_groups.txt"):
        """Export discovered groups to a file"""
        try:
            output_file = Path(output_path)
            with open(output_file, "w") as f:
                for group in sorted(self.discovered_groups):
                    f.write(f"{group}\n")
            logger.info(f"Exported {len(self.discovered_groups)} groups to {output_path}")
            return output_file
        except Exception as e:
            logger.error(f"Failed to export groups: {e}")
            return None

# ── Network Analysis ─────────────────────────────────────────────────────────
class NetworkAnalyzer:
    """Analyze connections between Telegram groups"""
    
    def __init__(self, data_dir: Path = None):
        self.data_dir = data_dir or Path("spectra_data")
        self.data_dir.mkdir(exist_ok=True)
        self.graph = nx.DiGraph()
        self.metrics = {}
    
    def load_crawler_graph(self, crawler_dir: Path = None) -> bool:
        """Load network data from telegram-groups-crawler"""
        if not crawler_dir:
            crawler_dir = Path.cwd() / "telegram-groups-crawler"
            
        try:
            # Try to load crawler dataframes
            df_edges = pd.read_pickle(crawler_dir / 'edges')
            df_groups = pd.read_pickle(crawler_dir / 'groups')
            
            # Create name mapping
            name_map = {}
            for row in df_groups.values.tolist():
                group_id = str(row[0])
                name = row[1] or f"Group_{group_id}"
                username = row[2]
                
                # Use username as identifier if available
                identifier = f"@{username}" if username else group_id
                name_map[group_id] = {"name": name, "id": identifier}
            
            # Clear existing graph
            self.graph.clear()
            
            # Add edges to graph
            for _, row in df_edges.iterrows():
                dest = str(row['destination vertex'])
                origins = row['origin vertices']
                
                for origin in origins:
                    origin_str = str(origin)
                    
                    # Get names from mapping
                    origin_data = name_map.get(origin_str, {"name": f"Unknown_{origin_str}", "id": origin_str})
                    dest_data = name_map.get(dest, {"name": f"Unknown_{dest}", "id": dest})
                    
                    # Add edge with metadata
                    self.graph.add_edge(
                        origin_data["id"],
                        dest_data["id"],
                        origin_name=origin_data["name"],
                        dest_name=dest_data["name"]
                    )
            
            logger.info(f"Loaded network with {self.graph.number_of_nodes()} nodes and {self.graph.number_of_edges()} edges")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load crawler graph: {e}")
            return False
    
    def calculate_metrics(self) -> Dict[str, Dict[str, float]]:
        """Calculate centrality metrics to identify important groups"""
        if not self.graph.nodes():
            logger.warning("No graph loaded. Load graph data first.")
            return {}
            
        # Calculate metrics
        self.metrics = {
            'degree': nx.degree_centrality(self.graph),
            'in_degree': nx.in_degree_centrality(self.graph),
            'betweenness': nx.betweenness_centrality(self.graph),
            'pagerank': nx.pagerank(self.graph, alpha=0.9)
        }
        
        # Combined score (normalized)
        combined = {}
        for node in self.graph.nodes():
            combined[node] = (
                self.metrics['in_degree'].get(node, 0) * 0.4 + 
                self.metrics['betweenness'].get(node, 0) * 0.3 + 
                self.metrics['pagerank'].get(node, 0) * 0.3
            )
        
        self.metrics['combined'] = combined
        logger.info("Calculated network metrics")
        return self.metrics
    
    def export_priority_targets(self, top_n=20, output_file=None) -> List[Dict[str, Any]]:
        """Export top groups by importance for SPECTRA archiving"""
        if not self.metrics:
            self.calculate_metrics()
            
        # Sort by combined score
        sorted_groups = sorted(
            self.metrics['combined'].items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:top_n]
        
        # Create target list
        targets = [{
            "id": group,
            "score": round(score, 4),
            "in_degree": round(self.metrics['in_degree'].get(group, 0), 4),
            "betweenness": round(self.metrics['betweenness'].get(group, 0), 4),
            "pagerank": round(self.metrics['pagerank'].get(group, 0), 4)
        } for group, score in sorted_groups]
        
        # Save to file if requested
        if output_file:
            output_path = self.data_dir / output_file
            try:
                with open(output_path, 'w') as f:
                    json.dump(targets, f, indent=2)
                logger.info(f"Exported {len(targets)} priority targets to {output_file}")
            except Exception as e:
                logger.error(f"Failed to export targets: {e}")
            
        return targets
        
    def plot_network(self, output_path="telegram_network.png", metric='combined', 
                    show_labels=True, min_size=100, max_size=2000) -> Optional[Path]:
        """Visualize the network with node sizes based on importance"""
        if not self.graph.nodes():
            logger.warning("No graph loaded. Load graph data first.")
            return None
            
        if not self.metrics:
            self.calculate_metrics()
            
        try:
            # Set up plot
            plt.figure(figsize=(16, 12))
            
            # Get metric values
            metric_values = self.metrics[metric]
            
            # Node sizes based on selected metric
            min_val = min(metric_values.values()) if metric_values else 0.01
            max_val = max(metric_values.values()) if metric_values else 0.1
            size_range = max_size - min_size
            
            # Calculate node sizes with normalization
            sizes = []
            for node in self.graph.nodes():
                val = metric_values.get(node, min_val)
                # Normalize to [0,1] range
                normalized = (val - min_val) / (max_val - min_val) if max_val > min_val else 0.5
                # Scale to desired size range
                sizes.append(min_size + normalized * size_range)
            
            # Layout
            pos = nx.spring_layout(self.graph, k=0.3, iterations=50, seed=42)
            
            # Draw graph
            nx.draw_networkx(
                self.graph, pos, 
                with_labels=show_labels,
                node_size=sizes,
                node_color="lightblue",
                font_size=8,
                font_weight="bold",
                arrows=True,
                edge_color="gray",
                alpha=0.8,
                connectionstyle="arc3,rad=0.1"
            )
            
            # Title and styling
            plt.title(f"Telegram Groups Network (sized by {metric})")
            plt.axis('off')
            plt.tight_layout()
            
            # Save figure
            output_file = self.data_dir / output_path
            plt.savefig(output_file, dpi=300, bbox_inches="tight")
            plt.close()
            
            logger.info(f"Network visualization saved to {output_file}")
            return output_file
            
        except Exception as e:
            logger.error(f"Failed to plot network: {e}")
            return None

# ── Account Rotation ──────────────────────────────────────────────────────
class AccountRotator:
    """Rotates through Telegram API accounts to distribute load and increase resilience"""
    
    def __init__(self, accounts: List[Dict[str, Any]], rotation_mode: str = "sequential", db_path: Optional[Path] = None):
        """
        Initialize account rotator with accounts list
        
        Args:
            accounts: List of account configurations with api_id, api_hash, session_name
            rotation_mode: How to select accounts - "sequential", "random", "weighted", or "smart"
            db_path: Optional path to SQLite database for persisting account usage data
        """
        self.accounts = accounts
        self.rotation_mode = rotation_mode
        self.current_index = 0
        self.usage_counts = {idx: 0 for idx in range(len(accounts))}
        self.last_used = {idx: datetime.now(TZ) for idx in range(len(accounts))}
        self._iterator = itertools.cycle(range(len(accounts)))
        self.db_path = db_path
        self.db = None
        
        # Set up database connection if path provided
        if self.db_path:
            self._setup_db()
            self._load_account_stats()
        
        # Validate accounts
        for idx, acc in enumerate(self.accounts):
            if not all(k in acc for k in ["api_id", "api_hash", "session_name"]):
                logger.warning(f"Account {idx} is missing required fields - skipping")
                self.usage_counts[idx] = float('inf')  # Mark as unusable
    
    def _setup_db(self):
        """Set up account usage database if not exists"""
        from .db import SpectraDB
        try:
            self.db = SpectraDB(self.db_path)
            
            # Create accounts table if not exists
            self.db.conn.execute("""
                CREATE TABLE IF NOT EXISTS account_rotation (
                    session_name TEXT PRIMARY KEY,
                    api_id INTEGER,
                    api_hash TEXT,
                    usage_count INTEGER DEFAULT 0,
                    last_used TEXT,
                    last_error TEXT,
                    cooldown_until TEXT,
                    is_banned BOOLEAN DEFAULT 0,
                    flood_wait_count INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0
                )
            """)
            self.db.conn.commit()
            logger.info(f"Account rotation DB initialized at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize account rotation database: {e}")
            self.db = None
    
    def _load_account_stats(self):
        """Load account usage statistics from database"""
        if not self.db:
            return
            
        try:
            # First, ensure all accounts are in the database
            for idx, acc in enumerate(self.accounts):
                if "session_name" not in acc:
                    continue
                    
                # Check if account exists in DB
                exists = self.db.conn.execute(
                    "SELECT 1 FROM account_rotation WHERE session_name = ?", 
                    (acc["session_name"],)
                ).fetchone()
                
                if not exists:
                    # Insert new account record
                    self.db.conn.execute(
                        """
                        INSERT INTO account_rotation 
                        (session_name, api_id, api_hash, usage_count, last_used)
                        VALUES (?, ?, ?, 0, ?)
                        """,
                        (
                            acc["session_name"], 
                            acc.get("api_id", 0), 
                            acc.get("api_hash", ""), 
                            datetime.now(TZ).isoformat()
                        )
                    )
            
            # Commit any inserts
            self.db.conn.commit()
            
            # Now load stats for all accounts
            for idx, acc in enumerate(self.accounts):
                if "session_name" not in acc:
                    continue
                
                row = self.db.conn.execute(
                    "SELECT usage_count, last_used, is_banned, cooldown_until FROM account_rotation WHERE session_name = ?",
                    (acc["session_name"],)
                ).fetchone()
                
                if row:
                    self.usage_counts[idx] = row[0]
                    
                    # Parse last_used timestamp
                    if row[1]:
                        try:
                            self.last_used[idx] = datetime.fromisoformat(row[1])
                        except (ValueError, TypeError):
                            self.last_used[idx] = datetime.now(TZ)
                    
                    # Check if account is banned or in cooldown
                    if row[2] or (row[3] and datetime.fromisoformat(row[3]) > datetime.now(TZ)):
                        self.usage_counts[idx] = float('inf')  # Mark as unusable
                        
            logger.info(f"Loaded usage statistics for {len(self.accounts)} accounts")
        except Exception as e:
            logger.error(f"Failed to load account statistics: {e}")
    
    def _save_account_stats(self, idx: int):
        """Persist account usage statistics to database"""
        if not self.db or idx >= len(self.accounts) or "session_name" not in self.accounts[idx]:
            return
            
        try:
            self.db.conn.execute(
                """
                UPDATE account_rotation SET
                usage_count = ?,
                last_used = ?
                WHERE session_name = ?
                """,
                (
                    self.usage_counts[idx],
                    self.last_used[idx].isoformat(),
                    self.accounts[idx]["session_name"]
                )
            )
            self.db.conn.commit()
        except Exception as e:
            logger.error(f"Failed to save account statistics: {e}")
    
    def get_next_account(self) -> Dict[str, Any]:
        """Get the next account based on rotation mode"""
        if self.rotation_mode == "random":
            # Randomly select from available accounts
            available_idx = [idx for idx, count in self.usage_counts.items() 
                            if count < float('inf')]
            if not available_idx:
                logger.error("No accounts available for rotation")
                return self.accounts[0]  # Return first account as fallback
                
            selected_idx = random.choice(available_idx)
            
        elif self.rotation_mode == "weighted":
            # Select account with least usage
            available_idx = [idx for idx, count in self.usage_counts.items() 
                            if count < float('inf')]
            if not available_idx:
                logger.error("No accounts available for rotation")
                return self.accounts[0]  # Return first account as fallback
                
            selected_idx = min(available_idx, key=lambda idx: self.usage_counts[idx])
            
        elif self.rotation_mode == "smart":
            # Smart selection based on time since last use and usage count
            available_idx = [idx for idx, count in self.usage_counts.items() 
                           if count < float('inf')]
            
            if not available_idx:
                logger.error("No accounts available for rotation")
                return self.accounts[0]  # Return first account as fallback
            
            # Calculate a score based on time since last use and usage count
            now = datetime.now(TZ)
            scores = {}
            for idx in available_idx:
                time_factor = (now - self.last_used[idx]).total_seconds() / 3600  # Hours since last use
                usage_factor = 1 / (self.usage_counts[idx] + 1)  # Inverse of usage count
                scores[idx] = time_factor * 0.7 + usage_factor * 0.3  # Weighted combination
            
            selected_idx = max(scores, key=scores.get)
            
        else:  # sequential (default)
            # Get next in cycle
            selected_idx = next(self._iterator)
            # Skip accounts marked as unusable
            while self.usage_counts[selected_idx] == float('inf'):
                selected_idx = next(self._iterator)
        
        # Update usage stats
        self.usage_counts[selected_idx] += 1
        self.last_used[selected_idx] = datetime.now(TZ)
        self.current_index = selected_idx
        
        # Save to database if configured
        self._save_account_stats(selected_idx)
        
        logger.info(f"Selected account: {self.accounts[selected_idx]['session_name']}")
        return self.accounts[selected_idx]
    
    def mark_account_failed(self, idx: int = None, error: str = None, cooldown_hours: float = None):
        """
        Mark an account as failed to avoid using it
        
        Args:
            idx: Index of account to mark as failed (defaults to current)
            error: Optional error message to record
            cooldown_hours: Optional cooldown period in hours before account can be used again
        """
        if idx is None:
            idx = self.current_index
            
        # Update local state
        self.usage_counts[idx] = float('inf')
        
        # Update database if configured
        if self.db and idx < len(self.accounts) and "session_name" in self.accounts[idx]:
            try:
                cooldown_until = None
                if cooldown_hours:
                    cooldown_until = (datetime.now(TZ) + datetime.timedelta(hours=cooldown_hours)).isoformat()
                
                self.db.conn.execute(
                    """
                    UPDATE account_rotation SET
                    last_error = ?,
                    cooldown_until = ?,
                    flood_wait_count = flood_wait_count + 1
                    WHERE session_name = ?
                    """,
                    (
                        error or "Unknown error",
                        cooldown_until,
                        self.accounts[idx]["session_name"]
                    )
                )
                self.db.conn.commit()
            except Exception as e:
                logger.error(f"Failed to mark account as failed in database: {e}")
        
        logger.warning(f"Marked account {self.accounts[idx]['session_name']} as failed")
    
    def mark_account_success(self, idx: int = None):
        """Record a successful operation for an account"""
        if idx is None:
            idx = self.current_index
            
        if self.db and idx < len(self.accounts) and "session_name" in self.accounts[idx]:
            try:
                self.db.conn.execute(
                    """
                    UPDATE account_rotation SET
                    success_count = success_count + 1
                    WHERE session_name = ?
                    """,
                    (self.accounts[idx]["session_name"],)
                )
                self.db.conn.commit()
            except Exception as e:
                logger.error(f"Failed to mark account success in database: {e}")
    
    def reset_usage_counts(self):
        """Reset usage counts for all accounts"""
        for idx in self.usage_counts:
            if self.usage_counts[idx] != float('inf'):
                self.usage_counts[idx] = 0
                
        # Reset in database if configured
        if self.db:
            try:
                self.db.conn.execute(
                    """
                    UPDATE account_rotation
                    SET usage_count = 0
                    WHERE is_banned = 0
                    """
                )
                self.db.conn.commit()
                logger.info("Reset usage counts in database")
            except Exception as e:
                logger.error(f"Failed to reset usage counts in database: {e}")
    
    def get_account_stats(self) -> List[Dict[str, Any]]:
        """Get statistics for all accounts"""
        if not self.db:
            return [{"session": acc.get("session_name", f"Account_{i}"), 
                    "usage": self.usage_counts[i], 
                    "last_used": self.last_used[i].isoformat()} 
                    for i, acc in enumerate(self.accounts)]
        
        try:
            rows = self.db.conn.execute(
                """
                SELECT 
                    session_name, 
                    usage_count, 
                    last_used, 
                    last_error, 
                    cooldown_until,
                    is_banned,
                    flood_wait_count,
                    success_count
                FROM account_rotation
                ORDER BY usage_count ASC
                """
            ).fetchall()
            
            return [
                {
                    "session": row[0],
                    "usage": row[1],
                    "last_used": row[2],
                    "last_error": row[3],
                    "cooldown_until": row[4],
                    "is_banned": bool(row[5]),
                    "flood_wait_count": row[6],
                    "success_count": row[7]
                }
                for row in rows
            ]
        except Exception as e:
            logger.error(f"Failed to get account statistics: {e}")
            return []

# ── Group Management ─────────────────────────────────────────────────────────
class GroupManager:
    """Join and manage Telegram groups for archiving"""
    
    def __init__(self, config: Config = None, db_path: Optional[Path] = None):
        self.config = config or Config()
        self.clients = {}
        self.active_client = None
        self.current_account = None
        self.db_path = db_path
        
        # Create account rotator with database integration if path provided
        self.account_rotator = AccountRotator(
            self.config.active_accounts,
            rotation_mode=self.config.data.get("account_rotation_mode", "sequential"),
            db_path=self.db_path
        )
        
    async def init_clients(self) -> Dict[str, TelegramClient]:
        """Initialize Telegram clients from accounts in config"""
        for idx, account in enumerate(self.config.active_accounts):
            if all(k in account for k in ["api_id", "api_hash", "session_name"]):
                try:
                    client = TelegramClient(
                        account["session_name"],
                        account["api_id"],
                        account["api_hash"]
                    )
                    await client.connect()
                    
                    # Check if authorization is needed
                    if not await client.is_user_authorized():
                        logger.warning(f"Account {account['session_name']} needs authorization")
                        self.account_rotator.mark_account_failed(idx, error="Needs authorization")
                        continue
                        
                    self.clients[account["session_name"]] = {
                        "client": client,
                        "account": account,
                        "idx": idx
                    }
                    logger.info(f"Initialized client: {account['session_name']}")
                    
                    # Use first successful client as active
                    if not self.active_client:
                        self.active_client = client
                        self.current_account = account
                        
                except Exception as e:
                    logger.error(f"Failed to initialize client {account['session_name']}: {e}")
                    self.account_rotator.mark_account_failed(idx, error=str(e))
        
        return self.clients
    
    async def select_client(self, session_name: str = None) -> Optional[TelegramClient]:
        """
        Select a specific client by session name or rotate automatically
        
        Args:
            session_name: Specific session to select, or None to use account rotator
        """
        # If specific session requested
        if session_name and session_name in self.clients:
            self.active_client = self.clients[session_name]["client"]
            self.current_account = self.clients[session_name]["account"]
            logger.info(f"Selected client: {session_name}")
            return self.active_client
            
        # Otherwise rotate accounts
        if not self.clients:
            logger.error("No clients initialized")
            return None
            
        # Get next account from rotator
        next_account = self.account_rotator.get_next_account()
        session_name = next_account["session_name"]
        
        # Check if we have a client for this account
        if session_name in self.clients:
            self.active_client = self.clients[session_name]["client"]
            self.current_account = next_account
            logger.info(f"Rotated to client: {session_name}")
            return self.active_client
        else:
            # Fallback to any available client
            logger.warning(f"Client {session_name} not found, using fallback")
            first_key = next(iter(self.clients))
            self.active_client = self.clients[first_key]["client"]
            self.current_account = self.clients[first_key]["account"]
            return self.active_client
    
    async def join_group(self, target_link: str) -> Optional[int]:
        """Join a Telegram group/channel"""
        # Rotate clients if no active client or based on policy
        rotate_policy = self.config.data.get("account_rotation_policy", "per_operation")
        if not self.active_client or rotate_policy == "per_operation":
            await self.select_client()
            
        if not self.active_client:
            if not self.clients:
                await self.init_clients()
            await self.select_client()
            
        if not self.active_client:
            logger.error("No client available. Cannot join group.")
            return None
            
        try:
            # Parse link type
            if target_link.startswith('@'):
                # Username channel/group
                username = target_link[1:]
                try:
                    channel = await self.active_client(JoinChannelRequest(username))
                    entity_id = channel.chats[0].id
                    logger.info(f"Joined group @{username} (ID: {entity_id})")
                    
                    # Record success
                    curr_idx = self.clients[self.current_account["session_name"]]["idx"]
                    self.account_rotator.mark_account_success(curr_idx)
                    
                    return entity_id
                except errors.FloodWaitError as e:
                    logger.warning(f"FloodWait: Need to wait {e.seconds} seconds")
                    
                    # Mark current account as having issues
                    idx = self.clients[self.current_account["session_name"]]["idx"]
                    self.account_rotator.mark_account_failed(
                        idx, 
                        error=f"FloodWaitError: {e.seconds}s",
                        cooldown_hours=e.seconds / 3600
                    )
                    
                    # Try again with different account
                    await self.select_client()
                    if self.active_client:
                        return await self.join_group(target_link)
                    return None
                    
                except (errors.ChatAdminRequiredError, errors.ChatWriteForbiddenError) as e:
                    logger.warning(f"Permission error for @{username}: {e}")
                    # No need to mark account as failed - this is a target-specific issue
                    return None
                    
                except errors.ChannelsTooMuchError:
                    logger.warning(f"Account has joined too many channels")
                    idx = self.clients[self.current_account["session_name"]]["idx"]
                    self.account_rotator.mark_account_failed(
                        idx, 
                        error="ChannelsTooMuchError", 
                        cooldown_hours=24
                    )
                    
                    # Try again with different account
                    await self.select_client()
                    if self.active_client:
                        return await self.join_group(target_link)
                    return None
                    
            elif "t.me/joinchat/" in target_link or "t.me/+" in target_link:
                # Private invite link
                if "t.me/joinchat/" in target_link:
                    invite_hash = target_link.split("t.me/joinchat/")[1].split("?")[0]
                else:
                    invite_hash = target_link.split("t.me/+")[1].split("?")[0]
                    
                try:
                    # First check the invite
                    invite_info = await self.active_client(CheckChatInviteRequest(invite_hash))
                    
                    # Then join using the invite
                    chat = await self.active_client(ImportChatInviteRequest(invite_hash))
                    
                    try:
                        entity_id = chat.chats[0].id
                    except (IndexError, AttributeError):
                        # Fallback attempt to get ID
                        entity_id = getattr(chat, 'chat_id', getattr(chat, 'channel_id', None))
                        
                    if entity_id:
                        logger.info(f"Joined group via invite {invite_hash} (ID: {entity_id})")
                        
                        # Record success
                        curr_idx = self.clients[self.current_account["session_name"]]["idx"]
                        self.account_rotator.mark_account_success(curr_idx)
                        
                        return entity_id
                    else:
                        logger.error(f"Could not determine entity ID after join")
                        return None
                        
                except errors.FloodWaitError as e:
                    logger.warning(f"FloodWait: Need to wait {e.seconds} seconds")
                    
                    # Mark current account as having issues
                    idx = self.clients[self.current_account["session_name"]]["idx"]
                    self.account_rotator.mark_account_failed(
                        idx, 
                        error=f"FloodWaitError: {e.seconds}s", 
                        cooldown_hours=e.seconds / 3600
                    )
                    
                    # Try again with different account
                    await self.select_client()
                    if self.active_client:
                        return await self.join_group(target_link)
                    return None
                    
                except errors.InviteHashExpiredError:
                    logger.warning(f"Invite hash expired: {invite_hash}")
                    return None
                    
                except errors.ChannelsTooMuchError:
                    logger.warning(f"Account has joined too many channels")
                    idx = self.clients[self.current_account["session_name"]]["idx"]
                    self.account_rotator.mark_account_failed(
                        idx,
                        error="ChannelsTooMuchError",
                        cooldown_hours=24
                    )
                    
                    # Try again with different account
                    await self.select_client()
                    if self.active_client:
                        return await self.join_group(target_link)
                    return None
                    
            else:
                # Try as entity ID
                try:
                    entity_id = int(target_link)
                    entity = await self.active_client.get_entity(entity_id)
                    logger.info(f"Using existing entity ID: {entity_id}")
                    return entity_id
                except (ValueError, errors.RPCError) as e:
                    logger.error(f"Failed to parse entity ID {target_link}: {e}")
                    return None
                    
        except Exception as e:
            logger.error(f"Failed to join group {target_link}: {e}")
            
            # Mark current account as having unexpected issues
            if self.current_account and "session_name" in self.current_account:
                idx = self.clients[self.current_account["session_name"]]["idx"]
                self.account_rotator.mark_account_failed(idx, error=str(e))
                
            return None
    
    async def leave_group(self, entity_id: int) -> bool:
        """Leave a Telegram group/channel"""
        if not self.active_client:
            logger.error("No client available")
            return False
            
        try:
            await self.active_client.delete_dialog(entity_id)
            logger.info(f"Left group {entity_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to leave group {entity_id}: {e}")
            return False
    
    async def join_and_archive(self, target_link: str, archive_immediately=True) -> bool:
        """Join a group and archive it"""
        entity_id = await self.join_group(target_link)
        
        if not entity_id:
            return False
            
        if archive_immediately:
            try:
                # Configure archive task
                entity_to_archive = target_link if target_link.startswith('@') else entity_id
                self.config.data["entity"] = entity_to_archive
                
                # Run archiver with account rotation
                next_account = self.account_rotator.get_next_account()
                await runner(self.config, next_account)
                logger.info(f"Archived {entity_to_archive}")
                return True
            except Exception as e:
                logger.error(f"Failed to archive {target_link}: {e}")
                return False
        
        return True
    
    async def batch_join_archive(self, group_list: List[str], delay=60, leave_after=True) -> Dict[str, bool]:
        """Process a list of groups to join and archive"""
        results = {}
        
        for idx, group in enumerate(group_list):
            logger.info(f"Processing {idx+1}/{len(group_list)}: {group}")
            
            # Join and archive with account rotation
            success = await self.join_and_archive(group, archive_immediately=True)
            results[group] = success
            
            if success and leave_after:
                if group.startswith('@'):
                    entity = await self.active_client.get_entity(group)
                    await self.leave_group(entity.id)
                else:
                    try:
                        await self.leave_group(int(group))
                    except:
                        pass
            
            # Reset usage counts periodically
            if (idx + 1) % 5 == 0:
                self.account_rotator.reset_usage_counts()
                
            # Delay between operations to avoid rate limits
            if idx < len(group_list) - 1:  # Don't wait after last item
                logger.info(f"Waiting {delay} seconds before next group...")
                await asyncio.sleep(delay)
                
        logger.info(f"Batch processing complete. Success: {sum(results.values())}/{len(results)}")
        return results
    
    async def close(self):
        """Close all client connections"""
        for name, client_data in self.clients.items():
            await client_data["client"].disconnect()
            logger.info(f"Disconnected client: {name}")

# ── Parallel Processing ──────────────────────────────────────────────────────
class ParallelTaskScheduler:
    """Schedules and executes tasks in parallel across multiple Telegram accounts"""
    
    def __init__(self, config: Config = None, db_path: Optional[Path] = None, max_workers: int = None):
        """
        Initialize the parallel task scheduler
        
        Args:
            config: Configuration with account information
            db_path: Path to SQLite database for tracking tasks
            max_workers: Maximum number of parallel workers (defaults to number of accounts)
        """
        self.config = config or Config()
        self.db_path = db_path
        self.group_manager = GroupManager(self.config, db_path=self.db_path)
        self.max_workers = max_workers
        self.active_tasks = {}  # Map of task_id -> task info
        self.clients = {}  # session_name -> client
        self.initialized = False
        
    async def initialize(self) -> bool:
        """Initialize clients and prepare for parallel execution"""
        # Initialize clients via group manager
        self.clients = await self.group_manager.init_clients()
        
        if not self.clients:
            logger.error("No clients initialized - cannot run parallel tasks")
            return False
            
        # Set max_workers if not specified
        if self.max_workers is None:
            self.max_workers = len(self.clients)
            
        # Initialize task tracking database if needed
        if self.db_path:
            await self._setup_task_db()
        
        self.initialized = True
        logger.info(f"Parallel scheduler initialized with {len(self.clients)} clients, max_workers={self.max_workers}")
        return True
        
    async def _setup_task_db(self):
        """Set up database tables for task tracking"""
        from .db import SpectraDB
        
        try:
            # Open DB connection
            db = SpectraDB(self.db_path)
            
            # Create tasks table
            db.conn.execute("""
                CREATE TABLE IF NOT EXISTS parallel_tasks (
                    task_id TEXT PRIMARY KEY,
                    task_type TEXT,
                    target TEXT,
                    session_name TEXT,
                    started_at TEXT,
                    completed_at TEXT,
                    success BOOLEAN,
                    error TEXT,
                    result TEXT
                )
            """)
            
            db.conn.commit()
            logger.info("Task database initialized")
        except Exception as e:
            logger.error(f"Failed to initialize task database: {e}")
    
    async def _save_task(self, task_id: str, task_data: Dict[str, Any]):
        """Save task data to database"""
        if not self.db_path:
            return
            
        from .db import SpectraDB
        
        try:
            db = SpectraDB(self.db_path)
            
            # Convert dict values to strings for storage
            task_dict = {k: str(v) if not isinstance(v, (str, bool, int, float)) else v 
                        for k, v in task_data.items()}
            
            # Store JSON for complex result data
            if "result" in task_dict and not isinstance(task_dict["result"], str):
                task_dict["result"] = json.dumps(task_dict["result"])
            
            # Build dynamic INSERT OR REPLACE
            fields = list(task_dict.keys())
            placeholders = ["?"] * len(fields)
            values = [task_dict[f] for f in fields]
            
            fields.append("task_id")
            placeholders.append("?")
            values.append(task_id)
            
            query = f"""
                INSERT OR REPLACE INTO parallel_tasks 
                ({', '.join(fields)})
                VALUES ({', '.join(placeholders)})
            """
            
            db.conn.execute(query, values)
            db.conn.commit()
        except Exception as e:
            logger.error(f"Failed to save task {task_id}: {e}")
    
    async def execute_parallel(self, task_type: str, targets: List[Any], 
                              task_fn, max_concurrent: int = None) -> Dict[str, Any]:
        """
        Execute tasks in parallel across multiple accounts
        
        Args:
            task_type: Type of task being executed (e.g., "join", "archive")
            targets: List of targets to process
            task_fn: Async function to call for each target with (client, target) arguments
            max_concurrent: Maximum concurrent tasks (defaults to self.max_workers)
        
        Returns:
            Dictionary mapping targets to results
        """
        if not self.initialized:
            if not await self.initialize():
                logger.error("Failed to initialize for parallel execution")
                return {}
                
        if max_concurrent is None:
            max_concurrent = self.max_workers
        
        # Prepare tasks
        results = {}
        pending_targets = list(targets)
        running_tasks = {}  # task -> (target, session)
        available_sessions = list(self.clients.keys())
        
        # Process tasks with bounded concurrency
        while pending_targets or running_tasks:
            # Start new tasks if sessions available and targets pending
            while pending_targets and len(running_tasks) < max_concurrent and available_sessions:
                # Get next target and session
                target = pending_targets.pop(0)
                session = available_sessions.pop(0)
                
                # Get client for this session
                client = self.clients[session]["client"]
                
                # Generate unique task ID
                task_id = f"{task_type}_{target}_{int(time.time())}"
                
                # Save task start info
                task_data = {
                    "task_type": task_type,
                    "target": str(target),
                    "session_name": session,
                    "started_at": datetime.now(TZ).isoformat()
                }
                await self._save_task(task_id, task_data)
                
                # Start task
                logger.info(f"Starting {task_type} task for {target} using {session}")
                task = asyncio.create_task(task_fn(client, target))
                running_tasks[task] = (target, session, task_id)
            
            if not running_tasks:
                if pending_targets:
                    # No available sessions but still have targets
                    logger.warning(f"Waiting for available sessions ({len(pending_targets)} targets remaining)")
                    await asyncio.sleep(1)
                    continue
                else:
                    # No tasks running and no pending targets - we're done
                    break
            
            # Wait for a task to complete
            done, pending = await asyncio.wait(
                running_tasks.keys(), 
                timeout=30,
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Process completed tasks
            for task in done:
                target, session, task_id = running_tasks.pop(task)
                available_sessions.append(session)  # Session available again
                
                try:
                    result = task.result()
                    success = True
                    error = None
                except Exception as e:
                    result = None
                    success = False
                    error = str(e)
                    logger.error(f"Task {task_id} failed: {e}")
                
                # Record result
                results[target] = result
                
                # Update task in database
                task_data = {
                    "completed_at": datetime.now(TZ).isoformat(),
                    "success": success,
                    "error": error,
                    "result": result
                }
                await self._save_task(task_id, task_data)
                
                # Update account statistics based on outcome
                account_idx = self.clients[session]["idx"]
                if success:
                    self.group_manager.account_rotator.mark_account_success(account_idx)
                else:
                    self.group_manager.account_rotator.mark_account_failed(
                        account_idx,
                        error=error,
                        cooldown_hours=1 if "FloodWait" in str(error or "") else None
                    )
                
                logger.info(f"Completed {task_type} task for {target} using {session}: {success}")
        
        return results
    
    async def parallel_join(self, group_links: List[str], max_concurrent: int = None) -> Dict[str, Optional[int]]:
        """Join multiple groups in parallel using multiple accounts"""
        async def join_task(client, group_link):
            # Task function to join a group
            try:
                if group_link.startswith('@'):
                    # Username channel/group
                    username = group_link[1:]
                    channel = await client(JoinChannelRequest(username))
                    entity_id = channel.chats[0].id
                    return entity_id
                elif "t.me/joinchat/" in group_link or "t.me/+" in group_link:
                    # Private invite link
                    if "t.me/joinchat/" in group_link:
                        invite_hash = group_link.split("t.me/joinchat/")[1].split("?")[0]
                    else:
                        invite_hash = group_link.split("t.me/+")[1].split("?")[0]
                        
                    # First check the invite
                    await client(CheckChatInviteRequest(invite_hash))
                    
                    # Then join using the invite
                    chat = await client(ImportChatInviteRequest(invite_hash))
                    
                    try:
                        entity_id = chat.chats[0].id
                    except (IndexError, AttributeError):
                        # Fallback attempt to get ID
                        entity_id = getattr(chat, 'chat_id', getattr(chat, 'channel_id', None))
                    
                    return entity_id
                else:
                    # Try as entity ID
                    entity_id = int(group_link)
                    entity = await client.get_entity(entity_id)
                    return entity_id
            except Exception as e:
                logger.error(f"Join task failed for {group_link}: {e}")
                raise
        
        return await self.execute_parallel("join", group_links, join_task, max_concurrent)
    
    async def parallel_archive(self, entities: List[Union[str, int]], max_concurrent: int = None) -> Dict[str, bool]:
        """Archive multiple entities in parallel"""
        # This is a placeholder - archiving requires a more complex implementation with sync.py
        # We would need to modify runner() to work with a pre-connected client
        logger.warning("Parallel archive not fully implemented - using sequential archive")
        
        results = {}
        for entity in entities:
            # Use standard archive process but with auto-account selection
            try:
                self.config.data["entity"] = entity
                account = self.config.auto_select_account()
                await runner(self.config, account)
                results[str(entity)] = True
            except Exception as e:
                logger.error(f"Failed to archive {entity}: {e}")
                results[str(entity)] = False
                
        return results
    
    async def parallel_discovery(self, seed_entities: List[str], depth: int = 1, 
                               max_messages: int = 1000, max_concurrent: int = None) -> Dict[str, Set[str]]:
        """Discover groups from multiple seed entities in parallel"""
        async def discovery_task(client, seed_entity):
            discovery = GroupDiscovery(client, data_dir=self.db_path.parent if self.db_path else None)
            return await discovery.extract_from_entity(seed_entity, limit=max_messages)
            
        # First join all seed entities
        joined_entities = await self.parallel_join(seed_entities, max_concurrent)
        
        # Filter out failed joins
        valid_seeds = {seed: entity_id for seed, entity_id in joined_entities.items() if entity_id is not None}
        
        if not valid_seeds:
            logger.error("No seed entities could be joined")
            return {}
            
        # Execute discovery on joined entities
        results = await self.execute_parallel(
            "discovery", 
            list(valid_seeds.values()), 
            discovery_task,
            max_concurrent
        )
        
        # If depth > 1, we need to process recursively
        all_discovered = {}
        for seed, seed_id in valid_seeds.items():
            seed_id_str = str(seed_id)
            all_discovered[seed] = results.get(seed_id, set())
            
        if depth > 1:
            logger.info(f"Processing to depth {depth}...")
            
            for current_depth in range(1, depth):
                # Collect all newly discovered entities from the previous level
                new_seeds = []
                for discovered in all_discovered.values():
                    new_seeds.extend(list(discovered)[:5])  # Limit to avoid explosion
                
                if not new_seeds:
                    logger.info(f"No more entities to process at depth {current_depth}")
                    break
                    
                logger.info(f"Joining {len(new_seeds)} entities for depth {current_depth}")
                
                # Join and discover from new seeds
                next_joined = await self.parallel_join(new_seeds, max_concurrent)
                valid_next_seeds = {seed: entity_id for seed, entity_id in next_joined.items() if entity_id is not None}
                
                if not valid_next_seeds:
                    logger.warning(f"Could not join any new entities at depth {current_depth}")
                    break
                
                # Discover from these new entities
                next_results = await self.execute_parallel(
                    f"discovery_depth_{current_depth}", 
                    list(valid_next_seeds.values()), 
                    discovery_task,
                    max_concurrent
                )
                
                # Store results
                for seed, entity_id in valid_next_seeds.items():
                    entity_id_str = str(entity_id)
                    all_discovered[seed] = next_results.get(entity_id, set())
        
        return all_discovered
    
    async def close(self):
        """Close all clients and connections"""
        await self.group_manager.close()

# ── Integration with Config Generator ──────────────────────────────────────────
def import_accounts_from_gen_config() -> List[Dict[str, Any]]:
    """Import accounts from gen_config.py or generated config files"""
    accounts = []
    
    # Try looking in standard locations
    config_paths = [
        Path.home() / ".telegram_reporter" / "telegram_reporter_config.json",
        Path.cwd() / "config" / "telegram_reporter_config.json",
        Path.cwd().parent / "config" / "telegram_reporter_config.json",
        Path.cwd() / "telegram_reporter_config.json",
    ]
    
    for config_path in config_paths:
        if config_path.exists():
            try:
                config_data = json.loads(config_path.read_text())
                if "accounts" in config_data and isinstance(config_data["accounts"], list):
                    raw_accounts = config_data["accounts"]
                    
                    # Convert to SPECTRA account format
                    for acc in raw_accounts:
                        if all(k in acc for k in ["phone_number", "api_id", "api_hash"]):
                            phone = acc["phone_number"].replace("+", "")
                            accounts.append({
                                "api_id": acc["api_id"],
                                "api_hash": acc["api_hash"],
                                "session_name": f"parallel_{phone}",
                                "phone_number": acc["phone_number"],
                                "password": acc.get("password", "")
                            })
                    
                    logger.info(f"Loaded {len(accounts)} accounts from {config_path}")
                    if accounts:
                        return accounts
            except Exception as e:
                logger.warning(f"Failed to load accounts from {config_path}: {e}")
    
    # Try to import directly from gen_config
    try:
        # Find gen_config.py
        gen_config_paths = [
            Path.cwd() / "gen_config.py",
            Path.cwd().parent / "gen_config.py",
            Path(__file__).parent.parent / "gen_config.py"
        ]
        
        for path in gen_config_paths:
            if not path.exists():
                continue
                
            # Extract accounts from gen_config.py using regex
            content = path.read_text()
            import re
            
            # Look for the accounts raw pattern
            pattern = r'ACCOUNTS_RAW\s*=\s*\[(.*?)\]'
            matches = re.search(pattern, content, re.DOTALL)
            
            if matches:
                accounts_text = matches.group(1)
                
                # Extract individual account tuples
                account_pattern = r'\(\s*["\']([^"\']+)["\']\s*,\s*(\d+)\s*,\s*["\']([^"\']+)["\']\s*\)'
                account_matches = re.findall(account_pattern, accounts_text)
                
                for phone, api_id, api_hash in account_matches:
                    accounts.append({
                        "api_id": int(api_id),
                        "api_hash": api_hash,
                        "session_name": f"parallel_{phone.replace('+', '')}",
                        "phone_number": phone
                    })
                
                logger.info(f"Loaded {len(accounts)} accounts from gen_config.py")
                if accounts:
                    return accounts
                    
    except Exception as e:
        logger.warning(f"Failed to import accounts directly from gen_config.py: {e}")
    
    logger.warning("No accounts found in gen_config.py or generated config files")
    return []

def enhance_config_with_gen_accounts(config: Config) -> Config:
    """Add accounts from gen_config to the existing config"""
    gen_accounts = import_accounts_from_gen_config()
    
    if not gen_accounts:
        return config
    
    # If the config has no existing accounts, use the gen_config accounts
    if not config.accounts:
        config.data["accounts"] = gen_accounts
        logger.info(f"Added {len(gen_accounts)} accounts from gen_config to config")
        return config
    
    # Otherwise, add any new accounts not already in the config
    existing_api_ids = {acc.get("api_id") for acc in config.accounts if "api_id" in acc}
    
    added = 0
    for acc in gen_accounts:
        if acc.get("api_id") not in existing_api_ids:
            config.data["accounts"].append(acc)
            existing_api_ids.add(acc.get("api_id"))
            added += 1
    
    if added > 0:
        logger.info(f"Added {added} new accounts from gen_config to config")
        
    return config

# ── Integrated Manager ─────────────────────────────────────────────────────────
class SpectraCrawlerManager:
    """Integrated management of discovery, analysis and archiving operations"""
    
    def __init__(self, config: Config = None, data_dir: Path = None, db_path: Optional[Path] = None):
        self.config = config or Config()
        self.data_dir = data_dir or Path("spectra_data")
        self.data_dir.mkdir(exist_ok=True)
        self.db_path = db_path
        
        # Initialize components
        self.group_manager = GroupManager(self.config, db_path=self.db_path)
        self.network_analyzer = NetworkAnalyzer(data_dir=self.data_dir)
        self.discovery = None
        self.initialized = False
        
    async def initialize(self) -> bool:
        """Initialize clients and crawler components"""
        try:
            # Initialize clients for Telegram API access
            clients = await self.group_manager.init_clients()
            if not clients:
                logger.error("No clients available - check account credentials")
                return False
                
            # Pick one client for the discovery process
            active_client = self.group_manager.active_client
            
            # Initialize discovery with active client
            self.discovery = GroupDiscovery(active_client, data_dir=self.data_dir)
            
            # Setup database for storing discovered groups if path specified
            if self.db_path:
                await self._setup_discovery_db()
            
            self.initialized = True
            logger.info("Crawler manager initialized")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize crawler manager: {e}")
            return False
    
    async def _setup_discovery_db(self):
        """Set up database tables for storing discovered groups"""
        from .db import SpectraDB
        
        try:
            # Open DB connection
            db = SpectraDB(self.db_path)
            
            # Create discovered_groups table
            db.conn.execute("""
                CREATE TABLE IF NOT EXISTS discovered_groups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_link TEXT UNIQUE,
                    group_type TEXT,
                    date_discovered TEXT,
                    source TEXT,
                    priority REAL DEFAULT 0.0,
                    status TEXT DEFAULT 'new',
                    last_checked TEXT,
                    member_count INTEGER DEFAULT 0,
                    title TEXT,
                    description TEXT
                )
            """)
            
            # Create discovery_sources table
            db.conn.execute("""
                CREATE TABLE IF NOT EXISTS discovery_sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_entity TEXT,
                    date_crawled TEXT,
                    groups_found INTEGER,
                    depth INTEGER
                )
            """)
            
            # Create group_relationships table for network analysis
            db.conn.execute("""
                CREATE TABLE IF NOT EXISTS group_relationships (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_group TEXT,
                    target_group TEXT,
                    relationship_type TEXT DEFAULT 'mention',
                    weight REAL DEFAULT 1.0,
                    UNIQUE(source_group, target_group, relationship_type)
                )
            """)
            
            db.conn.commit()
            logger.info(f"Discovery database tables initialized at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize discovery database: {e}")
    
    async def _save_discovered_groups(self, groups: Set[str], source: str):
        """Save discovered groups to database"""
        if not self.db_path:
            return
            
        from .db import SpectraDB
        
        try:
            db = SpectraDB(self.db_path)
            
            for group in groups:
                # Determine group type
                if group.startswith('@'):
                    group_type = 'username'
                elif 'joinchat' in group or '/+' in group:
                    group_type = 'private'
                else:
                    group_type = 'unknown'
                
                # Insert group if not exists
                db.conn.execute("""
                    INSERT OR IGNORE INTO discovered_groups 
                    (group_link, group_type, date_discovered, source)
                    VALUES (?, ?, ?, ?)
                """, (
                    group,
                    group_type,
                    datetime.now(TZ).isoformat(),
                    source
                ))
            
            db.conn.commit()
            logger.info(f"Saved {len(groups)} discovered groups to database")
        except Exception as e:
            logger.error(f"Failed to save discovered groups to database: {e}")
            
    async def _save_discovery_source(self, source_entity: str, groups_found: int, depth: int):
        """Record a discovery operation in the database"""
        if not self.db_path:
            return
            
        from .db import SpectraDB
        
        try:
            db = SpectraDB(self.db_path)
            
            db.conn.execute("""
                INSERT INTO discovery_sources
                (source_entity, date_crawled, groups_found, depth)
                VALUES (?, ?, ?, ?)
            """, (
                source_entity,
                datetime.now(TZ).isoformat(),
                groups_found,
                depth
            ))
            
            db.conn.commit()
            logger.info(f"Recorded discovery operation from {source_entity} (depth: {depth}, found: {groups_found})")
        except Exception as e:
            logger.error(f"Failed to record discovery source: {e}")
            
    async def _save_group_relationships(self, source_entity: str, target_groups: Set[str]):
        """Save network relationships between groups"""
        if not self.db_path:
            return
            
        from .db import SpectraDB
        
        try:
            db = SpectraDB(self.db_path)
            
            for target in target_groups:
                db.conn.execute("""
                    INSERT OR IGNORE INTO group_relationships
                    (source_group, target_group, relationship_type)
                    VALUES (?, ?, 'mention')
                """, (source_entity, target))
            
            db.conn.commit()
            logger.info(f"Saved {len(target_groups)} group relationships for {source_entity}")
        except Exception as e:
            logger.error(f"Failed to save group relationships: {e}")
    
    async def discover_from_seed(self, seed_entity: str, depth=1, max_messages=1000) -> Set[str]:
        """Recursively discover groups from a seed entity"""
        if not self.initialized:
            if not await self.initialize():
                logger.error("Failed to initialize before discovery")
                return set()
                
        if not self.discovery:
            logger.error("Discovery component not initialized")
            return set()
            
        all_discovered = set()
        current_seeds = {seed_entity}
        visited = set()
        
        # Join the seed group if needed
        try:
            entity_id = await self.group_manager.join_group(seed_entity)
            if entity_id:
                # Use entity ID format if available
                seed_entity = str(entity_id)
                current_seeds = {seed_entity}
                logger.info(f"Joined seed entity: {seed_entity}")
            else:
                logger.warning(f"Could not join seed entity: {seed_entity}")
                
        except Exception as e:
            logger.error(f"Error joining seed entity: {e}")
        
        for current_depth in range(depth + 1):
            next_seeds = set()
            
            for entity in current_seeds:
                if entity in visited:
                    continue
                    
                visited.add(entity)
                
                # Extract links from this entity
                try:
                    logger.info(f"Extracting from entity {entity} (depth {current_depth}/{depth})")
                    new_links = await self.discovery.extract_from_entity(entity, limit=max_messages)
                    
                    # Save to database
                    await self._save_discovered_groups(new_links, f"discovery_depth_{current_depth}")
                    await self._save_group_relationships(entity, new_links)
                    
                    all_discovered.update(new_links)
                    
                    # Add to next depth if we're not at max depth
                    if current_depth < depth:
                        next_seeds.update(new_links)
                        
                    # Record this discovery operation
                    await self._save_discovery_source(entity, len(new_links), current_depth)
                    
                except Exception as e:
                    logger.error(f"Failed to extract from {entity}: {e}")
            
            # Update current seeds for next iteration
            current_seeds = next_seeds
            
            if not current_seeds:
                logger.info(f"No more links to process at depth {current_depth}")
                break
                
            logger.info(f"Moving to depth {current_depth + 1} with {len(current_seeds)} entities")
        
        # Save all to discovery cache
        self.discovery.save_discovered_groups()
        
        # Update database with final counts
        await self._update_group_priorities()
        
        logger.info(f"Discovery completed - found {len(all_discovered)} groups")
        return all_discovered
    
    async def _update_group_priorities(self):
        """Update priority scores for groups based on network analysis"""
        if not self.db_path:
            return
            
        from .db import SpectraDB
        import networkx as nx
        
        try:
            db = SpectraDB(self.db_path)
            
            # Build network from database
            G = nx.DiGraph()
            
            # Load nodes
            for row in db.conn.execute("SELECT group_link FROM discovered_groups").fetchall():
                G.add_node(row[0])
                
            # Load edges
            for row in db.conn.execute("SELECT source_group, target_group, weight FROM group_relationships").fetchall():
                G.add_edge(row[0], row[1], weight=row[2])
                
            if not G.nodes():
                logger.warning("No nodes in graph for priority calculation")
                return
                
            # Calculate metrics
            pagerank = nx.pagerank(G, alpha=0.85)
            in_degree = nx.in_degree_centrality(G)
            
            # Update database with new scores
            for node, score in pagerank.items():
                # Combined score is 70% pagerank, 30% in-degree
                combined = score * 0.7 + in_degree.get(node, 0) * 0.3
                
                db.conn.execute(
                    "UPDATE discovered_groups SET priority = ? WHERE group_link = ?",
                    (combined, node)
                )
                
            db.conn.commit()
            logger.info("Updated group priorities based on network analysis")
        except Exception as e:
            logger.error(f"Failed to update group priorities: {e}")
    
    async def get_priority_targets(self, top_n=20, min_priority=0.0) -> List[Dict[str, Any]]:
        """Get highest priority groups for archiving"""
        if not self.db_path:
            # Fall back to network analyzer if no database
            self.network_analyzer.calculate_metrics()
            return self.network_analyzer.export_priority_targets(top_n)
            
        from .db import SpectraDB
        
        try:
            db = SpectraDB(self.db_path)
            
            rows = db.conn.execute("""
                SELECT 
                    group_link, 
                    group_type, 
                    priority, 
                    status, 
                    date_discovered,
                    title
                FROM discovered_groups
                WHERE priority >= ? AND status != 'archived'
                ORDER BY priority DESC
                LIMIT ?
            """, (min_priority, top_n)).fetchall()
            
            results = [
                {
                    "id": row[0],
                    "type": row[1],
                    "priority": row[2],
                    "status": row[3],
                    "discovered": row[4],
                    "title": row[5] or row[0]
                }
                for row in rows
            ]
            
            logger.info(f"Retrieved {len(results)} priority targets from database")
            return results
        except Exception as e:
            logger.error(f"Failed to get priority targets: {e}")
            return []
    
    async def load_and_analyze_network(self, crawler_dir=None) -> Optional[List[Dict[str, Any]]]:
        """Load and analyze network from telegram-groups-crawler data"""
        if not self.initialized:
            if not await self.initialize():
                logger.error("Failed to initialize before network analysis")
                return None
        
        # Load network from crawler data
        success = self.network_analyzer.load_crawler_graph(crawler_dir)
        if not success:
            logger.error("Failed to load crawler graph")
            return None
            
        # Calculate network metrics
        self.network_analyzer.calculate_metrics()
        
        # Get priority targets
        targets = self.network_analyzer.export_priority_targets()
        
        # If we have a database, save these to it
        if self.db_path and targets:
            from .db import SpectraDB
            
            try:
                db = SpectraDB(self.db_path)
                
                for target in targets:
                    group_link = target["id"]
                    
                    # Update or insert
                    db.conn.execute("""
                        INSERT INTO discovered_groups
                        (group_link, group_type, priority, source, title, date_discovered)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(group_link) DO UPDATE SET
                        priority = excluded.priority,
                        title = COALESCE(excluded.title, title)
                    """, (
                        group_link,
                        "username" if group_link.startswith("@") else "unknown",
                        target["score"],
                        "network_analysis",
                        target.get("name", ""),
                        datetime.now(TZ).isoformat()
                    ))
                
                db.conn.commit()
                logger.info(f"Saved {len(targets)} priority targets to database")
            except Exception as e:
                logger.error(f"Failed to save targets to database: {e}")
        
        return targets
    
    async def archive_priority_targets(self, top_n=10, delay=60) -> Dict[str, bool]:
        """Archive the highest priority targets from the database or network analysis"""
        if not self.initialized:
            if not await self.initialize():
                logger.error("Failed to initialize before archiving")
                return {}
                
        # Get priority targets
        targets = await self.get_priority_targets(top_n)
        if not targets:
            logger.warning("No priority targets found for archiving")
            return {}
            
        # Extract links
        links = [t["id"] for t in targets]
        logger.info(f"Archiving {len(links)} priority targets")
        
        # Batch join and archive
        results = await self.group_manager.batch_join_archive(links, delay=delay)
        
        # Update status in database for archived groups
        if self.db_path:
            from .db import SpectraDB
            
            try:
                db = SpectraDB(self.db_path)
                
                for link, success in results.items():
                    if success:
                        db.conn.execute(
                            "UPDATE discovered_groups SET status = 'archived' WHERE group_link = ?",
                            (link,)
                        )
                
                db.conn.commit()
                logger.info(f"Updated database status for {sum(results.values())} archived groups")
            except Exception as e:
                logger.error(f"Failed to update archive status in database: {e}")
        
        return results
    
    async def close(self):
        """Close all clients and connections"""
        if self.group_manager:
            await self.group_manager.close()
            
        logger.info("Crawler manager closed") 