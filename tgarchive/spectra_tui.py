"""
SPECTRA-003 TUI — Advanced Telegram Archiving & Discovery Interface
==================================================================
*Integrated TUI* · *Group Discovery* · *Network Analysis* · *Batch Operations*

Comprehensive npyscreen-based Terminal UI for the SPECTRA system, integrating
both archiving and discovery capabilities.
"""
from __future__ import annotations

# ── Standard Library ──────────────────────────────────────────────────────
import asyncio
import curses
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union, cast

# ── Third-party ───────────────────────────────────────────────────────────
import npyscreen
from rich.console import Console

# ── Local Imports ──────────────────────────────────────────────────────────
from . import discovery
from .sync import Config, logger

# ── Global Config ──────────────────────────────────────────────────────────
TZ = timezone.utc
console = Console()
TITLE = """
╔═══════════════════════════════════════════════════════════════════════════╗
║                          ███████╗██████╗ ███████╗ ██████╗████████╗██████╗  ║
║                          ██╔════╝██╔══██╗██╔════╝██╔════╝╚══██╔══╝██╔══██╗ ║
║                          ███████╗██████╔╝█████╗  ██║        ██║   ██████╔╗ ║
║                          ╚════██║██╔═══╝ ██╔══╝  ██║        ██║   ██╔══██║ ║
║                          ███████║██║     ███████╗╚██████╗   ██║   ██║  ██║ ║
║                          ╚══════╝╚═╝     ╚══════╝ ╚═════╝   ╚═╝   ╚═╝  ╚═╝ ║
╚═══════════════════════════════════════════════════════════════════════════╝
               Telegram Network Discovery & Archiving System v3.0
"""


# ── Async Helper ───────────────────────────────────────────────────────────
class AsyncRunner:
    """Helper class to run async functions from npyscreen"""
    
    @staticmethod
    def run_async(coroutine):
        """Run an async function and wait for the result"""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(coroutine)

    @staticmethod
    def run_in_thread(coroutine, callback=None):
        """Run an async function in a separate thread"""
        def _worker():
            result = AsyncRunner.run_async(coroutine)
            if callback:
                callback(result)
        
        thread = threading.Thread(target=_worker)
        thread.daemon = True
        thread.start()
        return thread


# ── Status Messages Widget ───────────────────────────────────────────────────
class StatusMessages(npyscreen.BoxTitle):
    """Widget for displaying status messages in a scrollable box"""
    
    _contained_widget = npyscreen.MultiLine
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.messages = []
        self.entry_order = []
        self.max_messages = 1000
        
    def add_message(self, message, level="INFO"):
        """Add a new message to the log with timestamp"""
        timestamp = datetime.now(TZ).strftime("%H:%M:%S")
        entry = f"[{timestamp}] {level}: {message}"
        
        # Add to the beginning for reverse chronological order
        self.messages.insert(0, entry)
        self.entry_order.insert(0, entry)
        
        # Limit the number of messages
        if len(self.messages) > self.max_messages:
            self.messages.pop()
            self.entry_order.pop()
            
        self.update_display()
        
    def update_display(self):
        """Update the display with current messages"""
        self.values = self.messages
        self.display()


# ── Graph Explorer Form ──────────────────────────────────────────────────────
class GraphExplorerForm(npyscreen.Form):
    """Form for visualizing and exploring the network graph"""
    
    def create(self):
        self.name = "SPECTRA Network Explorer"
        self.manager = self.parentApp.manager
        
        # Instructions
        self.add(npyscreen.FixedText, value=TITLE)
        self.add(npyscreen.FixedText, value="Network Graph Explorer - Analyze Telegram Group Relationships")
        self.add(npyscreen.FixedText, value="")
        
        # Network loading options
        self.add(npyscreen.TitleText, name="Crawler Directory:", value=str(Path.cwd() / "telegram-groups-crawler"))
        
        # Buttons for actions
        self.add(npyscreen.ButtonPress, name="Load Network Data", when_pressed_function=self.load_network)
        self.add(npyscreen.ButtonPress, name="Generate Network Visualization", when_pressed_function=self.visualize_network)
        self.add(npyscreen.FixedText, value="")
        
        # Metrics selection for targeting
        self.add(npyscreen.TitleSelectOne, name="Importance Metric:",
                max_height=5, value=[4],
                values=["Degree Centrality", "In-Degree Centrality", "Betweenness Centrality", 
                       "PageRank", "Combined Score"])
        
        self.top_n = self.add(npyscreen.TitleSlider, name="Top Groups:", 
                            out_of=50, step=5, value=10)
        
        # Results area
        self.add(npyscreen.FixedText, value="")
        self.add(npyscreen.FixedText, value="Priority Targets (Select number of groups above):")
        self.targets_box = self.add(npyscreen.Pager, name="Priority Targets", height=10)
        
        # Status and navigation
        self.status = self.add(StatusMessages, name="Status Messages", max_height=8)
        self.add(npyscreen.ButtonPress, name="Archive Selected Targets", when_pressed_function=self.archive_targets)
        self.add(npyscreen.ButtonPress, name="Export Target List", when_pressed_function=self.export_targets)
        self.add(npyscreen.ButtonPress, name="Back to Main Menu", when_pressed_function=self.back_to_main)
        
        # Initialize
        self.targets = []
        self.status.add_message("Graph Explorer ready. Load network data to begin.")
        
    def load_network(self):
        """Load network data from crawler"""
        self.status.add_message("Loading network data...")
        
        crawler_dir = Path(self.get_widget(1).value)
        
        def load_callback(targets):
            if targets:
                self.targets = targets
                self.update_targets_display()
                self.status.add_message(f"Loaded network with {len(self.manager.network.graph.nodes())} nodes and " 
                                      f"{len(self.manager.network.graph.edges())} edges")
            else:
                self.status.add_message("Failed to load network data", "ERROR")
        
        AsyncRunner.run_in_thread(
            self.manager.load_and_analyze_network(crawler_dir),
            callback=load_callback
        )
    
    def update_targets_display(self):
        """Update the display of priority targets"""
        if not self.targets:
            self.targets_box.values = ["No targets available. Load network data first."]
            return
            
        # Limit to top_n
        top_n = self.top_n.value
        display_targets = self.targets[:top_n]
        
        # Format for display
        lines = []
        for i, target in enumerate(display_targets):
            lines.append(f"{i+1}. {target['id']} (Score: {target['score']:.4f})")
            
        self.targets_box.values = lines
        self.targets_box.display()
    
    def visualize_network(self):
        """Generate network visualization"""
        if not self.manager.network.graph.nodes():
            self.status.add_message("No network data loaded", "ERROR")
            return
            
        metrics = ["degree", "in_degree", "betweenness", "pagerank", "combined"]
        selected_idx = self.get_widget(6).value[0]
        selected_metric = metrics[selected_idx]
            
        self.status.add_message(f"Generating network visualization using {selected_metric} metric...")
        
        def viz_callback(output_file):
            if output_file:
                self.status.add_message(f"Network visualization saved to {output_file}")
                
                # Show file path in a popup
                npyscreen.notify_confirm(
                    f"Network graph visualization saved to:\n\n{output_file}\n\nUse a graphical file viewer to open.",
                    title="Visualization Complete"
                )
            else:
                self.status.add_message("Failed to generate visualization", "ERROR")
        
        AsyncRunner.run_in_thread(
            self.manager.network.plot_network(metric=selected_metric),
            callback=viz_callback
        )
    
    def archive_targets(self):
        """Archive the priority targets"""
        if not self.targets:
            self.status.add_message("No targets available to archive", "ERROR")
            return
            
        top_n = self.top_n.value
        
        if npyscreen.notify_yes_no(
            f"Archive top {top_n} priority targets?\n\nThis will join groups and archive content.",
            title="Confirm Batch Archive"
        ):
            self.status.add_message(f"Starting batch archive of {top_n} targets...")
            
            def archive_callback(results):
                success_count = sum(1 for success in results.values() if success)
                self.status.add_message(f"Batch archive complete: {success_count}/{len(results)} successful")
            
            AsyncRunner.run_in_thread(
                self.manager.archive_priority_targets(top_n=top_n),
                callback=archive_callback
            )
    
    def export_targets(self):
        """Export the priority targets to a file"""
        if not self.targets:
            self.status.add_message("No targets available to export", "ERROR")
            return
            
        top_n = self.top_n.value
        
        # Filename with timestamp
        timestamp = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
        filename = f"priority_targets_{timestamp}.json"
        file_path = self.manager.data_dir / filename
        
        # Export
        try:
            with open(file_path, 'w') as f:
                json.dump(self.targets[:top_n], f, indent=2)
            
            self.status.add_message(f"Exported {top_n} targets to {file_path}")
            
            # Show file path in a popup
            npyscreen.notify_confirm(
                f"Target list exported to:\n\n{file_path}",
                title="Export Complete"
            )
        except Exception as e:
            self.status.add_message(f"Failed to export targets: {e}", "ERROR")
    
    def back_to_main(self):
        """Return to the main menu"""
        self.parentApp.switchForm("MAIN")


# ── Discovery Form ──────────────────────────────────────────────────────────
class DiscoveryForm(npyscreen.Form):
    """Form for discovering Telegram groups from seeds"""
    
    def create(self):
        self.name = "SPECTRA Group Discovery"
        self.manager = self.parentApp.manager
        
        # Instructions
        self.add(npyscreen.FixedText, value=TITLE)
        self.add(npyscreen.FixedText, value="Group Discovery - Find Telegram Groups from Seeds")
        self.add(npyscreen.FixedText, value="")
        
        # Input fields
        self.seed_input = self.add(npyscreen.TitleText, name="Seed Entity (e.g. @channel):")
        self.depth = self.add(npyscreen.TitleSlider, name="Discovery Depth:", 
                            out_of=3, value=1,
                            comment="Higher depth = exponential search")
        self.msg_limit = self.add(npyscreen.TitleSlider, name="Messages to Scan:", 
                               out_of=2000, step=100, value=500)
        
        # Options for loading existing data
        self.add(npyscreen.FixedText, value="")
        self.add(npyscreen.FixedText, value="Additional Data Sources:")
        self.crawler_path = self.add(npyscreen.TitleText, name="Crawler Directory:", 
                                  value=str(Path.cwd() / "telegram-groups-crawler"))
        
        # Action buttons
        self.add(npyscreen.ButtonPress, name="Start Discovery from Seed", when_pressed_function=self.start_discovery)
        self.add(npyscreen.ButtonPress, name="Load Data from Crawler", when_pressed_function=self.load_crawler_data)
        self.add(npyscreen.ButtonPress, name="Export Discovered Groups", when_pressed_function=self.export_groups)
        
        # Results area
        self.add(npyscreen.FixedText, value="")
        self.add(npyscreen.TitleText, name="Discovered Groups Count:", value="0", editable=False)
        self.groups_box = self.add(npyscreen.Pager, name="Discovered Groups (Sample)", height=8)
        
        # Status and navigation
        self.status = self.add(StatusMessages, name="Status Messages", max_height=8)
        self.add(npyscreen.ButtonPress, name="Archive Selected Groups", when_pressed_function=self.archive_groups)
        self.add(npyscreen.ButtonPress, name="Back to Main Menu", when_pressed_function=self.back_to_main)
        
        # Initialize
        self.status.add_message("Group Discovery ready. Enter a seed entity to begin.")
    
    def update_groups_display(self):
        """Update the display of discovered groups"""
        if not self.manager.discovery or not self.manager.discovery.discovered_groups:
            self.groups_box.values = ["No groups discovered yet."]
            return
            
        # Update count
        count_widget = self.get_widget(13)
        count_widget.value = str(len(self.manager.discovery.discovered_groups))
        count_widget.display()
        
        # Show sample in box (limit to avoid excessive display)
        sample = list(self.manager.discovery.discovered_groups)[:50]
        self.groups_box.values = sample
        self.groups_box.display()
    
    def start_discovery(self):
        """Start group discovery from seed entity"""
        seed = self.seed_input.value.strip()
        
        if not seed:
            self.status.add_message("Please enter a seed entity", "ERROR")
            return
            
        # Configure discovery
        depth = self.depth.value
        msg_limit = self.msg_limit.value
        
        self.status.add_message(f"Starting discovery from {seed} with depth {depth}...")
        
        def discovery_callback(discovered):
            self.status.add_message(f"Discovery complete. Found {len(discovered)} groups.")
            self.update_groups_display()
        
        AsyncRunner.run_in_thread(
            self.manager.discover_from_seed(seed, depth=depth, max_messages=msg_limit),
            callback=discovery_callback
        )
    
    def load_crawler_data(self):
        """Load group data from crawler"""
        crawler_path = Path(self.crawler_path.value)
        
        if not crawler_path.exists():
            self.status.add_message(f"Directory not found: {crawler_path}", "ERROR")
            return
            
        self.status.add_message(f"Loading groups from crawler data in {crawler_path}...")
        
        def load_callback(groups):
            self.status.add_message(f"Loaded {len(groups)} groups from crawler data.")
            self.update_groups_display()
        
        # Make sure discovery is initialized
        if not self.manager.discovery:
            AsyncRunner.run_async(self.manager.initialize())
            
        AsyncRunner.run_in_thread(
            self.manager.discovery.load_crawler_data(crawler_path),
            callback=load_callback
        )
    
    def export_groups(self):
        """Export discovered groups to a file"""
        if not self.manager.discovery or not self.manager.discovery.discovered_groups:
            self.status.add_message("No groups to export", "ERROR")
            return
            
        # Filename with timestamp
        timestamp = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
        filename = f"discovered_groups_{timestamp}.txt"
        
        output_path = self.manager.discovery.export_groups_to_file(str(self.manager.data_dir / filename))
        
        if output_path:
            self.status.add_message(f"Exported {len(self.manager.discovery.discovered_groups)} groups to {output_path}")
            
            # Show file path in a popup
            npyscreen.notify_confirm(
                f"Group list exported to:\n\n{output_path}",
                title="Export Complete"
            )
        else:
            self.status.add_message("Failed to export groups", "ERROR")
    
    def archive_groups(self):
        """Archive discovered groups"""
        if not self.manager.discovery or not self.manager.discovery.discovered_groups:
            self.status.add_message("No groups to archive", "ERROR")
            return
            
        # Ask how many to archive
        groups_count = len(self.manager.discovery.discovered_groups)
        
        F = npyscreen.Form(name="Archive Options")
        F.add(npyscreen.TitleText, name="Total Groups:", value=str(groups_count), editable=False)
        F.add(npyscreen.TitleSlider, name="Number to Archive:", out_of=min(groups_count, 50), value=5)
        F.add(npyscreen.TitleSlider, name="Delay Between Groups (seconds):", out_of=120, value=60, step=10)
        F.edit()
        
        to_archive = F.get_widget(1).value
        delay = F.get_widget(2).value
        
        if to_archive > 0:
            if npyscreen.notify_yes_no(
                f"Archive {to_archive} groups with {delay}s delay between each?\n\nThis will join groups and archive content.",
                title="Confirm Batch Archive"
            ):
                # Get list of groups to archive
                groups_list = list(self.manager.discovery.discovered_groups)[:to_archive]
                
                self.status.add_message(f"Starting batch archive of {to_archive} groups...")
                
                def archive_callback(results):
                    success_count = sum(1 for success in results.values() if success)
                    self.status.add_message(f"Batch archive complete: {success_count}/{len(results)} successful")
                
                AsyncRunner.run_in_thread(
                    self.manager.group_manager.batch_join_archive(groups_list, delay=delay),
                    callback=archive_callback
                )
    
    def back_to_main(self):
        """Return to the main menu"""
        self.parentApp.switchForm("MAIN")


# ── Archive Form ───────────────────────────────────────────────────────────
class ArchiveForm(npyscreen.Form):
    """Form for standard archiving operations"""
    
    def create(self):
        self.name = "SPECTRA Archiver"
        self.manager = self.parentApp.manager
        self.config = self.manager.config
        
        # Instructions
        self.add(npyscreen.FixedText, value=TITLE)
        self.add(npyscreen.FixedText, value="Channel/Group Archiver - Archive Telegram Content")
        self.add(npyscreen.FixedText, value="")
        
        # Account selection
        self.add(npyscreen.TitleSelectOne, name="Account:", max_height=6,
               values=self.get_account_names(), scroll_exit=True)
        
        # Channel input
        self.entity = self.add(npyscreen.TitleText, name="Channel/Group:", value=self.config["entity"])
        
        # Archive options
        self.add(npyscreen.FixedText, value="")
        self.add(npyscreen.FixedText, value="Archive Options:")
        self.dl_media = self.add(npyscreen.Checkbox, name="Download Media", value=self.config["download_media"])
        self.dl_avatars = self.add(npyscreen.Checkbox, name="Download Avatars", value=self.config["download_avatars"])
        self.sidecar = self.add(npyscreen.Checkbox, name="Write Sidecar Metadata", value=self.config["sidecar_metadata"])
        self.archive_topics = self.add(npyscreen.Checkbox, name="Archive Topics/Threads", value=self.config["archive_topics"])
        
        # Proxy options
        self.add(npyscreen.FixedText, value="")
        self.add(npyscreen.FixedText, value="Proxy Configuration:")
        self.use_proxy = self.add(npyscreen.Checkbox, name="Use Rotating Proxy", 
                               value=bool(self.config.proxy_conf.get("host")))
        
        # File paths
        self.add(npyscreen.FixedText, value="")
        self.add(npyscreen.TitleText, name="Media Directory:", value=self.config["media_dir"])
        self.add(npyscreen.TitleText, name="Database File:", value=self.config["db_path"])
        
        # Actions
        self.add(npyscreen.ButtonPress, name="Start Archive", when_pressed_function=self.start_archive)
        self.add(npyscreen.ButtonPress, name="Join Entity", when_pressed_function=self.join_entity)
        self.add(npyscreen.ButtonPress, name="Save Configuration", when_pressed_function=self.save_config)
        
        # Status and navigation
        self.status = self.add(StatusMessages, name="Status Messages", max_height=8)
        self.add(npyscreen.ButtonPress, name="Back to Main Menu", when_pressed_function=self.back_to_main)
        
        # Initialize
        self.status.add_message("Archive tool ready. Configure settings and start archiving.")
    
    def get_account_names(self):
        """Get list of account names for selection"""
        accounts = self.config.active_accounts
        if not accounts:
            return ["No accounts available"]
        return [acc.get("session_name", f"Account {i}") for i, acc in enumerate(accounts)]
    
    def save_config(self):
        """Save current configuration"""
        # Update config with form values
        self.config.data["entity"] = self.entity.value
        self.config.data["download_media"] = self.dl_media.value
        self.config.data["download_avatars"] = self.dl_avatars.value
        self.config.data["sidecar_metadata"] = self.sidecar.value
        self.config.data["archive_topics"] = self.archive_topics.value
        self.config.data["media_dir"] = self.get_widget(16).value
        self.config.data["db_path"] = self.get_widget(17).value
        
        # Save to file
        self.config.save()
        self.status.add_message("Configuration saved.")
    
    def start_archive(self):
        """Start the archiving process"""
        # Get selected account
        account_idx = self.get_widget(3).value[0] if self.get_widget(3).value else 0
        if account_idx >= len(self.config.active_accounts):
            self.status.add_message("No valid account selected", "ERROR")
            return
            
        selected_account = self.config.active_accounts[account_idx]
        
        # Update entity
        entity = self.entity.value.strip()
        if not entity:
            self.status.add_message("Please enter a channel/group to archive", "ERROR")
            return
            
        self.config.data["entity"] = entity
        
        # Update other options
        self.save_config()
        
        # Confirm
        if npyscreen.notify_yes_no(
            f"Start archiving {entity}?\n\nThis may take a while depending on the size of the channel/group.",
            title="Confirm Archive"
        ):
            self.status.add_message(f"Starting archive of {entity}...")
            
            def archive_callback(_):
                self.status.add_message(f"Archive of {entity} complete.")
            
            AsyncRunner.run_in_thread(
                self.manager.config.runner(self.config, selected_account),
                callback=archive_callback
            )
    
    def join_entity(self):
        """Join the specified entity"""
        entity = self.entity.value.strip()
        if not entity:
            self.status.add_message("Please enter a channel/group to join", "ERROR")
            return
            
        self.status.add_message(f"Joining {entity}...")
        
        def join_callback(entity_id):
            if entity_id:
                self.status.add_message(f"Successfully joined {entity} (ID: {entity_id})")
            else:
                self.status.add_message(f"Failed to join {entity}", "ERROR")
        
        AsyncRunner.run_in_thread(
            self.manager.group_manager.join_group(entity),
            callback=join_callback
        )
    
    def back_to_main(self):
        """Return to the main menu"""
        self.parentApp.switchForm("MAIN")


# ── Main Menu Form ─────────────────────────────────────────────────────────
class MainMenuForm(npyscreen.Form):
    """Main menu form for the application"""
    
    def create(self):
        self.name = "SPECTRA - Telegram Network Discovery & Archiving"
        
        # Title and description
        self.add(npyscreen.FixedText, value=TITLE)
        self.add(npyscreen.FixedText, value="Integrated Telegram Intelligence Platform")
        self.add(npyscreen.FixedText, value="")
        
        # Options
        self.add(npyscreen.ButtonPress, name="1. Archive Channel/Group", when_pressed_function=self.archive_form)
        self.add(npyscreen.ButtonPress, name="2. Discover Groups", when_pressed_function=self.discovery_form)
        self.add(npyscreen.ButtonPress, name="3. Network Analysis", when_pressed_function=self.graph_form)
        self.add(npyscreen.ButtonPress, name="4. Account Management", when_pressed_function=self.account_form)
        self.add(npyscreen.ButtonPress, name="5. Settings", when_pressed_function=self.settings_form)
        self.add(npyscreen.ButtonPress, name="6. Help & About", when_pressed_function=self.help_form)
        self.add(npyscreen.ButtonPress, name="7. Exit", when_pressed_function=self.exit_app)
        
        # Status
        self.add(npyscreen.FixedText, value="")
        self.status = self.add(npyscreen.TitleFixedText, name="Status:", 
                            value="Initializing components...")
        
        # Initialize the manager
        self.parentApp.setup_manager()
        self.status.value = f"Ready. {len(self.parentApp.manager.config.active_accounts)} accounts available."
        self.status.display()
    
    def archive_form(self):
        """Switch to archive form"""
        self.parentApp.switchForm("ARCHIVE")
    
    def discovery_form(self):
        """Switch to discovery form"""
        self.parentApp.switchForm("DISCOVERY")
    
    def graph_form(self):
        """Switch to graph explorer form"""
        self.parentApp.switchForm("GRAPH")
    
    def account_form(self):
        """Switch to account management form (not implemented yet)"""
        npyscreen.notify_confirm(
            "Account Management not yet implemented in this version.",
            title="Coming Soon"
        )
    
    def settings_form(self):
        """Switch to settings form (not implemented yet)"""
        npyscreen.notify_confirm(
            "Settings Management not yet implemented in this version.",
            title="Coming Soon"
        )
    
    def help_form(self):
        """Show help and about information"""
        about_text = """
SPECTRA Telegram Intelligence Platform v3.0
------------------------------------------

An integrated solution for:
- Archiving Telegram channels and groups
- Discovering new groups from seeds
- Analyzing network relationships
- Batch operations with multi-account support

Features:
• Multi-account support with rotation
• Proxy rotation for OPSEC
• Full message, media, and metadata archiving
• Network analysis and visualization
• Recursive group discovery
• SQLite database for forensic analysis
• Sidecar metadata for media files

© 2023-2025 SWORD-EPI (SPECTRA Team)
"""
        npyscreen.notify_confirm(about_text, title="About SPECTRA")
    
    def exit_app(self):
        """Exit the application"""
        if npyscreen.notify_yes_no(
            "Are you sure you want to exit?",
            title="Confirm Exit"
        ):
            # Clean up
            AsyncRunner.run_async(self.parentApp.manager.close())
            
            # Exit
            self.parentApp.switchForm(None)


# ── Main Application ─────────────────────────────────────────────────────────
class SpectraApp(npyscreen.NPSAppManaged):
    """Main application class"""
    
    def onStart(self):
        """Initialize application forms"""
        self.manager = None  # Will be initialized in setup_manager
        
        # Register forms
        self.addForm("MAIN", MainMenuForm, name="SPECTRA Main Menu")
        self.addForm("ARCHIVE", ArchiveForm, name="SPECTRA Archiver")
        self.addForm("DISCOVERY", DiscoveryForm, name="SPECTRA Group Discovery")
        self.addForm("GRAPH", GraphExplorerForm, name="SPECTRA Network Explorer")
    
    def setup_manager(self):
        """Initialize the integrated manager"""
        if self.manager is None:
            # Load config
            config = Config()
            
            # Create integrated manager
            self.manager = discovery.SpectraCrawlerManager(config)
            
            # Initialize in background
            AsyncRunner.run_in_thread(self.manager.initialize())


# ── Entry point ────────────────────────────────────────────────────────────
def main():
    """Application entry point"""
    try:
        app = SpectraApp()
        app.run()
    except KeyboardInterrupt:
        print("\nApplication terminated by user.")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Make sure we clean up
        if hasattr(app, 'manager') and app.manager:
            AsyncRunner.run_async(app.manager.close())


if __name__ == "__main__":
    main() 