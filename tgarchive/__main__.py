"""
SPECTRA — Telegram Network Discovery & Archiving System
=======================================================

Main entry point for the integrated SPECTRA system.
Provides access to both archiving and discovery capabilities.
"""
from __future__ import annotations

# ── Standard Library ──────────────────────────────────────────────────────
import argparse
import asyncio
import logging
import sys
from pathlib import Path

# ── Local Imports ──────────────────────────────────────────────────────────
from .sync import Config, runner, logger
from .discovery import (
    GroupDiscovery, 
    NetworkAnalyzer, 
    GroupManager, 
    SpectraCrawlerManager,
    ParallelTaskScheduler,
    enhance_config_with_gen_accounts
)

try:
    from .cloud_processor import CloudProcessor
except ImportError:
    CloudProcessor = None # Or handle more gracefully if cloud mode is essential
    logger.debug("CloudProcessor could not be imported. Cloud mode might be unavailable.")

# ── Try to import TUI ────────────────────────────────────────────────────────
try:
    from .spectra_tui import main as tui_main
    HAS_TUI = True
except ImportError:
    HAS_TUI = False

# ── CLI Parser ─────────────────────────────────────────────────────────────
def setup_parser() -> argparse.ArgumentParser:
    """Set up command-line argument parser"""
    parser = argparse.ArgumentParser(
        description="SPECTRA - Telegram Network Discovery & Archiving System"
    )
    
    # Main mode selection
    parser.add_argument("--no-tui", action="store_true", help="Run without TUI interface")
    
    # Global options
    parser.add_argument("--db", type=str, help="Path to SQLite database", default="spectra.db")
    parser.add_argument("--data-dir", type=str, help="Directory for cached data", default="spectra_data")
    parser.add_argument("--config", type=str, help="Path to config file", default="spectra_config.json")
    parser.add_argument("--parallel", action="store_true", help="Enable parallel processing")
    parser.add_argument("--max-workers", type=int, help="Maximum number of parallel workers")
    parser.add_argument("--import-accounts", action="store_true", help="Import accounts from gen_config.py")
    
    # Add subcommands
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Archive command
    archive_parser = subparsers.add_parser("archive", help="Archive a Telegram channel/group")
    archive_parser.add_argument("--entity", required=True, help="Channel/group to archive (e.g. @channel)")
    archive_parser.add_argument("--no-media", action="store_true", help="Don't download media")
    archive_parser.add_argument("--no-avatars", action="store_true", help="Don't download avatars")
    archive_parser.add_argument("--no-topics", action="store_true", help="Don't archive topics/threads")
    archive_parser.add_argument("--auto", action="store_true", help="Use auto-selected account")
    
    # Discover command
    discover_parser = subparsers.add_parser("discover", help="Discover Telegram groups")
    discover_parser.add_argument("--seed", help="Seed entity to start discovery from")
    discover_parser.add_argument("--seeds-file", help="File with multiple seed entities (one per line)")
    discover_parser.add_argument("--depth", type=int, default=1, help="Discovery depth (1-3)")
    discover_parser.add_argument("--export", help="Export discovered groups to file")
    discover_parser.add_argument("--crawler-dir", help="Load data from crawler directory")
    discover_parser.add_argument("--messages", type=int, default=1000, help="Maximum messages to check per entity")
    
    # Network command
    network_parser = subparsers.add_parser("network", help="Analyze network of Telegram groups")
    network_parser.add_argument("--crawler-dir", help="Crawler data directory")
    network_parser.add_argument("--plot", action="store_true", help="Generate network visualization")
    network_parser.add_argument("--metric", default="combined", 
                               choices=["degree", "in_degree", "betweenness", "pagerank", "combined"],
                               help="Metric for importance calculation")
    network_parser.add_argument("--export", help="Export priority targets to file")
    network_parser.add_argument("--top", type=int, default=20, help="Number of top groups to include")
    network_parser.add_argument("--from-db", action="store_true", help="Use groups from database for analysis")
    
    # Batch command
    batch_parser = subparsers.add_parser("batch", help="Batch operations on multiple groups")
    batch_parser.add_argument("--file", help="File with list of groups to process")
    batch_parser.add_argument("--from-db", action="store_true", help="Use priority groups from database")
    batch_parser.add_argument("--delay", type=int, default=60, help="Delay between operations (seconds)")
    batch_parser.add_argument("--limit", type=int, default=10, help="Limit number of groups to process")
    batch_parser.add_argument("--min-priority", type=float, default=0.0, help="Minimum priority score")
    batch_parser.add_argument("--no-leave", action="store_true", help="Don't leave groups after archiving")
    
    # Parallel command
    parallel_parser = subparsers.add_parser("parallel", help="Run operations in parallel across multiple accounts")
    parallel_subparsers = parallel_parser.add_subparsers(dest="parallel_command", help="Parallel command")
    
    # Parallel discover
    parallel_discover = parallel_subparsers.add_parser("discover", help="Parallel discovery")
    parallel_discover.add_argument("--seeds-file", required=True, help="File with seed entities (one per line)")
    parallel_discover.add_argument("--depth", type=int, default=1, help="Discovery depth (1-3)")
    parallel_discover.add_argument("--max-workers", type=int, help="Maximum parallel workers")
    parallel_discover.add_argument("--export", help="Export discovered groups to file")
    
    # Parallel join
    parallel_join = parallel_subparsers.add_parser("join", help="Parallel group joining")
    parallel_join.add_argument("--file", required=True, help="File with groups to join (one per line)")
    parallel_join.add_argument("--max-workers", type=int, help="Maximum parallel workers")
    
    # Parallel archive
    parallel_archive = parallel_subparsers.add_parser("archive", help="Parallel archiving")
    parallel_archive.add_argument("--file", help="File with entities to archive (one per line)")
    parallel_archive.add_argument("--from-db", action="store_true", help="Use priority groups from database")
    parallel_archive.add_argument("--limit", type=int, default=10, help="Limit number of groups")
    parallel_archive.add_argument("--min-priority", type=float, default=0.0, help="Minimum priority score")
    parallel_archive.add_argument("--max-workers", type=int, help="Maximum parallel workers")
    
    # Account command
    account_parser = subparsers.add_parser("accounts", help="Manage Telegram accounts")
    account_parser.add_argument("--list", action="store_true", help="List all accounts and their status")
    account_parser.add_argument("--reset", action="store_true", help="Reset usage counts for all accounts")
    account_parser.add_argument("--test", action="store_true", help="Test all accounts for connectivity")
    account_parser.add_argument("--import", action="store_true", dest="import_accs", help="Import accounts from gen_config.py")
    
    # Cloud command
    cloud_parser = subparsers.add_parser("cloud", help="Run in cloud mode for targeted channel traversal and downloading.")
    cloud_parser.add_argument("--channels-file", type=str, required=True, help="Path to a file containing the initial list of channel URLs or IDs (one per line).")
    cloud_parser.add_argument("--output-dir", type=str, required=True, help="Directory to store downloaded files and logs for the cloud mode session.")
    cloud_parser.add_argument("--max-depth", type=int, default=2, help="Maximum depth to follow channel links during discovery (default: 2).")
    cloud_parser.add_argument("--min-files-gateway", type=int, default=100, help="Minimum number of files a channel must have to be considered a 'gateway' for focused downloading (default: 100).")
    
    return parser

# ── Command handlers ───────────────────────────────────────────────────────
async def handle_archive(args: argparse.Namespace) -> int:
    """Handle archive command"""
    cfg = Config(Path(args.config))
    
    # Import accounts from gen_config if requested
    if args.import_accounts:
        cfg = enhance_config_with_gen_accounts(cfg)
    
    # Set entity and options
    cfg.data["entity"] = args.entity
    cfg.data["download_media"] = not args.no_media
    cfg.data["download_avatars"] = not args.no_avatars
    cfg.data["archive_topics"] = not args.no_topics
    cfg.data["db_path"] = args.db
    
    # Use auto-selected account or default
    account = cfg.auto_select_account() if args.auto else None
    
    try:
        await runner(cfg, account)
        logger.info(f"Archive of {args.entity} complete")
        return 0
    except Exception as e:
        logger.error(f"Archive failed: {e}")
        return 1

async def handle_discover(args: argparse.Namespace) -> int:
    """Handle discover command"""
    cfg = Config(Path(args.config))
    
    # Import accounts from gen_config if requested
    if args.import_accounts:
        cfg = enhance_config_with_gen_accounts(cfg)
        
    data_dir = Path(args.data_dir)
    db_path = Path(args.db)
    
    # Use parallel processing if requested
    if args.parallel and args.seeds_file:
        return await handle_parallel_discover(args)
    
    # Initialize manager with database support
    manager = SpectraCrawlerManager(
        config=cfg,
        data_dir=data_dir,
        db_path=db_path
    )
    
    if not await manager.initialize():
        logger.error("Failed to initialize crawler manager")
        return 1
    
    try:
        if args.seed:
            # Discover from seed
            logger.info(f"Starting discovery from {args.seed} with depth {args.depth}")
            discovered = await manager.discover_from_seed(
                args.seed, 
                depth=args.depth,
                max_messages=args.messages
            )
            logger.info(f"Discovered {len(discovered)} groups")
        elif args.seeds_file:
            # Load seeds from file
            seeds_path = Path(args.seeds_file)
            if not seeds_path.exists():
                logger.error(f"Seeds file not found: {seeds_path}")
                return 1
                
            with open(seeds_path, 'r') as f:
                seeds = [line.strip() for line in f if line.strip()]
                
            if not seeds:
                logger.error(f"No seeds found in {seeds_path}")
                return 1
                
            # Process each seed sequentially
            all_discovered = set()
            for i, seed in enumerate(seeds):
                logger.info(f"Processing seed {i+1}/{len(seeds)}: {seed}")
                discovered = await manager.discover_from_seed(
                    seed, 
                    depth=args.depth,
                    max_messages=args.messages
                )
                all_discovered.update(discovered)
                
            logger.info(f"Discovered {len(all_discovered)} groups from {len(seeds)} seeds")
        elif args.crawler_dir:
            # Load from crawler data
            crawler_dir = Path(args.crawler_dir)
            if not crawler_dir.exists():
                logger.error(f"Crawler directory not found: {crawler_dir}")
                return 1
                
            if not manager.discovery:
                logger.error("Discovery component not initialized")
                return 1
                
            discovered = await manager.discovery.load_crawler_data(crawler_dir)
            
            # Save to database if using db
            if db_path.exists():
                await manager._save_discovered_groups(discovered, "crawler_import")
                logger.info(f"Saved {len(discovered)} groups to database")
                
            logger.info(f"Loaded {len(discovered)} groups from crawler data")
        else:
            logger.error("Either --seed, --seeds-file or --crawler-dir must be specified")
            return 1
            
        # Export if requested
        if args.export:
            if manager.discovery and manager.discovery.discovered_groups:
                output_path = manager.discovery.export_groups_to_file(args.export)
                if output_path:
                    logger.info(f"Exported {len(manager.discovery.discovered_groups)} groups to {output_path}")
                else:
                    logger.error("Failed to export groups")
                    return 1
            else:
                logger.error("No groups to export")
                return 1
                
        await manager.close()
        return 0
            
    except Exception as e:
        logger.error(f"Discovery failed: {e}")
        await manager.close()
        return 1

async def handle_network(args: argparse.Namespace) -> int:
    """Handle network command"""
    cfg = Config(Path(args.config))
    
    # Import accounts from gen_config if requested
    if args.import_accounts:
        cfg = enhance_config_with_gen_accounts(cfg)
        
    data_dir = Path(args.data_dir)
    db_path = Path(args.db)
    
    # Initialize manager with database support
    manager = SpectraCrawlerManager(
        config=cfg,
        data_dir=data_dir,
        db_path=db_path
    )
    
    try:
        if args.from_db and db_path.exists():
            # Use database for network analysis
            logger.info("Using database for network analysis")
            await manager.initialize()
            await manager._update_group_priorities()
            
            # Get priority targets
            targets = await manager.get_priority_targets(args.top)
            if not targets:
                logger.error("No groups found in database for analysis")
                return 1
                
            logger.info(f"Analyzed {len(targets)} groups from database")
            
            # Export if requested
            if args.export and targets:
                try:
                    # Write targets to JSON file
                    import json
                    with open(args.export, 'w') as f:
                        json.dump(targets, f, indent=2)
                    logger.info(f"Exported {len(targets)} priority targets to {args.export}")
                except Exception as e:
                    logger.error(f"Failed to export targets: {e}")
                    
            # Display top targets
            for i, target in enumerate(targets[:5]):  # Show top 5
                logger.info(f"  {i+1}. {target['id']} (Score: {target['priority']})")
                
        elif args.crawler_dir:
            crawler_dir = Path(args.crawler_dir)
            if not crawler_dir.exists():
                logger.error(f"Crawler directory not found: {crawler_dir}")
                return 1
            
            # Initialize with db support
            await manager.initialize()
            
            # Load and analyze network
            targets = await manager.load_and_analyze_network(crawler_dir)
            if not targets:
                logger.error("Failed to analyze network data")
                return 1
                
            logger.info(f"Analyzed network with {len(targets)} priority targets")
            
            # Plot if requested
            if args.plot:
                output_file = manager.network_analyzer.plot_network(metric=args.metric)
                if output_file:
                    logger.info(f"Network visualization saved to {output_file}")
                else:
                    logger.error("Failed to generate visualization")
                    
            # Export if requested
            if args.export and targets:
                # Already exported via load_and_analyze_network if using db
                if not db_path.exists():
                    try:
                        # Write targets to JSON file
                        import json
                        with open(args.export, 'w') as f:
                            json.dump(targets, f, indent=2)
                        logger.info(f"Exported {len(targets)} priority targets to {args.export}")
                    except Exception as e:
                        logger.error(f"Failed to export targets: {e}")
                    
            # Display top targets
            for i, target in enumerate(targets[:5]):  # Show top 5
                score_key = "priority" if "priority" in target else "score"
                logger.info(f"  {i+1}. {target['id']} (Score: {target.get(score_key, 0.0)})")
        else:
            logger.error("Either --crawler-dir or --from-db must be specified")
            return 1
            
        await manager.close()
        return 0
            
    except Exception as e:
        logger.error(f"Network analysis failed: {e}")
        if manager:
            await manager.close()
        return 1

async def handle_batch(args: argparse.Namespace) -> int:
    """Handle batch command"""
    cfg = Config(Path(args.config))
    
    # Import accounts from gen_config if requested
    if args.import_accounts:
        cfg = enhance_config_with_gen_accounts(cfg)
        
    data_dir = Path(args.data_dir)
    db_path = Path(args.db)
    
    # Use parallel processing if requested
    if args.parallel:
        return await handle_parallel_archive(args)
    
    # Initialize manager with database support
    manager = SpectraCrawlerManager(
        config=cfg,
        data_dir=data_dir,
        db_path=db_path
    )
    
    if not await manager.initialize():
        logger.error("Failed to initialize crawler manager")
        return 1
    
    try:
        groups_to_process = []
        
        if args.file:
            # Load groups from file
            file_path = Path(args.file)
            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                return 1
                
            with open(file_path, 'r') as f:
                groups_to_process = [line.strip() for line in f if line.strip()]
                
            logger.info(f"Loaded {len(groups_to_process)} groups from file")
            
        elif args.from_db:
            # Get priority groups from database
            targets = await manager.get_priority_targets(
                top_n=args.limit,
                min_priority=args.min_priority
            )
            
            if not targets:
                logger.error("No suitable targets found in database")
                return 1
                
            groups_to_process = [t["id"] for t in targets]
            logger.info(f"Selected {len(groups_to_process)} priority groups from database")
            
        else:
            logger.error("Either --file or --from-db must be specified")
            return 1
            
        # Limit number of groups if needed
        if args.limit and len(groups_to_process) > args.limit:
            groups_to_process = groups_to_process[:args.limit]
            logger.info(f"Limited to {len(groups_to_process)} groups")
            
        # Process groups
        leave_after = not args.no_leave
        results = await manager.group_manager.batch_join_archive(
            groups_to_process,
            delay=args.delay,
            leave_after=leave_after
        )
        
        # Show results
        success_count = sum(1 for v in results.values() if v)
        logger.info(f"Processed {len(results)} groups: {success_count} succeeded, {len(results) - success_count} failed")
        
        await manager.close()
        return 0
        
    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
        await manager.close()
        return 1

async def handle_accounts(args: argparse.Namespace) -> int:
    """Handle accounts command"""
    cfg = Config(Path(args.config))
    db_path = Path(args.db)
    
    # Import accounts from gen_config if requested
    if args.import_accounts or getattr(args, "import_accs", False):
        cfg = enhance_config_with_gen_accounts(cfg)
        cfg.save()
        logger.info("Saved updated config with imported accounts")
    
    # Create an account rotator to manage accounts
    manager = GroupManager(cfg, db_path=db_path)
    
    try:
        if args.list:
            # List all accounts and their status
            account_stats = manager.account_rotator.get_account_stats()
            
            if not account_stats:
                logger.info("No account statistics available")
                return 0
                
            logger.info(f"Account statistics ({len(account_stats)} accounts):")
            for i, stats in enumerate(account_stats):
                status = "BANNED" if stats.get("is_banned") else "OK"
                cooldown = f" (cooldown until {stats.get('cooldown_until')})" if stats.get("cooldown_until") else ""
                logger.info(f"  {i+1}. {stats['session']}: Usage: {stats['usage']}, Status: {status}{cooldown}")
                if stats.get("last_error"):
                    logger.info(f"     Last error: {stats['last_error']}")
                    
        if args.reset:
            # Reset usage counts
            manager.account_rotator.reset_usage_counts()
            logger.info("Reset usage counts for all accounts")
            
        if args.test:
            # Test all accounts
            await manager.init_clients()
            
            if not manager.clients:
                logger.error("No clients initialized - check account credentials")
                return 1
                
            logger.info(f"Successfully connected to {len(manager.clients)} accounts:")
            for session_name in manager.clients:
                logger.info(f"  - {session_name}")
                
        return 0
            
    except Exception as e:
        logger.error(f"Account management failed: {e}")
        return 1

async def handle_cloud(args: argparse.Namespace) -> int:
    """Handle cloud command"""
    logger.info("Cloud mode invoked with the following arguments:")
    logger.info(f"  Channels file: {args.channels_file}")
    logger.info(f"  Output directory: {args.output_dir}")
    logger.info(f"  Max depth: {args.max_depth}")
    logger.info(f"  Min files for gateway: {args.min_files_gateway}")

    # Load configuration
    cfg = Config(Path(args.config))
    
    # Note: Global args.import_accounts is handled in async_main, 
    # which calls enhance_config_with_gen_accounts and saves the config.
    # Thus, cfg here should reflect any imported accounts.

    accounts = cfg.accounts
    if not accounts:
        logger.error("No API accounts configured. Cannot proceed with cloud mode.")
        logger.error("Please configure accounts, e.g., by running `python gen_config.py` and then `python -m tgarchive accounts --import` or by ensuring spectra_config.json has accounts.")
        return 1

    selected_account = accounts[0]  # Select the first available account
    logger.info(f"Cloud mode will use the single API account: {selected_account.get('session_name', 'N/A')}")
    logger.info(f"Account details (for verification): API ID {selected_account.get('api_id')}")

    if CloudProcessor is None:
        logger.error("CloudProcessor is not available. Cannot run cloud mode. Please check for import errors.")
        return 1

    logger.info(f"Initializing CloudProcessor with output directory: {args.output_dir}")
    # Ensure output directory exists before CloudProcessor tries to use it for logging
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    processor = CloudProcessor(
        selected_account=selected_account,
        channels_file=args.channels_file,
        output_dir=args.output_dir,
        max_depth=args.max_depth,
        min_files_gateway=args.min_files_gateway,
        config=cfg
    )

    try:
        logger.info("Starting cloud mode channel processing...")
        await processor.process_channels()
        logger.info("Cloud mode processing completed successfully.")
        return 0  # Success
    except Exception as e:
        # Log the full traceback for unexpected errors
        logger.error(f"An critical error occurred during cloud mode processing: {e}", exc_info=True)
        return 1  # Failure

# ── Parallel operation handlers ───────────────────────────────────────────────
async def handle_parallel_discover(args: argparse.Namespace) -> int:
    """Handle parallel discover command"""
    cfg = Config(Path(args.config))
    
    # Import accounts from gen_config if requested
    if args.import_accounts:
        cfg = enhance_config_with_gen_accounts(cfg)
        
    data_dir = Path(args.data_dir)
    db_path = Path(args.db)
    
    # Determine if this is from the dedicated parallel command
    is_parallel_command = hasattr(args, 'parallel_command') and args.parallel_command == 'discover'
    seeds_file = args.seeds_file
    
    if not seeds_file:
        logger.error("Missing --seeds-file argument")
        return 1
        
    seeds_path = Path(seeds_file)
    if not seeds_path.exists():
        logger.error(f"Seeds file not found: {seeds_path}")
        return 1
        
    # Load seed entities
    with open(seeds_path, 'r') as f:
        seeds = [line.strip() for line in f if line.strip()]
        
    if not seeds:
        logger.error(f"No seeds found in {seeds_path}")
        return 1
        
    logger.info(f"Loaded {len(seeds)} seed entities")
    
    # Set up parallel scheduler
    max_workers = args.max_workers
    if is_parallel_command and hasattr(args, 'max_workers'):
        max_workers = args.max_workers
    
    scheduler = ParallelTaskScheduler(
        config=cfg,
        db_path=db_path,
        max_workers=max_workers
    )
    
    try:
        # Initialize scheduler
        if not await scheduler.initialize():
            logger.error("Failed to initialize parallel scheduler")
            return 1
            
        # Run parallel discovery
        depth = args.depth
        max_messages = getattr(args, 'messages', 1000)
        
        logger.info(f"Starting parallel discovery of {len(seeds)} seeds with depth {depth}")
        results = await scheduler.parallel_discovery(
            seeds, 
            depth=depth,
            max_messages=max_messages,
            max_concurrent=max_workers
        )
        
        # Process results
        total_discovered = 0
        for seed, discovered in results.items():
            if discovered:
                total_discovered += len(discovered)
                
        logger.info(f"Parallel discovery complete: {total_discovered} groups discovered from {len(seeds)} seeds")
        
        # Export if requested
        if args.export:
            # Combine all discovered groups
            all_discovered = set()
            for discovered in results.values():
                all_discovered.update(discovered)
                
            try:
                # Export to file
                with open(args.export, 'w') as f:
                    for group in sorted(all_discovered):
                        f.write(f"{group}\n")
                logger.info(f"Exported {len(all_discovered)} groups to {args.export}")
            except Exception as e:
                logger.error(f"Failed to export discovered groups: {e}")
                
        await scheduler.close()
        return 0
        
    except Exception as e:
        logger.error(f"Parallel discovery failed: {e}")
        await scheduler.close()
        return 1

async def handle_parallel_join(args: argparse.Namespace) -> int:
    """Handle parallel join command"""
    cfg = Config(Path(args.config))
    
    # Import accounts from gen_config if requested
    if args.import_accounts:
        cfg = enhance_config_with_gen_accounts(cfg)
        
    db_path = Path(args.db)
    
    # Load groups from file
    if not args.file:
        logger.error("Missing --file argument")
        return 1
        
    file_path = Path(args.file)
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return 1
        
    with open(file_path, 'r') as f:
        groups = [line.strip() for line in f if line.strip()]
        
    if not groups:
        logger.error(f"No groups found in {file_path}")
        return 1
        
    logger.info(f"Loaded {len(groups)} groups to join")
    
    # Set up parallel scheduler
    max_workers = None
    if hasattr(args, 'max_workers'):
        max_workers = args.max_workers
    
    scheduler = ParallelTaskScheduler(
        config=cfg,
        db_path=db_path,
        max_workers=max_workers
    )
    
    try:
        # Initialize scheduler
        if not await scheduler.initialize():
            logger.error("Failed to initialize parallel scheduler")
            return 1
            
        # Run parallel join
        logger.info(f"Starting parallel join of {len(groups)} groups")
        results = await scheduler.parallel_join(
            groups,
            max_concurrent=max_workers
        )
        
        # Process results
        success_count = sum(1 for entity_id in results.values() if entity_id is not None)
        logger.info(f"Parallel join complete: {success_count}/{len(groups)} groups joined successfully")
        
        await scheduler.close()
        return 0
        
    except Exception as e:
        logger.error(f"Parallel join failed: {e}")
        await scheduler.close()
        return 1

async def handle_parallel_archive(args: argparse.Namespace) -> int:
    """Handle parallel archive command"""
    cfg = Config(Path(args.config))
    
    # Import accounts from gen_config if requested
    if args.import_accounts:
        cfg = enhance_config_with_gen_accounts(cfg)
        
    data_dir = Path(args.data_dir)
    db_path = Path(args.db)
    
    # Set up parallel scheduler
    max_workers = None
    if hasattr(args, 'max_workers'):
        max_workers = args.max_workers
    
    scheduler = ParallelTaskScheduler(
        config=cfg,
        db_path=db_path,
        max_workers=max_workers
    )
    
    # Get entities to archive
    entities = []
    
    # Determine source
    is_parallel_command = hasattr(args, 'parallel_command') and args.parallel_command == 'archive'
    
    if (is_parallel_command and args.file) or (hasattr(args, 'file') and args.file):
        # Load from file
        file_path = Path(args.file)
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return 1
            
        with open(file_path, 'r') as f:
            entities = [line.strip() for line in f if line.strip()]
            
        logger.info(f"Loaded {len(entities)} entities from file")
        
    elif (is_parallel_command and args.from_db) or (hasattr(args, 'from_db') and args.from_db):
        # Initialize crawler manager to get entities from DB
        manager = SpectraCrawlerManager(
            config=cfg,
            data_dir=data_dir,
            db_path=db_path
        )
        
        if not await manager.initialize():
            logger.error("Failed to initialize manager for DB access")
            return 1
            
        # Get priority targets
        limit = args.limit if hasattr(args, 'limit') else 10
        min_priority = args.min_priority if hasattr(args, 'min_priority') else 0.0
        
        targets = await manager.get_priority_targets(
            top_n=limit,
            min_priority=min_priority
        )
        
        if not targets:
            logger.error("No suitable targets found in database")
            return 1
            
        entities = [t["id"] for t in targets]
        logger.info(f"Selected {len(entities)} priority entities from database")
        await manager.close()
        
    else:
        logger.error("Either --file or --from-db must be specified")
        return 1
    
    try:
        # Initialize scheduler
        if not await scheduler.initialize():
            logger.error("Failed to initialize parallel scheduler")
            return 1
            
        # Run parallel archive
        logger.info(f"Starting parallel archive of {len(entities)} entities")
        results = await scheduler.parallel_archive(
            entities,
            max_concurrent=max_workers
        )
        
        # Process results
        success_count = sum(1 for success in results.values() if success)
        logger.info(f"Parallel archive complete: {success_count}/{len(entities)} entities archived successfully")
        
        await scheduler.close()
        return 0
        
    except Exception as e:
        logger.error(f"Parallel archive failed: {e}")
        await scheduler.close()
        return 1

# ── Parallel command handler ───────────────────────────────────────────────
async def handle_parallel(args: argparse.Namespace) -> int:
    """Handle parallel command and its subcommands"""
    if args.parallel_command == "discover":
        return await handle_parallel_discover(args)
    elif args.parallel_command == "join":
        return await handle_parallel_join(args)
    elif args.parallel_command == "archive":
        return await handle_parallel_archive(args)
    else:
        logger.error(f"Unknown parallel command: {args.parallel_command}")
        return 1

# ── Main function ───────────────────────────────────────────────────────────
async def async_main(args: argparse.Namespace) -> int:
    """Async entry point for command-line application"""
    
    # Import accounts from gen_config if requested
    if args.import_accounts:
        cfg = Config(Path(args.config))
        cfg = enhance_config_with_gen_accounts(cfg)
        cfg.save()
        logger.info(f"Imported accounts from gen_config.py and saved to {args.config}")
    
    # If TUI mode is requested and available
    if not args.no_tui and HAS_TUI:
        try:
            # Pass any global options to the TUI
            tui_options = {
                "db_path": args.db,
                "data_dir": args.data_dir,
                "config_path": args.config,
                "parallel": args.parallel,
                "max_workers": args.max_workers
            }
            return await tui_main(tui_options)
        except Exception as e:
            logger.error(f"TUI failed: {e}")
            return 1
    
    # Handle CLI commands
    if args.command == "archive":
        return await handle_archive(args)
    elif args.command == "discover":
        return await handle_discover(args)
    elif args.command == "network":
        return await handle_network(args)
    elif args.command == "batch":
        return await handle_batch(args)
    elif args.command == "accounts":
        return await handle_accounts(args)
    elif args.command == "parallel":
        return await handle_parallel(args)
    elif args.command == "cloud":
        return await handle_cloud(args)
    else:
        # No command or unrecognized command
        if HAS_TUI:
            # Default to TUI if no command specified
            try:
                tui_options = {
                    "db_path": args.db,
                    "data_dir": args.data_dir,
                    "config_path": args.config,
                    "parallel": args.parallel,
                    "max_workers": args.max_workers
                }
                return await tui_main(tui_options)
            except Exception as e:
                logger.error(f"TUI failed: {e}")
                return 1
        else:
            logger.error("No command specified and TUI not available. Use --help to see available commands.")
            return 1

def main() -> int:
    """Command-line entry point"""
    parser = setup_parser()
    args = parser.parse_args()
    
    # Launch TUI if no command given and TUI is available
    if not args.command and not args.no_tui and HAS_TUI:
        return tui_main()
    
    # Otherwise process command
    try:
        return asyncio.run(async_main(args))
    except KeyboardInterrupt:
        logger.info("Operation interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Unhandled error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 