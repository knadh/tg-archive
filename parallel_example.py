#!/usr/bin/env python3
"""
SPECTRA Parallel Processing Example
==================================

This script demonstrates how to use SPECTRA's parallel processing capabilities
to download and process multiple Telegram channels simultaneously using multiple accounts.

Usage:
    python parallel_example.py --seeds-file seeds.txt --max-workers 4
"""

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("parallel_demo.log")
    ]
)
logger = logging.getLogger("parallel_demo")

# Import SPECTRA components
from tgarchive.discovery import (
    Config, 
    ParallelTaskScheduler,
    enhance_config_with_gen_accounts,
    import_accounts_from_gen_config
)

async def run_parallel_demo(args):
    """Run the parallel processing demonstration"""
    
    # Step 1: Load or create configuration
    config_path = Path(args.config_file)
    if config_path.exists():
        logger.info(f"Loading config from {config_path}")
        config = Config(config_path)
    else:
        logger.info("Creating new config")
        config = Config()
    
    # Step 2: Import accounts from gen_config.py if needed
    if not config.accounts or args.import_accounts:
        logger.info("Importing accounts from gen_config.py")
        gen_accounts = import_accounts_from_gen_config()
        if gen_accounts:
            config.data["accounts"] = gen_accounts
            logger.info(f"Imported {len(gen_accounts)} accounts")
            # Save updated config
            if args.save_config:
                config.save()
                logger.info(f"Saved updated config to {config_path}")
        else:
            logger.warning("No accounts found in gen_config.py")
            if not config.accounts:
                logger.error("No accounts available - cannot continue")
                return 1
    
    # Step 3: Configure the parallel task scheduler
    db_path = Path(args.db_path)
    max_workers = args.max_workers or min(4, len(config.accounts))
    
    scheduler = ParallelTaskScheduler(
        config=config,
        db_path=db_path,
        max_workers=max_workers
    )
    
    # Step 4: Initialize the scheduler (connects to accounts)
    logger.info(f"Initializing scheduler with {max_workers} workers")
    if not await scheduler.initialize():
        logger.error("Failed to initialize scheduler - check account credentials")
        return 1
        
    success = True
    
    try:
        # Step 5: Load seed entities from file
        if args.seeds_file:
            seeds_path = Path(args.seeds_file)
            if not seeds_path.exists():
                logger.error(f"Seeds file not found: {seeds_path}")
                return 1
                
            with open(seeds_path, 'r') as f:
                seeds = [line.strip() for line in f if line.strip()]
                
            if not seeds:
                logger.error(f"No seeds found in {seeds_path}")
                return 1
                
            logger.info(f"Loaded {len(seeds)} seed entities from {seeds_path}")
            
            # Step 6: Run parallel discovery
            if args.discover:
                logger.info(f"Starting parallel discovery with depth {args.depth}")
                
                discovered = await scheduler.parallel_discovery(
                    seeds,
                    depth=args.depth,
                    max_messages=args.max_messages,
                    max_concurrent=max_workers
                )
                
                # Process results
                total_discovered = 0
                for seed, links in discovered.items():
                    if links:
                        total_discovered += len(links)
                        
                logger.info(f"Discovery complete: found {total_discovered} groups from {len(seeds)} seeds")
                
                # Export discovered links if requested
                if args.export_file:
                    all_discovered = set()
                    for links in discovered.values():
                        all_discovered.update(links)
                        
                    export_path = Path(args.export_file)
                    with open(export_path, 'w') as f:
                        for link in sorted(all_discovered):
                            f.write(f"{link}\n")
                            
                    logger.info(f"Exported {len(all_discovered)} discovered links to {export_path}")
            
            # Step 7: Run parallel join
            if args.join:
                logger.info(f"Starting parallel join operation for {len(seeds)} groups")
                
                join_results = await scheduler.parallel_join(
                    seeds,
                    max_concurrent=max_workers
                )
                
                success_count = sum(1 for entity_id in join_results.values() if entity_id is not None)
                logger.info(f"Join operation complete: {success_count}/{len(seeds)} groups joined successfully")
            
            # Step 8: Run parallel archive
            if args.archive:
                logger.info(f"Starting parallel archive operation for {len(seeds)} entities")
                
                archive_results = await scheduler.parallel_archive(
                    seeds,
                    max_concurrent=max_workers
                )
                
                success_count = sum(1 for success in archive_results.values() if success)
                logger.info(f"Archive operation complete: {success_count}/{len(seeds)} entities archived successfully")
                
                if success_count < len(seeds):
                    success = False
        
        # Step 9: Display account statistics
        account_stats = scheduler.group_manager.account_rotator.get_account_stats()
        logger.info(f"Account statistics:")
        for i, stats in enumerate(account_stats):
            status = "BANNED" if stats.get("is_banned") else "OK"
            logger.info(f"  {i+1}. {stats['session']}: Usage: {stats['usage']}, Success: {stats.get('success_count', 0)}, Status: {status}")
            if stats.get("last_error"):
                logger.info(f"     Last error: {stats['last_error']}")
    
    except KeyboardInterrupt:
        logger.info("Operation interrupted by user")
        success = False
    except Exception as e:
        logger.error(f"Error during parallel processing: {e}")
        success = False
    finally:
        # Always close clients properly
        await scheduler.close()
        logger.info("Scheduler closed - all clients disconnected")
    
    return 0 if success else 1

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="SPECTRA Parallel Processing Demo")
    
    # Configuration
    parser.add_argument("--config-file", default="spectra_config.json", help="Path to config file")
    parser.add_argument("--db-path", default="spectra.db", help="Path to SQLite database")
    parser.add_argument("--import-accounts", action="store_true", help="Import accounts from gen_config.py")
    parser.add_argument("--save-config", action="store_true", help="Save updated config after import")
    
    # Operation control
    parser.add_argument("--max-workers", type=int, help="Maximum number of parallel workers")
    parser.add_argument("--seeds-file", help="File with seed entities (one per line)")
    
    # Operations to perform
    parser.add_argument("--discover", action="store_true", help="Run discovery operation")
    parser.add_argument("--join", action="store_true", help="Run join operation")
    parser.add_argument("--archive", action="store_true", help="Run archive operation")
    
    # Discovery options
    parser.add_argument("--depth", type=int, default=1, help="Discovery depth (1-3)")
    parser.add_argument("--max-messages", type=int, default=1000, help="Maximum messages to check per entity")
    parser.add_argument("--export-file", help="Export discovered groups to file")
    
    return parser.parse_args()

def main():
    """Command-line entry point"""
    args = parse_args()
    
    # Default to all operations if none specified
    if not any([args.discover, args.join, args.archive]):
        args.discover = True
        args.join = True
        args.archive = False  # Default to not archiving as it's slower
    
    try:
        return asyncio.run(run_parallel_demo(args))
    except KeyboardInterrupt:
        logger.info("Operation interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Unhandled error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
