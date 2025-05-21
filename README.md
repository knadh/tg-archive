# SPECTRA

**Spectrally-Processing Extraction, Crawling, & Tele-Reconnaissance Archive**

SPECTRA is an advanced framework for Telegram data collection, network discovery, and forensic-grade archiving with multi-account support, graph-based targeting, and robust OPSEC features.

![SPECTRA](SPECTRA.png)

## Features

- 🔄 **Multi-account & API key rotation** with smart, persistent selection and failure detection
- 🕵️ **Proxy rotation** for OPSEC and anti-detection
- 🔎 **Network discovery** of connected groups and channels (with SQL audit trail)
- 📊 **Graph/network analysis** to identify high-value targets
- 📁 **Forensic archiving** with integrity checksums and sidecar metadata
- 📱 **Topic/thread support** for complete conversation capture
- 🗄️ **SQL database storage** for all discovered groups, relationships, and archive metadata
- ⚡ **Parallel processing** leveraging multiple accounts and proxies simultaneously
- 🖥️ **Modern TUI** (npyscreen) and CLI, both using the same modular backend
- 🛡️ **Red team/OPSEC features**: account/proxy rotation, SQL audit trail, sidecar metadata, persistent state

## Installation

```bash
# Clone the repository
git clone https://github.com/username/SPECTRA.git
cd SPECTRA

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install package in development mode
pip install -e .
```

## Configuration

SPECTRA supports multi-account configuration with automatic account import from `gen_config.py` (TELESMASHER-compatible) and persistent SQL storage for all operations.

### Setting up Telegram API access

1. Visit https://my.telegram.org/apps to register your application
2. Create a config file or use the built-in account import:

```bash
# Import accounts from gen_config.py
python -m tgarchive accounts --import
```

## Usage

SPECTRA can be used in several modes:

### TUI Mode (Terminal User Interface)

```bash
# Launch the interactive TUI
python -m tgarchive
```

- The TUI supports all major workflows: discovery, network analysis, batch/parallel archiving, and account management.
- All TUI and CLI operations use the same modular, OPSEC-aware backend.

### Account Management

```bash
# Import accounts from gen_config.py
python -m tgarchive accounts --import

# List configured accounts and their status
python -m tgarchive accounts --list

# Test all accounts for connectivity
python -m tgarchive accounts --test

# Reset account usage statistics
python -m tgarchive accounts --reset
```

### Discovery Mode

```bash
# Discover groups from a seed entity
python -m tgarchive discover --seed @example_channel --depth 2

# Discover from multiple seeds in a file
python -m tgarchive discover --seeds-file seeds.txt --depth 2 --export discovered.txt

# Import existing scan data
python -m tgarchive discover --crawler-dir ./telegram-groups-crawler/
```

### Network Analysis

```bash
# Analyze network from crawler data
python -m tgarchive network --crawler-dir ./telegram-groups-crawler/ --plot

# Analyze network from SQL database
python -m tgarchive network --from-db --export priority_targets.json --top 50
```

### Archive Mode

```bash
# Archive a specific channel
default
python -m tgarchive archive --entity @example_channel
```

### Batch Operations

```bash
# Process multiple groups from file
python -m tgarchive batch --file groups.txt --delay 30

# Process high-priority groups from database
python -m tgarchive batch --from-db --limit 20 --min-priority 0.1
```

### Parallel Processing

SPECTRA supports parallel processing using multiple Telegram accounts and proxies simultaneously, with full SQL-backed state and OPSEC-aware account/proxy rotation:

```bash
# Run discovery in parallel across multiple accounts
python -m tgarchive parallel discover --seeds-file seeds.txt --depth 2 --max-workers 4

# Join multiple groups in parallel
python -m tgarchive parallel join --file groups.txt --max-workers 4

# Archive multiple entities in parallel
python -m tgarchive parallel archive --file entities.txt --max-workers 4

# Archive high-priority entities from DB in parallel
python -m tgarchive parallel archive --from-db --limit 20 --min-priority 0.1
```

You can also use the global parallel flag with standard commands:

```bash
# Run batch operations in parallel
python -m tgarchive batch --file groups.txt --parallel --max-workers 4

# Run discovery in parallel
python -m tgarchive discover --seeds-file seeds.txt --parallel --max-workers 4
```

---

## Parallel Processing Example Script

A ready-to-use example script is provided to demonstrate parallel discovery, join, and archive operations:

**`SPECTRA/parallel_example.py`**

```bash
# Run parallel discovery, join, and archive from a list of seeds
python SPECTRA/parallel_example.py --seeds-file seeds.txt --max-workers 4 --discover --join --archive --export-file discovered.txt
```

- Supports importing accounts from `gen_config.py` automatically
- All operations are SQL-backed and use persistent account/proxy rotation
- Exports discovered groups to a file if requested
- See the script for more advanced usage and options

---

## Advanced OPSEC & Red Team Features

- **Account & API key rotation**: Smart, persistent, and SQL-audited
- **Proxy rotation**: Supports rotating proxies for every operation
- **SQL audit trail**: All group discovery, joins, and archiving are logged in the database
- **Sidecar metadata**: Forensic metadata and integrity checksums for all archives
- **Persistent state**: All operations are resumable and stateful
- **Modular backend**: All TUI/CLI operations use the same importable modules for maximum reusability
- **Detection/OPSEC notes**: Designed for red team and forensic use, with anti-detection and audit features

---

## Integration & Architecture

- **`SPECTRA/tgarchive/discovery.py`**: Integration point for group crawling, network analysis, parallel archiving, and SQL-backed state
- **`SPECTRA/tgarchive/__main__.py`**: Unified CLI/TUI entry point
- **`SPECTRA/parallel_example.py`**: Example for parallel, multi-account operations
- All modules are importable and can be reused in your own scripts or pipelines

---

## Database Integration

SPECTRA stores all discovery and archiving data in a SQLite database:

- Discovered groups with metadata and priority scores
- Group relationships and network graph data
- Account usage statistics and health metrics
- Archive status tracking

You can specify a custom database path with `--db path/to/database.db`

---

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
