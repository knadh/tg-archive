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
- ☁️ **Cloud Mode:** Traverse a series of channels, discover related channels, and download text/archive files with specific rules, using a single API key.
- 🛡️ **Red team/OPSEC features**: account/proxy rotation, SQL audit trail, sidecar metadata, persistent state

## Installation

```bash
# Clone the repository
git clone https://github.com/SWORDIntel/SPECTRA.git
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

### Cloud Mode

This mode is designed for automated traversal and targeted downloading from an initial set of seed channels. It uses a single API key to explore channels, discover new ones through links in messages (up to a defined depth), and download specific file types (text and common archives) into an organized output directory.

**Command Structure:**
```bash
python -m tgarchive cloud --channels-file <path_to_channels.txt> --output-dir <path_to_output_directory> [options]
```

**Arguments:**

*   `--channels-file PATH`: Required. Path to a text file containing the initial list of seed channel URLs or IDs (one per line).
*   `--output-dir PATH`: Required. Directory where downloaded files (in `text_files/` and `archive_files/` subfolders) and the `cloud_download_log.csv` will be stored.
*   `--max-depth INT`: Optional. Maximum depth to follow channel links during discovery. Default is 2.
*   `--min-files-gateway INT`: Optional. Minimum number of files a channel should ideally have to be considered a 'gateway' for focused downloading (Note: current implementation downloads from all accessible discovered channels; this option is for future refinement). Default is 100.

**API Key Usage:**

Cloud mode is designed to use a single API key (specifically, the first account configured in your `spectra_config.json` or imported from `gen_config.py`) for all its operations. This is to avoid potentially joining the same channel with multiple accounts, which might be undesirable for certain operational goals.

**Output Structure:**

In the specified output directory, you will find:

*   `text_files/`: Contains downloaded plain text files.
*   `archive_files/`: Contains downloaded archive files (e.g., .zip, .rar) along with their metadata in `.json` sidecar files (e.g., `example.zip.json`).
*   `cloud_download_log.csv`: A CSV log detailing every downloaded file, its source channel, message ID, timestamp, and other metadata.

**Running Long Cloud Sessions:**

For extended cloud mode operations, it is highly recommended to use a terminal multiplexer like `screen` or `tmux` to ensure the process continues running even if your connection drops.

Example using `screen`:
1. Start a new screen session: `screen -S spectra_cloud_session`
2. Run the command: `python -m tgarchive cloud --channels-file your_seeds.txt --output-dir ./cloud_output`
3. Detach from the session: Press `Ctrl+A` then `D`.
4. To reattach later: `screen -r spectra_cloud_session`

SPECTRA will not install `screen` or `tmux` for you. Please install them using your system's package manager if needed (e.g., `sudo apt install screen`).

---

## Message Forwarding Features

SPECTRA includes powerful features for forwarding messages with attachments from origin channels/chats to a specified destination, or even to the "Saved Messages" of multiple configured accounts. This can be useful for consolidating information, creating backups, or distributing content.

### Forwarding Modes

1.  **Selective Forwarding:** Forward messages from a specific origin to a specific destination.
    ```bash
    python -m tgarchive forward --origin <origin_id_or_username> --destination <destination_id_or_username>
    ```

2.  **Total Forward Mode:** Forward messages from all channels accessible by your configured accounts (as listed in the `account_channel_access` table) to a specific destination. This mode requires the channel access table to be populated first.
    ```bash
    python -m tgarchive forward --total-mode [--destination <destination_id_or_username>]
    ```
    To populate the `account_channel_access` table, run:
    ```bash
    python -m tgarchive channels --update-access
    ```

### Forwarding Command Details (`tgarchive forward`)

The main command for forwarding is `python -m tgarchive forward` with the following options:

*   `--origin <id_or_username>`: Specifies the source channel or chat from which to forward messages. This is required unless `--total-mode` is used.
*   `--destination <id_or_username>`: Specifies the target channel or chat to which messages will be forwarded. If not provided, SPECTRA will use the `default_forwarding_destination_id` set in your `spectra_config.json` file.
*   `--account <phone_or_session_name>`: Specifies which configured Telegram account to use for the forwarding operation. If not provided, the first account in your configuration is typically used. For "Total Forward Mode", this account is used for orchestration, while individual channel forwarding uses an account known to have access to that specific channel (from the `account_channel_access` table).
*   `--total-mode`: Enables "Total Forward Mode". When this flag is used, the `--origin` argument is ignored, and SPECTRA will attempt to forward messages from all channels recorded in the `account_channel_access` database table.
*   `--forward-to-all-saved`: When enabled, messages successfully forwarded to the main destination will *also* be forwarded to the "Saved Messages" of *every account* configured in `spectra_config.json`. This can be useful for creating broad personal backups but will significantly increase API calls and data redundancy. Use with caution.
*   `--prepend-origin-info`: If enabled, and if not using topic-based forwarding (see below), information about the original channel (e.g., "[Forwarded from OriginalChannelName (ID: 12345)]") will be prepended to the text of the forwarded message. This helps in identifying the source of messages when they are consolidated into a general channel.

### Related Configuration and Utility Commands

*   **Setting Default Destination:**
    ```bash
    python -m tgarchive config --set-forward-dest <destination_id_or_username>
    ```
    This command updates the `default_forwarding_destination_id` in your `spectra_config.json`.

*   **Viewing Default Destination:**
    ```bash
    python -m tgarchive config --view-forward-dest
    ```

*   **Updating Channel Access Data (for Total Mode):**
    ```bash
    python -m tgarchive channels --update-access
    ```
    This command populates the `account_channel_access` table in the database by iterating through all your configured accounts and listing the channels each can access. This table is crucial for the `--total-mode` forwarding feature.

### Configuration for Forwarding

*   **`default_forwarding_destination_id`**: Located in `spectra_config.json`, this key (added manually or via the `config --set-forward-dest` command) allows you to set a global default destination for forwarding operations, so you don't have to specify `--destination` every time.
*   **Supergroup Topic Sorting (Conceptual):**
    Telegram's "Topics" feature in supergroups allows for organized discussions. SPECTRA's forwarding can conceptually support sending messages into specific topics. This is typically done by forwarding a message as a *reply* to the message that represents the topic's creation or its main "general" topic message.
    If you manually identify the message ID for a specific topic in the destination supergroup, this ID could be used (currently via code modification or future enhancement as `destination_topic_id` in the `AttachmentForwarder`) with the `reply_to` parameter in Telegram's API when forwarding.
    Currently, SPECTRA does **not** automatically create or manage topics by name due to limitations with user accounts (topic creation/management often requires bot privileges or specific admin rights).
    The `--prepend-origin-info` flag is the primary method for distinguishing messages from different origins when forwarded to a common, non-topic-based channel.

### "Forward to All Saved Messages" Feature

Enabling `--forward-to-all-saved` provides a way to create a distributed backup or personal archive of forwarded content across all your configured Telegram accounts. Each message successfully forwarded to the main destination will also be sent to the "Saved Messages" chat of each account.

**Implications:**
*   **Increased API Usage:** This feature will make significantly more API calls (one forward per account for each original message). Be mindful of Telegram's rate limits. The system has built-in handling for `FloodWaitError` (rate limit exceeded) and will pause as instructed by Telegram, but excessive use could still lead to temporary restrictions on accounts.
*   **Data Redundancy:** You will have multiple copies of the forwarded messages across your accounts.
*   **Sequential Operation:** Forwarding to each account's "Saved Messages" happens sequentially for each original message to manage client connections and reduce simultaneous API load from this specific feature.

### Database and `account_channel_access` Table

The "Total Forward Mode" (`--total-mode`) relies on the `account_channel_access` table in the SPECTRA database. This table stores a record of which channels are accessible by which of your configured accounts, including their names and access hashes. It is populated by the `tgarchive channels --update-access` command.

For more details on the database schema, please refer to the [DATABASE_SCHEMA.md](DATABASE_SCHEMA.md) file.


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
