#!/bin/bash
# SPECTRA Integrated Installer
# Sets up both SPECTRA and telegram-groups-crawler with all dependencies

set -e  # Exit on error

BLUE='\033[0;34m'
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}=================================================${NC}"
echo -e "${BLUE}  SPECTRA — Telegram Network Discovery & Archiving${NC}"
echo -e "${BLUE}  Integrated Installation Script${NC}"
echo -e "${BLUE}=================================================${NC}"

# Check Python version
echo -e "\n${YELLOW}Checking Python version...${NC}"
python_version=$(python3 --version 2>&1 | awk '{print $2}')
python_major=$(echo $python_version | cut -d. -f1)
python_minor=$(echo $python_version | cut -d. -f2)

if [ "$python_major" -lt 3 ] || ([ "$python_major" -eq 3 ] && [ "$python_minor" -lt 10 ]); then
    echo -e "${RED}Error: Python 3.10+ is required (found $python_version)${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Python $python_version detected${NC}"

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo -e "\n${YELLOW}Creating Python virtual environment...${NC}"
    python3 -m venv .venv
    echo -e "${GREEN}✓ Virtual environment created${NC}"
else
    echo -e "\n${YELLOW}Using existing virtual environment${NC}"
fi

# Activate virtual environment
echo -e "\n${YELLOW}Activating virtual environment...${NC}"
source .venv/bin/activate
echo -e "${GREEN}✓ Virtual environment activated${NC}"

# Update pip
echo -e "\n${YELLOW}Updating pip...${NC}"
pip install --upgrade pip
echo -e "${GREEN}✓ Pip updated${NC}"

# Install SPECTRA
echo -e "\n${YELLOW}Installing SPECTRA...${NC}"
pip install -e .
echo -e "${GREEN}✓ SPECTRA installed${NC}"

# Clone telegram-groups-crawler if it doesn't exist
if [ ! -d "telegram-groups-crawler" ]; then
    echo -e "\n${YELLOW}Cloning telegram-groups-crawler repository...${NC}"
    git clone https://github.com/zehrakocairi/telegram-groups-crawler.git
    echo -e "${GREEN}✓ telegram-groups-crawler cloned${NC}"
else
    echo -e "\n${YELLOW}Updating telegram-groups-crawler repository...${NC}"
    (cd telegram-groups-crawler && git pull)
    echo -e "${GREEN}✓ telegram-groups-crawler updated${NC}"
fi

# Check if credentials are set
echo -e "\n${YELLOW}Checking configuration...${NC}"

# Create data directories
echo -e "\n${YELLOW}Creating data directories...${NC}"
mkdir -p spectra_data/media
mkdir -p logs
echo -e "${GREEN}✓ Data directories created${NC}"

# Check if config file exists 
if [ ! -f "spectra_config.json" ]; then
    echo -e "${YELLOW}No configuration file found. Running SPECTRA to generate default config...${NC}"
    python -m tgarchive.sync --no-tui || true
    echo -e "${GREEN}✓ Default configuration created${NC}"
    
    echo -e "${YELLOW}Please edit spectra_config.json with your Telegram API credentials${NC}"
    echo -e "${YELLOW}You can obtain API credentials from https://my.telegram.org/apps${NC}"
fi

# Modify telegram-groups-crawler script if needed
if [ -f "telegram-groups-crawler/scraper.py" ]; then
    echo -e "\n${YELLOW}Checking telegram-groups-crawler script...${NC}"
    if grep -q "INSERT YOUR API ID" "telegram-groups-crawler/scraper.py"; then
        echo -e "${YELLOW}telegram-groups-crawler needs API credentials${NC}"
        echo -e "${YELLOW}Please edit telegram-groups-crawler/scraper.py with your credentials${NC}"
    fi
fi

# Installation complete
echo -e "\n${GREEN}=================================================${NC}"
echo -e "${GREEN}  SPECTRA Installation Complete!${NC}"
echo -e "${GREEN}=================================================${NC}"
echo -e "\n${BLUE}To start SPECTRA TUI:${NC}"
echo -e "  source .venv/bin/activate"
echo -e "  spectra"
echo -e "\n${BLUE}To use command-line tools:${NC}"
echo -e "  spectra discover --seed @channelname --depth 2"
echo -e "  spectra network --crawler-dir telegram-groups-crawler --plot"
echo -e "  spectra archive --entity @channelname --auto"
echo -e "  spectra batch --file groups.txt --limit 10"
echo -e "\n${YELLOW}Note: Make sure to edit the configuration files with your Telegram API credentials${NC}"
echo -e "${YELLOW}before using the tools.${NC}" 