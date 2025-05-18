"""setup.py for the SPECTRA tool-chain.
Builds and distributes spectra_002 / 003 / 004 as a single installable
Python package (`spectra-archive`).
"""
from __future__ import annotations

from pathlib import Path
from setuptools import setup, find_packages

# ---------------------------------------------------------------------
# Metadata collected from the core orchestrator (single source of truth)
# ---------------------------------------------------------------------
METADATA: dict = {}
root = Path(__file__).parent
metadata_file = root / "spectra_003_main.py"

# fallback version
version = "0.0.0"

if metadata_file.exists():
    # exec only __version__ to avoid heavy imports
    for line in metadata_file.read_text().splitlines():
        if line.startswith("__version__"):
            exec(line, METADATA)
            break
    version = METADATA.get("__version__", version)

# ---------------------------------------------------------------------
# Helper for requirements
# ---------------------------------------------------------------------

def list_requirements() -> list[str]:
    req_file = root / "requirements.txt"
    if req_file.exists():
        return [r.strip() for r in req_file.read_text().splitlines() if r.strip() and not r.startswith("#")]
    # inline fallback
    return [
        "telethon>=1.34",
        "rich>=13",
        "tqdm>=4",
        "pyyaml>=6",
        "Pillow>=10",
        "npyscreen>=4.10",
        "jinja2>=3",
    ]

# ---------------------------------------------------------------------
# Long description from README
# ---------------------------------------------------------------------
long_description = (root / "README.md").read_text() if (root / "README.md").exists() else "SPECTRA archive toolkit."

# ---------------------------------------------------------------------
# Setup call
# ---------------------------------------------------------------------
setup(
    name="spectra-archive",
    version=version,
    description="Telegram archiving & static-site toolkit (SPECTRA)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="John (SWORD-EPI)",
    author_email="n/a",
    url="https://github.com/SWORDIntel/SPECTRA002",
    packages=find_packages(include=["spectra_*"]),
    install_requires=list_requirements(),
    include_package_data=True,
    license="MIT",
    python_requires=">=3.10",
    entry_points={
        "console_scripts": [
            "spectra-orchestrator = spectra_003_main:main",
            "spectra-archiver   = spectra_002_archiver:main",
            "spectra-site-build = build_site:build_site",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Topic :: Communications :: Chat",
        "Topic :: Internet :: WWW/HTTP :: Site Management",
        "Environment :: Console",
    ],
)
