#!/usr/bin/env python
from codecs import open
from setuptools import setup

from tgarchive import __version__

README = open("README.md").read()

def requirements():
    with open('requirements.txt') as f:
        return f.read().splitlines()



setup(
    name="tg-archive",
    version=__version__,
    description="is a tool for exporting Telegram group chats into static websites, preserving the chat history like mailing list archives.",
    long_description=README,
    long_description_content_type="text/markdown",
    author="Kailash Nadh",
    author_email="kailash@nadh.in",
    url="https://github.com/knadh/tg-archive",
    packages=['tgarchive'],
    install_requires=requirements(),
    include_package_data=True,
    download_url="https://github.com/knadh/tg-archive",
    license="MIT License",
    entry_points={
        'console_scripts': [
            'tg-archive = tgarchive:main'
        ],
    },
    classifiers=[
        "Topic :: Communications :: Chat",
        "Topic :: Internet :: WWW/HTTP :: Site Management",
        "Topic :: Documentation"
    ],
)
