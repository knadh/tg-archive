#!/bin/bash
pushd /session

while true; do
    #sleep infinity
    python3 /app/index.py
    echo "Restarting in 30 seconds..."
    sleep 30
done