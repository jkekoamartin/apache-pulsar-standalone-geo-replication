#!/bin/bash

# Check if Pulsar clusters are running
if ! docker ps | grep -q "alpha"; then
    echo "Pulsar clusters are not running. Starting them now..."
    ./deploy.sh
    echo "Waiting for clusters to be fully ready..."
    sleep 10
else
    echo "Pulsar clusters are already running."
fi

# Check if virtual environment exists
if [ ! -d "pulsar_venv" ]; then
    echo "Creating Python virtual environment..."
    python3.9 -m venv pulsar_venv
    echo "Installing Python dependencies in virtual environment..."
    source pulsar_venv/bin/activate
    pip install -r requirements.txt
else
    echo "Using existing virtual environment."
    source pulsar_venv/bin/activate
fi

# Run the Python debug demo
echo "Starting Pulsar debug demo application..."
python3.9 pulsar_demo_debug.py

# Note: The script will end when the Python application is closed
echo "Debug demo application closed."
