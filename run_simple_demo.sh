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

# Parse command line arguments
RUN_TIME=30  # Default run time is 30 seconds

# Check if run time is provided
if [ "$#" -ge 1 ]; then
    if [[ "$1" =~ ^[0-9]+$ ]]; then
        RUN_TIME=$1
        echo "Setting run time to $RUN_TIME seconds."
    else
        echo "Invalid run time: $1. Using default of 30 seconds."
    fi
fi

# Run the Simple Pulsar Demo
echo "Starting Simple Pulsar Demo..."
python3.9 simple_pulsar_demo.py --run-time $RUN_TIME

# Note: The script will end when the Python application is closed
echo "Demo application closed."