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

# Make sure the chaos script is executable
chmod +x chaos.py

# Start the chaos script in the background with no-chaos mode and 30 seconds runtime
echo "Starting chaos script in NO-CHAOS mode for 30 seconds..."
./chaos.py --no-chaos --run-time 30 &
CHAOS_PID=$!

# Set the environment variable to use the external chaos script
export USE_EXTERNAL_CHAOS=true

# Run the Python demo
echo "Starting Pulsar demo application with NO-CHAOS mode..."
python3.9 pulsar_demo.py

# When the demo exits, kill the chaos script if it's still running
echo "Demo application closed. Checking if chaos script is still running..."
if ps -p $CHAOS_PID > /dev/null; then
    echo "Stopping chaos script..."
    kill $CHAOS_PID
else
    echo "Chaos script already completed."
fi

# Make sure all clusters are running before exiting
echo "Ensuring all clusters are running..."
for cluster in alpha beta gamma; do
    if ! docker ps | grep -q "$cluster"; then
        echo "Starting $cluster cluster..."
        docker start $cluster
    fi
done

echo "No-chaos demo completed."