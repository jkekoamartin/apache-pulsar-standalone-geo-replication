#!/bin/bash

# Start the Docker containers
echo "Starting Pulsar clusters with Docker Compose..."
docker-compose up -d

# Wait for the containers to be ready
echo "Waiting for containers to be ready..."
sleep 10

# Check if containers are running
echo "Checking container status..."
docker-compose ps

# Source the Docker aliases
echo "Setting up aliases..."
source alias.sh

# Configure geo replication
echo "Configuring geo replication..."
./configure.sh

echo "Deployment complete! You can now test geo replication."
echo "To test, open three shells and run:"
echo "  source alias.sh"
echo "  alpha-client consume -n 10 -s hello -p Earliest acme/test/hello"
echo "  beta-client consume -n 10 -s hello -p Earliest acme/test/hello"
echo "  gamma-client consume -n 10 -s hello -p Earliest acme/test/hello"
echo "Then in a fourth shell, run:"
echo "  source alias.sh"
echo "  alpha-client produce -n 10 -m hello acme/test/hello"
echo "To clean up, run:"
echo "  docker-compose down -v"
