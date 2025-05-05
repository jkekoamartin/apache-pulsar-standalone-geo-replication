# Simple Pulsar Demo

This is a simple demo for Apache Pulsar that demonstrates message production and consumption across three clusters (alpha, beta, and gamma).

## Features

- Writes unique messages from alpha, beta, and gamma producers to respective brokers
- Consumes from corresponding consumers
- Runs for a configurable amount of time
- Logs inputs and outputs per alpha, beta, and gamma for reference after the run

## Requirements

- Docker and Docker Compose (for running the Pulsar clusters)
- Python 3.9 or higher
- Required Python packages (installed automatically by the script):
  - pulsar-client
  - termcolor

## How to Run

1. Make sure you have Docker and Docker Compose installed.

2. Run the demo script:

   ```bash
   ./run_simple_demo.sh [run_time]
   ```

   Where `[run_time]` is an optional parameter specifying the duration of the demo in seconds (default: 30).

   For example, to run the demo for 60 seconds:

   ```bash
   ./run_simple_demo.sh 60
   ```

3. The script will:
   - Check if Pulsar clusters are running and start them if needed
   - Set up a Python virtual environment if it doesn't exist
   - Run the demo for the specified duration
   - Print a summary of all messages sent and received

## Output

During the run, the demo will log:
- Messages sent by each producer
- Messages received by each consumer

After the run completes, a summary will be displayed showing:
- Total messages sent and received for each cluster
- Details of each message sent and received

## How It Works

1. The demo creates three producers, one for each Pulsar cluster (alpha, beta, gamma).
2. Each producer sends unique messages to its respective cluster.
3. Three consumers are created, one for each cluster.
4. Each consumer receives messages from its respective cluster.
5. All messages are logged and summarized at the end of the run.

## Troubleshooting

If you encounter any issues:

1. Make sure Docker is running.
2. Check if the Pulsar clusters are running with `docker ps`.
3. If the clusters are not running, you can start them manually with `./deploy.sh`.
4. Check the Python virtual environment at `pulsar_venv` and ensure dependencies are installed.