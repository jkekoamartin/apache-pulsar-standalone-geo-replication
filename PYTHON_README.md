# Pulsar Multi-Region Demo Application

This Python application demonstrates Apache Pulsar's geo-replication capabilities with a focus on:

1. Producer failover from alpha to beta cluster
2. Shared subscription usage
3. Duplicate and out-of-order message handling
4. Live visualization of message flow

## Prerequisites

- Apache Pulsar clusters set up with geo-replication (follow the main README.md)
- Python 3.6+
- Required Python packages (install with `pip install -r requirements.txt`)

## Installation

1. Make sure the Pulsar clusters are running:
   ```
   ./deploy.sh
   ```

2. Install the required Python packages:
   ```
   pip install -r requirements.txt
   ```

## Running the Demo

1. Start the application:
   ```
   python pulsar_demo.py
   ```

2. The application will:
   - Connect a producer to the alpha cluster
   - Connect consumers to both alpha and beta clusters using a shared subscription
   - Periodically simulate alpha cluster failures to demonstrate failover
   - Show a live visualization of message flow and latency

3. Press Ctrl+C to exit the application

## Features Demonstrated

### 1. Producer Failover

The application initially connects to the alpha cluster. When a failure is detected or simulated, the producer automatically switches to the beta cluster. This demonstrates Pulsar's ability to maintain service availability during cluster outages.

### 2. Shared Subscription

The application uses a shared subscription named "shared-subscription" across both clusters. This allows multiple consumers to process messages from the same topic, with each message being delivered to only one consumer.

### 3. Duplicate and Out-of-Order Handling

The application detects and visualizes:
- Duplicate messages (which can occur during failover)
- Out-of-order messages (which can happen due to network delays or failover)

Each message contains a sequence number and timestamp to enable this detection.

## Visualization

The application provides a real-time visualization with two main components:

### Message Flow Diagram

Shows the flow of messages between:
- Producer → Alpha Cluster → Consumer
- Producer → Beta Cluster → Consumer

The status of each cluster is displayed, along with message counts and statistics.

### Latency Graph

Displays the latency of each message, with special markers for:
- Duplicate messages (red X)
- Out-of-order messages (purple star)

## Scenarios Demonstrated

1. **Normal Operation**: Messages flow from producer to alpha cluster to consumer
2. **Failover**: When alpha fails, the producer switches to beta cluster
3. **Recovery**: When alpha recovers, the producer can switch back
4. **Duplicate Detection**: During failover, some messages might be received twice
5. **Out-of-Order Detection**: Messages might arrive out of sequence during cluster transitions

## Troubleshooting

- If you encounter connection errors, ensure the Pulsar clusters are running
- Check that the correct ports are being used (alpha: 6650, beta: 6651)
- Verify that geo-replication is properly configured between clusters