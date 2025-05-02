# Pulsar Multi-Region Demo Application

This Python application demonstrates Apache Pulsar's geo-replication capabilities with a focus on:

1. Producer failover between alpha, beta, and gamma clusters
2. Shared subscription usage across all clusters
3. Duplicate and out-of-order message handling
4. Live visualization of message flow with connection status
5. End-to-end message tracking with success rate statistics
6. Interactive chaos parameter controls

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

### Standard Demo

1. Start the application:
   ```
   ./run_demo.sh
   ```
   or
   ```
   python pulsar_demo.py
   ```

2. The application will:
   - Connect separate producers to each cluster (alpha, beta, gamma)
   - Connect separate consumers to each cluster using a shared subscription
   - Periodically simulate cluster failures to demonstrate failover
   - Show a live visualization of message flow, connection status, and latency
   - Track and display end-to-end message statistics and success rate

3. Press Ctrl+C to exit the application

### Chaos Testing Demo

For a more realistic demonstration with actual cluster failures:

1. Start the chaos demo:
   ```
   ./run_chaos_demo.sh
   ```

2. This will:
   - Start the demo application with real-time cluster health monitoring
   - Run a chaos script in the background that randomly stops and starts the Pulsar clusters
   - Show health indicators for all three clusters in the visualization
   - Demonstrate how the system handles real cluster failures and recoveries
   - Allow you to adjust chaos parameters using interactive sliders
   - Track and display end-to-end message success rates during chaos events

3. The chaos script (`chaos.py`) can also be run independently with custom parameters:
   ```
   ./chaos.py --min-interval 30 --max-interval 90 --min-downtime 10 --max-downtime 30 --failure-probability 0.7
   ```

4. Press Ctrl+C to exit the application

## Features Demonstrated

### 1. Multi-Cluster Producer Failover

The application creates separate producers for each cluster (alpha, beta, gamma). When a cluster failure is detected, the producers automatically switch to available clusters in a priority order. This demonstrates Pulsar's ability to maintain service availability during cluster outages across multiple regions.

### 2. Shared Subscription Across All Clusters

The application uses a shared subscription named "shared-subscription" across all three clusters. This allows multiple consumers to process messages from the same topic, with each message being delivered to only one consumer, regardless of which cluster it comes from.

### 3. Duplicate and Out-of-Order Handling

The application detects and visualizes:
- Duplicate messages (which can occur during failover)
- Out-of-order messages (which can happen due to network delays or failover)

Each message contains a sequence number and timestamp to enable this detection.

### 4. End-to-End Message Tracking

The application tracks messages from production to consumption and calculates:
- Total messages sent
- Unique messages received
- Success rate (percentage of messages that made it through the system)
- Per-cluster message counts

This provides insights into the reliability of the system during normal operation and failure scenarios.

### 5. Interactive Chaos Controls

The application includes sliders to adjust chaos parameters in real-time:
- Failure probability: Controls how likely a cluster is to fail
- Maximum downtime: Controls how long a cluster stays down when it fails
- Minimum interval: Controls how frequently failures can occur

These controls allow you to experiment with different failure scenarios and observe how the system responds.

## Visualization

The application provides a real-time visualization with three main components:

### Message Flow Diagram

Shows the flow of messages between:
- Alpha Producer → Alpha Cluster → Alpha Consumer
- Beta Producer → Beta Cluster → Beta Consumer
- Gamma Producer → Gamma Cluster → Gamma Consumer

Each component displays its connection status (Connected/Disconnected) in real-time, allowing you to see the impact of cluster failures and recoveries.

### Latency Graph

Displays the latency of each message, with special markers for:
- Duplicate messages (red X)
- Out-of-order messages (purple star)

### Message Counters & Controls

A separate window displays:
- Cluster status (UP/DOWN) for all three clusters
- Message counts for each cluster (sent and received)
- Total unique messages and success rate
- Duplicate and out-of-order message counts
- Interactive sliders to control chaos parameters

## Scenarios Demonstrated

1. **Multi-Cluster Operation**: Messages flow from producers to their respective clusters to consumers
2. **Failover Chain**: When a cluster fails, producers and consumers automatically switch to available clusters
3. **Dynamic Recovery**: When a cluster recovers, connections are automatically re-established
4. **Duplicate Detection**: During failover, some messages might be received twice
5. **Out-of-Order Detection**: Messages might arrive out of sequence during cluster transitions
6. **Success Rate Monitoring**: Track the percentage of messages that successfully make it through the system
7. **Chaos Parameter Tuning**: Adjust failure parameters in real-time to observe different failure scenarios

## Troubleshooting

- If you encounter connection errors, ensure the Pulsar clusters are running
- Check that the correct ports are being used (alpha: 6650, beta: 6651, gamma: 6652)
- Verify that geo-replication is properly configured between all clusters
- If the visualization windows appear cluttered, try resizing them or adjusting your screen resolution
- If sliders don't seem to affect the chaos behavior immediately, give it some time as changes take effect on the next failure cycle
