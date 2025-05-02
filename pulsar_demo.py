import pulsar
import time
import random
import threading
import sys
import os
import signal
import json
from datetime import datetime
from termcolor import colored
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import matplotlib.patches as patches
import numpy as np
from collections import deque

# Global variables for visualization
messages_data = {
    'producer_to_alpha': deque(maxlen=10),
    'producer_to_beta': deque(maxlen=10),
    'alpha_to_consumer': deque(maxlen=10),
    'beta_to_consumer': deque(maxlen=10),
    'duplicates': deque(maxlen=10),
    'out_of_order': deque(maxlen=10)
}

# Track received message IDs to detect duplicates
received_messages = set()
# Track message sequence to detect out-of-order
last_sequence = -1
# Track message latencies
latencies = []

# Flag to control the application
running = True
# Flag to simulate alpha cluster failure
alpha_failed = False

def signal_handler(sig, frame):
    global running
    print(colored("\nShutting down...", "yellow"))
    running = False
    plt.close('all')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def log_message(source, message, color="white"):
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(colored(f"[{timestamp}] [{source}] {message}", color))

def update_visualization_data(source, target, message_id, timestamp=None):
    if timestamp is None:
        timestamp = time.time()

    key = f"{source}_to_{target}"
    if key in messages_data:
        messages_data[key].append({
            'id': message_id,
            'timestamp': timestamp
        })

def detect_duplicate(message_id):
    if message_id in received_messages:
        log_message("DUPLICATE", f"Detected duplicate message: {message_id}", "red")
        messages_data['duplicates'].append({
            'id': message_id,
            'timestamp': time.time()
        })
        return True
    received_messages.add(message_id)
    return False

def detect_out_of_order(sequence):
    global last_sequence
    if last_sequence != -1 and sequence < last_sequence:
        log_message("OUT-OF-ORDER", f"Detected out-of-order message: expected > {last_sequence}, got {sequence}", "red")
        messages_data['out_of_order'].append({
            'id': sequence,
            'timestamp': time.time()
        })
        return True
    last_sequence = sequence
    return False

def producer_task():
    global alpha_failed

    try:
        # Connect to alpha cluster
        log_message("PRODUCER", "Connecting to alpha cluster...", "cyan")
        client = pulsar.Client('pulsar://localhost:6650')
        producer = client.create_producer(
            'persistent://acme/test/demo',
            properties={
                "producer-name": "demo-producer",
                "producer-id": "1"
            }
        )
        log_message("PRODUCER", "Connected to alpha cluster", "green")
    except Exception as e:
        log_message("PRODUCER", f"Failed to connect to alpha cluster: {str(e)}", "red")
        alpha_failed = True
        return

    sequence = 0
    while running:
        try:
            if alpha_failed:
                # Switch to beta cluster if alpha fails
                log_message("PRODUCER", "Alpha cluster failed, switching to beta cluster", "yellow")
                try:
                    client.close()
                    client = pulsar.Client('pulsar://localhost:6651')
                    producer = client.create_producer(
                        'persistent://acme/test/demo',
                        properties={
                            "producer-name": "demo-producer",
                            "producer-id": "1"
                        }
                    )
                    log_message("PRODUCER", "Connected to beta cluster", "green")
                    # Don't reset the flag here, let the failure simulator handle it
                except Exception as e:
                    log_message("PRODUCER", f"Failed to connect to beta cluster: {str(e)}", "red")
                    time.sleep(1)
                    continue

            # Create message with sequence number and timestamp
            message_data = {
                'id': f"msg-{sequence}",
                'sequence': sequence,
                'timestamp': time.time(),
                'content': f"Message content {sequence}"
            }

            # Serialize message to JSON
            message_json = json.dumps(message_data)

            # Send the message
            producer.send(message_json.encode('utf-8'))

            # Update visualization data
            if not alpha_failed:
                update_visualization_data('producer', 'alpha', message_data['id'])
                log_message("PRODUCER", f"Sent message to alpha: {message_data['id']}", "cyan")
            else:
                update_visualization_data('producer', 'beta', message_data['id'])
                log_message("PRODUCER", f"Sent message to beta: {message_data['id']}", "cyan")

            sequence += 1
            time.sleep(1)  # Send a message every second

        except Exception as e:
            log_message("PRODUCER", f"Error: {str(e)}", "red")
            # If we get an error with alpha, switch to beta
            if not alpha_failed:
                alpha_failed = True
            time.sleep(1)

    # Clean up
    try:
        producer.close()
        client.close()
    except Exception as e:
        log_message("PRODUCER", f"Error during cleanup: {str(e)}", "red")

    log_message("PRODUCER", "Producer task completed", "green")

def consumer_task():
    try:
        # Connect to both clusters for failover
        log_message("CONSUMER", "Connecting to alpha and beta clusters...", "cyan")
        client_alpha = pulsar.Client('pulsar://localhost:6650')
        client_beta = pulsar.Client('pulsar://localhost:6651')

        # Create a shared subscription consumer on alpha
        consumer_alpha = client_alpha.subscribe(
            'persistent://acme/test/demo',
            'shared-subscription',
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest
        )

        # Create a shared subscription consumer on beta
        consumer_beta = client_beta.subscribe(
            'persistent://acme/test/demo',
            'shared-subscription',
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest
        )

        log_message("CONSUMER", "Connected to alpha and beta clusters with shared subscription", "green")
    except Exception as e:
        log_message("CONSUMER", f"Failed to connect to clusters: {str(e)}", "red")
        return

    while running:
        try:
            # Try to receive from alpha
            if not alpha_failed:
                try:
                    msg_alpha = consumer_alpha.receive(timeout_millis=500)
                    process_message(msg_alpha, consumer_alpha, 'alpha')
                except Exception as e:
                    if "Timeout" not in str(e):
                        log_message("CONSUMER", f"Alpha error: {str(e)}", "red")

            # Try to receive from beta
            try:
                msg_beta = consumer_beta.receive(timeout_millis=500)
                process_message(msg_beta, consumer_beta, 'beta')
            except Exception as e:
                if "Timeout" not in str(e):
                    log_message("CONSUMER", f"Beta error: {str(e)}", "red")

        except Exception as e:
            log_message("CONSUMER", f"Error: {str(e)}", "red")
            time.sleep(1)

    # Clean up
    try:
        consumer_alpha.close()
        consumer_beta.close()
        client_alpha.close()
        client_beta.close()
    except Exception as e:
        log_message("CONSUMER", f"Error during cleanup: {str(e)}", "red")

    log_message("CONSUMER", "Consumer task completed", "green")
    log_message("SUMMARY", f"Received {len(received_messages)} unique messages", "yellow")

def process_message(msg, consumer, source):
    try:
        # Decode and parse the message
        message_json = msg.data().decode('utf-8')
        message_data = json.loads(message_json)

        # Extract message details
        message_id = message_data['id']
        sequence = message_data['sequence']
        send_time = message_data['timestamp']

        # Calculate latency
        receive_time = time.time()
        latency = (receive_time - send_time) * 1000  # Convert to ms
        latencies.append(latency)

        # Check for duplicates and out-of-order messages
        is_duplicate = detect_duplicate(message_id)
        is_out_of_order = detect_out_of_order(sequence)

        # Update visualization data
        update_visualization_data(source, 'consumer', message_id, receive_time)

        # Log the message
        status = ""
        if is_duplicate:
            status = " (DUPLICATE)"
        if is_out_of_order:
            status += " (OUT-OF-ORDER)"

        log_message("CONSUMER", f"Received from {source}: {message_id}{status} - Latency: {latency:.2f}ms", "green")

        # Acknowledge the message
        consumer.acknowledge(msg)

    except Exception as e:
        log_message("CONSUMER", f"Error processing message: {str(e)}", "red")
        # Negative acknowledge to have the message redelivered later
        consumer.negative_acknowledge(msg)

def simulate_failures():
    global alpha_failed
    while running:
        # Wait for a random time between 15-30 seconds
        sleep_time = random.randint(15, 30)
        time.sleep(sleep_time)

        if random.random() < 0.7:  # 70% chance of failure
            log_message("SIMULATOR", "Simulating alpha cluster failure", "yellow")
            alpha_failed = True

            # Keep alpha down for 5-10 seconds
            failure_duration = random.randint(5, 10)
            time.sleep(failure_duration)

            log_message("SIMULATOR", "Alpha cluster recovered", "green")
            alpha_failed = False

def create_visualization():
    # Create figure and subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
    fig.suptitle('Pulsar Message Flow Visualization', fontsize=16)

    # Create rectangles for clusters and components
    producer_rect = patches.Rectangle((0.1, 0.6), 0.2, 0.3, linewidth=2, edgecolor='blue', facecolor='lightblue', label='Producer')
    alpha_rect = patches.Rectangle((0.5, 0.7), 0.2, 0.2, linewidth=2, edgecolor='green', facecolor='lightgreen', label='Alpha Cluster')
    beta_rect = patches.Rectangle((0.5, 0.4), 0.2, 0.2, linewidth=2, edgecolor='orange', facecolor='lightsalmon', label='Beta Cluster')
    consumer_rect = patches.Rectangle((0.8, 0.6), 0.2, 0.3, linewidth=2, edgecolor='purple', facecolor='plum', label='Consumer')

    # Add rectangles to the plot
    ax1.add_patch(producer_rect)
    ax1.add_patch(alpha_rect)
    ax1.add_patch(beta_rect)
    ax1.add_patch(consumer_rect)

    # Add labels
    ax1.text(0.2, 0.75, 'Producer', ha='center')
    ax1.text(0.6, 0.8, 'Alpha', ha='center')
    ax1.text(0.6, 0.5, 'Beta', ha='center')
    ax1.text(0.9, 0.75, 'Consumer', ha='center')

    # Set axis limits and remove ticks
    ax1.set_xlim(0, 1.1)
    ax1.set_ylim(0.3, 1)
    ax1.set_xticks([])
    ax1.set_yticks([])
    ax1.set_title('Message Flow')

    # Create empty line objects for messages
    producer_to_alpha_line, = ax1.plot([], [], 'g-', lw=2, alpha=0.7)
    producer_to_beta_line, = ax1.plot([], [], 'y-', lw=2, alpha=0.7)
    alpha_to_consumer_line, = ax1.plot([], [], 'g-', lw=2, alpha=0.7)
    beta_to_consumer_line, = ax1.plot([], [], 'y-', lw=2, alpha=0.7)

    # Create empty scatter objects for messages
    producer_to_alpha_msgs = ax1.scatter([], [], c='green', s=50, alpha=0.7)
    producer_to_beta_msgs = ax1.scatter([], [], c='orange', s=50, alpha=0.7)
    alpha_to_consumer_msgs = ax1.scatter([], [], c='green', s=50, alpha=0.7)
    beta_to_consumer_msgs = ax1.scatter([], [], c='orange', s=50, alpha=0.7)

    # Create a line for latency plot
    latency_line, = ax2.plot([], [], 'b-', lw=2)
    duplicate_markers = ax2.scatter([], [], c='red', s=50, marker='x', label='Duplicate')
    out_of_order_markers = ax2.scatter([], [], c='purple', s=50, marker='*', label='Out of Order')

    # Set up the latency plot
    ax2.set_xlim(0, 100)
    ax2.set_ylim(0, 500)
    ax2.set_xlabel('Message Number')
    ax2.set_ylabel('Latency (ms)')
    ax2.set_title('Message Latency')
    ax2.grid(True)
    ax2.legend()

    # Animation update function
    def update(frame):
        # Update message flow visualization

        # Define the fixed positions for the components
        producer_pos = (0.2, 0.75)
        alpha_pos = (0.6, 0.8)
        beta_pos = (0.6, 0.5)
        consumer_pos = (0.9, 0.75)

        # Set the fixed lines for the message paths
        producer_to_alpha_line.set_data([producer_pos[0], alpha_pos[0]], [producer_pos[1], alpha_pos[1]])
        producer_to_beta_line.set_data([producer_pos[0], beta_pos[0]], [producer_pos[1], beta_pos[1]])
        alpha_to_consumer_line.set_data([alpha_pos[0], consumer_pos[0]], [alpha_pos[1], consumer_pos[1]])
        beta_to_consumer_line.set_data([beta_pos[0], consumer_pos[0]], [beta_pos[1], consumer_pos[1]])

        # Animate messages along the paths
        # This is simplified - in a real implementation, you'd animate the messages moving along the paths

        # Update latency plot
        if latencies:
            x = list(range(len(latencies)))
            latency_line.set_data(x, latencies)
            ax2.set_xlim(0, max(100, len(latencies)))
            ax2.set_ylim(0, max(500, max(latencies) * 1.1))

        # Update duplicate and out-of-order markers
        duplicate_x = []
        duplicate_y = []
        for i, msg_id in enumerate([msg['id'] for msg in messages_data['duplicates']]):
            if msg_id.startswith('msg-'):
                try:
                    msg_num = int(msg_id.split('-')[1])
                    if msg_num < len(latencies):
                        duplicate_x.append(msg_num)
                        duplicate_y.append(latencies[msg_num])
                except:
                    pass

        out_of_order_x = []
        out_of_order_y = []
        for i, msg_id in enumerate([msg['id'] for msg in messages_data['out_of_order']]):
            if msg_id < len(latencies):
                out_of_order_x.append(msg_id)
                out_of_order_y.append(latencies[msg_id])

        duplicate_markers.set_offsets(np.column_stack([duplicate_x, duplicate_y]) if duplicate_x else np.empty((0, 2)))
        out_of_order_markers.set_offsets(np.column_stack([out_of_order_x, out_of_order_y]) if out_of_order_x else np.empty((0, 2)))

        # Update status text
        status_text = f"Alpha: {'DOWN' if alpha_failed else 'UP'} | "
        status_text += f"Messages: {len(received_messages)} | "
        status_text += f"Duplicates: {len(messages_data['duplicates'])} | "
        status_text += f"Out-of-order: {len(messages_data['out_of_order'])}"

        # Remove old text and add new
        for txt in ax1.texts:
            if txt.get_position()[1] < 0.4:  # Only remove the status text
                txt.remove()

        ax1.text(0.5, 0.35, status_text, ha='center', fontsize=10, 
                 bbox=dict(facecolor='white', alpha=0.5))

        return (producer_to_alpha_line, producer_to_beta_line, alpha_to_consumer_line, beta_to_consumer_line,
                producer_to_alpha_msgs, producer_to_beta_msgs, alpha_to_consumer_msgs, beta_to_consumer_msgs,
                latency_line, duplicate_markers, out_of_order_markers)

    # Create animation
    ani = FuncAnimation(fig, update, frames=range(1000), interval=500, blit=True)
    plt.tight_layout()
    plt.show()

def main():
    print(colored("Starting Pulsar Demo Application", "cyan"))
    print(colored("Press Ctrl+C to exit", "yellow"))

    # Start the consumer thread
    consumer_thread = threading.Thread(target=consumer_task)
    consumer_thread.daemon = True
    consumer_thread.start()

    # Start the producer thread
    producer_thread = threading.Thread(target=producer_task)
    producer_thread.daemon = True
    producer_thread.start()

    # Start the failure simulator thread
    failure_thread = threading.Thread(target=simulate_failures)
    failure_thread.daemon = True
    failure_thread.start()

    # Start the visualization
    create_visualization()

    # Wait for threads to complete
    consumer_thread.join()
    producer_thread.join()
    failure_thread.join()

if __name__ == "__main__":
    main()
