import pulsar
import time
import random
import threading
import sys
import os
import signal
import json
import subprocess
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
    'producer_to_gamma': deque(maxlen=10),
    'alpha_to_consumer': deque(maxlen=10),
    'beta_to_consumer': deque(maxlen=10),
    'gamma_to_consumer': deque(maxlen=10),
    'duplicates': deque(maxlen=10),
    'out_of_order': deque(maxlen=10)
}

# Track message counts for each cluster
message_counts = {
    'alpha_sent': 0,
    'beta_sent': 0,
    'gamma_sent': 0,
    'alpha_received': 0,
    'beta_received': 0,
    'gamma_received': 0,
    'unique_total': 0,
    'end_to_end_success': 0,
    'total_sent': 0
}

# Track producers and consumers
producers = {
    'alpha': None,
    'beta': None,
    'gamma': None
}

producer_clients = {
    'alpha': None,
    'beta': None,
    'gamma': None
}

consumers = {
    'alpha': None,
    'beta': None,
    'gamma': None
}

consumer_clients = {
    'alpha': None,
    'beta': None,
    'gamma': None
}

# Track received message IDs to detect duplicates
received_messages = set()
# Track message sequence to detect out-of-order
last_sequence = -1
# Track message latencies
latencies = []

# Flag to control the application
running = True
# Flags to track cluster status
alpha_failed = False
beta_failed = False
gamma_failed = False

# Chaos parameters
chaos_params = {
    'failure_probability': 0.7,
    'min_downtime': 10,
    'max_downtime': 30,
    'min_interval': 30,
    'max_interval': 90
}

def signal_handler(sig, frame):
    global running
    print(colored("\nShutting down...", "yellow"))
    running = False
    plt.close('all')  # Close all matplotlib windows
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

def connect_producer(cluster_name):
    """Connect to a specific cluster and create a producer"""
    global producers, producer_clients

    port_map = {
        'alpha': 6650,
        'beta': 6651,
        'gamma': 6652
    }

    try:
        # Connect to the cluster
        log_message("PRODUCER", f"Connecting to {cluster_name} cluster...", "cyan")
        client = pulsar.Client(f'pulsar://localhost:{port_map[cluster_name]}')
        producer = client.create_producer(
            'persistent://acme/test/demo',
            properties={
                "producer-name": f"{cluster_name}-producer",
                "producer-id": f"{cluster_name}"
            }
        )

        # Store the client and producer
        producer_clients[cluster_name] = client
        producers[cluster_name] = producer

        log_message("PRODUCER", f"Connected to {cluster_name} cluster", "green")
        return True
    except Exception as e:
        log_message("PRODUCER", f"Failed to connect to {cluster_name} cluster: {str(e)}", "red")
        return False

def producer_task():
    global alpha_failed, beta_failed, gamma_failed

    # Connect to all clusters initially
    alpha_connected = connect_producer('alpha')
    beta_connected = connect_producer('beta')
    gamma_connected = connect_producer('gamma')

    # If we couldn't connect to any cluster, exit
    if not (alpha_connected or beta_connected or gamma_connected):
        log_message("PRODUCER", "Failed to connect to any cluster, exiting", "red")
        return

    sequence = 0
    while running:
        try:
            # Determine which cluster to use
            active_cluster = None

            # Try alpha first if it's not failed
            if not alpha_failed and producers['alpha'] is not None:
                active_cluster = 'alpha'
            # Try beta if alpha failed or beta if it's not failed
            elif not beta_failed and producers['beta'] is not None:
                active_cluster = 'beta'
            # Try gamma if both alpha and beta failed
            elif not gamma_failed and producers['gamma'] is not None:
                active_cluster = 'gamma'
            # If all clusters failed, try to reconnect to any available cluster
            else:
                if not alpha_failed and connect_producer('alpha'):
                    active_cluster = 'alpha'
                elif not beta_failed and connect_producer('beta'):
                    active_cluster = 'beta'
                elif not gamma_failed and connect_producer('gamma'):
                    active_cluster = 'gamma'
                else:
                    log_message("PRODUCER", "All clusters are down, waiting...", "red")
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

            # Send the message to the active cluster
            producers[active_cluster].send(message_json.encode('utf-8'))

            # Update visualization data
            message_counts['total_sent'] += 1
            message_counts[f'{active_cluster}_sent'] += 1
            update_visualization_data('producer', active_cluster, message_data['id'])
            log_message("PRODUCER", f"Sent message to {active_cluster}: {message_data['id']}", "cyan")

            sequence += 1
            time.sleep(1)  # Send a message every second

        except Exception as e:
            log_message("PRODUCER", f"Error: {str(e)}", "red")

            # If we get an error with the active cluster, mark it as failed
            if active_cluster == 'alpha':
                alpha_failed = True
            elif active_cluster == 'beta':
                beta_failed = True
            elif active_cluster == 'gamma':
                gamma_failed = True

            # Close the failed producer and client
            try:
                if producers[active_cluster]:
                    producers[active_cluster].close()
                    producers[active_cluster] = None
                if producer_clients[active_cluster]:
                    producer_clients[active_cluster].close()
                    producer_clients[active_cluster] = None
            except Exception as cleanup_error:
                log_message("PRODUCER", f"Error during cleanup: {str(cleanup_error)}", "red")

            time.sleep(1)

    # Clean up all producers and clients
    for cluster in ['alpha', 'beta', 'gamma']:
        try:
            if producers[cluster]:
                producers[cluster].close()
                producers[cluster] = None
            if producer_clients[cluster]:
                producer_clients[cluster].close()
                producer_clients[cluster] = None
        except Exception as e:
            log_message("PRODUCER", f"Error during cleanup of {cluster}: {str(e)}", "red")

    log_message("PRODUCER", "Producer task completed", "green")

def connect_consumer(cluster_name):
    """Connect to a specific cluster and create a consumer"""
    global consumers, consumer_clients

    port_map = {
        'alpha': 6650,
        'beta': 6651,
        'gamma': 6652
    }

    try:
        # Connect to the cluster
        log_message("CONSUMER", f"Connecting to {cluster_name} cluster...", "cyan")
        client = pulsar.Client(f'pulsar://localhost:{port_map[cluster_name]}')
        consumer = client.subscribe(
            'persistent://acme/test/demo',
            'shared-subscription',
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest
        )

        # Store the client and consumer
        consumer_clients[cluster_name] = client
        consumers[cluster_name] = consumer

        log_message("CONSUMER", f"Connected to {cluster_name} cluster with shared subscription", "green")
        return True
    except Exception as e:
        log_message("CONSUMER", f"Failed to connect to {cluster_name} cluster: {str(e)}", "red")
        return False

def consumer_task():
    global alpha_failed, beta_failed, gamma_failed

    # Connect to all clusters initially
    alpha_connected = connect_consumer('alpha')
    beta_connected = connect_consumer('beta')
    gamma_connected = connect_consumer('gamma')

    # If we couldn't connect to any cluster, exit
    if not (alpha_connected or beta_connected or gamma_connected):
        log_message("CONSUMER", "Failed to connect to any cluster, exiting", "red")
        return

    log_message("CONSUMER", "Connected to clusters with shared subscription", "green")

    while running:
        try:
            # Try to receive from alpha
            if not alpha_failed and consumers['alpha'] is not None:
                try:
                    msg_alpha = consumers['alpha'].receive(timeout_millis=500)
                    process_message(msg_alpha, consumers['alpha'], 'alpha')
                except Exception as e:
                    if "Timeout" not in str(e):
                        log_message("CONSUMER", f"Alpha error: {str(e)}", "red")
                        # If we get a non-timeout error, try to reconnect
                        try:
                            if consumers['alpha']:
                                consumers['alpha'].close()
                            if consumer_clients['alpha']:
                                consumer_clients['alpha'].close()
                        except:
                            pass
                        consumers['alpha'] = None
                        consumer_clients['alpha'] = None
                        alpha_failed = True

            # Try to receive from beta
            if not beta_failed and consumers['beta'] is not None:
                try:
                    msg_beta = consumers['beta'].receive(timeout_millis=500)
                    process_message(msg_beta, consumers['beta'], 'beta')
                except Exception as e:
                    if "Timeout" not in str(e):
                        log_message("CONSUMER", f"Beta error: {str(e)}", "red")
                        # If we get a non-timeout error, try to reconnect
                        try:
                            if consumers['beta']:
                                consumers['beta'].close()
                            if consumer_clients['beta']:
                                consumer_clients['beta'].close()
                        except:
                            pass
                        consumers['beta'] = None
                        consumer_clients['beta'] = None
                        beta_failed = True

            # Try to receive from gamma
            if not gamma_failed and consumers['gamma'] is not None:
                try:
                    msg_gamma = consumers['gamma'].receive(timeout_millis=500)
                    process_message(msg_gamma, consumers['gamma'], 'gamma')
                except Exception as e:
                    if "Timeout" not in str(e):
                        log_message("CONSUMER", f"Gamma error: {str(e)}", "red")
                        # If we get a non-timeout error, try to reconnect
                        try:
                            if consumers['gamma']:
                                consumers['gamma'].close()
                            if consumer_clients['gamma']:
                                consumer_clients['gamma'].close()
                        except:
                            pass
                        consumers['gamma'] = None
                        consumer_clients['gamma'] = None
                        gamma_failed = True

            # Try to reconnect to failed clusters
            if alpha_failed and not connect_consumer('alpha'):
                time.sleep(0.1)  # Small delay to avoid hammering the cluster
            if beta_failed and not connect_consumer('beta'):
                time.sleep(0.1)
            if gamma_failed and not connect_consumer('gamma'):
                time.sleep(0.1)

        except Exception as e:
            log_message("CONSUMER", f"Error: {str(e)}", "red")
            time.sleep(1)

    # Clean up all consumers and clients
    for cluster in ['alpha', 'beta', 'gamma']:
        try:
            if consumers[cluster]:
                consumers[cluster].close()
                consumers[cluster] = None
            if consumer_clients[cluster]:
                consumer_clients[cluster].close()
                consumer_clients[cluster] = None
        except Exception as e:
            log_message("CONSUMER", f"Error during cleanup of {cluster}: {str(e)}", "red")

    log_message("CONSUMER", "Consumer task completed", "green")
    log_message("SUMMARY", f"Received {len(received_messages)} unique messages", "yellow")
    message_counts['unique_total'] = len(received_messages)

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

        # Update message counts
        if not is_duplicate:
            message_counts[f'{source}_received'] += 1
            message_counts['unique_total'] = len(received_messages)
            message_counts['end_to_end_success'] += 1

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

def check_cluster_status(cluster_name):
    """Check if a cluster is running by querying Docker"""
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={cluster_name}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=True
        )
        return cluster_name in result.stdout.strip()
    except subprocess.CalledProcessError:
        return False

def monitor_clusters():
    """Monitor the status of all clusters and update the global flags"""
    global alpha_failed, beta_failed, gamma_failed

    while running:
        # Check alpha cluster
        alpha_running = check_cluster_status("alpha")
        if not alpha_running and not alpha_failed:
            log_message("MONITOR", "Alpha cluster is down", "yellow")
            alpha_failed = True
        elif alpha_running and alpha_failed:
            log_message("MONITOR", "Alpha cluster is back up", "green")
            alpha_failed = False

        # Check the beta cluster
        beta_running = check_cluster_status("beta")
        if not beta_running and not beta_failed:
            log_message("MONITOR", "Beta cluster is down", "yellow")
            beta_failed = True
        elif beta_running and beta_failed:
            log_message("MONITOR", "Beta cluster is back up", "green")
            beta_failed = False

        # Check gamma cluster
        gamma_running = check_cluster_status("gamma")
        if not gamma_running and not gamma_failed:
            log_message("MONITOR", "Gamma cluster is down", "yellow")
            gamma_failed = True
        elif gamma_running and gamma_failed:
            log_message("MONITOR", "Gamma cluster is back up", "green")
            gamma_failed = False

        # Sleep for a short time before checking again
        time.sleep(5)

def simulate_failures():
    """
    Legacy function for simulating failures in-process.
    This is kept for backward compatibility but is not used when running with the chaos script.
    """
    global alpha_failed, beta_failed, gamma_failed
    while running:
        # Wait for a random time between min_interval and max_interval seconds
        sleep_time = random.randint(chaos_params['min_interval'], chaos_params['max_interval'])
        time.sleep(sleep_time)

        if random.random() < chaos_params['failure_probability']:
            # Choose a random cluster to fail
            cluster = random.choice(['alpha', 'beta', 'gamma'])

            if cluster == 'alpha':
                log_message("SIMULATOR", "Simulating alpha cluster failure", "yellow")
                alpha_failed = True

                # Keep alpha down for a random time between min_downtime and max_downtime
                failure_duration = random.randint(chaos_params['min_downtime'], chaos_params['max_downtime'])
                time.sleep(failure_duration)

                log_message("SIMULATOR", "Alpha cluster recovered", "green")
                alpha_failed = False

            elif cluster == 'beta':
                log_message("SIMULATOR", "Simulating beta cluster failure", "yellow")
                beta_failed = True

                # Keep beta down for a random time between min_downtime and max_downtime
                failure_duration = random.randint(chaos_params['min_downtime'], chaos_params['max_downtime'])
                time.sleep(failure_duration)

                log_message("SIMULATOR", "Beta cluster recovered", "green")
                beta_failed = False

            elif cluster == 'gamma':
                log_message("SIMULATOR", "Simulating gamma cluster failure", "yellow")
                gamma_failed = True

                # Keep gamma down for a random time between min_downtime and max_downtime
                failure_duration = random.randint(chaos_params['min_downtime'], chaos_params['max_downtime'])
                time.sleep(failure_duration)

                log_message("SIMULATOR", "Gamma cluster recovered", "green")
                gamma_failed = False

def create_counter_window():
    # Create a separate figure for counters
    counter_fig = plt.figure(figsize=(8, 6))  # Increased height for sliders
    counter_fig.suptitle('Message Counters & Controls', fontsize=16)

    # Create grid spec for layout
    gs = plt.GridSpec(2, 1, height_ratios=[3, 1], figure=counter_fig)

    # Counter area
    counter_ax = counter_fig.add_subplot(gs[0])

    # Set axis limits and remove ticks
    counter_ax.set_xlim(0, 1)
    counter_ax.set_ylim(0, 1)
    counter_ax.set_xticks([])
    counter_ax.set_yticks([])

    # Slider area
    slider_ax = counter_fig.add_subplot(gs[1])
    slider_ax.set_title('Chaos Parameters', fontsize=12)

    # Create sliders
    from matplotlib.widgets import Slider

    # Failure probability slider
    failure_prob_ax = plt.axes([0.2, 0.15, 0.65, 0.03])
    failure_prob_slider = Slider(
        ax=failure_prob_ax,
        label='Failure Probability',
        valmin=0.0,
        valmax=1.0,
        valinit=0.7,
        valstep=0.05
    )

    # Downtime slider (min/max)
    downtime_ax = plt.axes([0.2, 0.1, 0.65, 0.03])
    downtime_slider = Slider(
        ax=downtime_ax,
        label='Max Downtime (s)',
        valmin=5,
        valmax=60,
        valinit=30,
        valstep=5
    )

    # Interval slider (min/max)
    interval_ax = plt.axes([0.2, 0.05, 0.65, 0.03])
    interval_slider = Slider(
        ax=interval_ax,
        label='Min Interval (s)',
        valmin=10,
        valmax=120,
        valinit=30,
        valstep=10
    )

    # Hide the main slider axis
    slider_ax.set_visible(False)

    return counter_fig, counter_ax, failure_prob_slider, downtime_slider, interval_slider

def create_visualization():
    # Create figure and subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
    fig.suptitle('Pulsar Message Flow Visualization', fontsize=16)

    # Create counter window with sliders
    counter_fig, counter_ax, failure_prob_slider, downtime_slider, interval_slider = create_counter_window()

    # Connect sliders to update chaos parameters
    def update_failure_prob(val):
        chaos_params['failure_probability'] = val

    def update_downtime(val):
        chaos_params['max_downtime'] = val
        chaos_params['min_downtime'] = max(5, val // 2)  # Set min to half of max or 5, whichever is larger

    def update_interval(val):
        chaos_params['min_interval'] = val
        chaos_params['max_interval'] = val * 2  # Set max to double of min

    # Register callbacks
    failure_prob_slider.on_changed(update_failure_prob)
    downtime_slider.on_changed(update_downtime)
    interval_slider.on_changed(update_interval)

    # Create rectangles for clusters and components
    # Producer rectangles for each cluster
    producer_alpha_rect = patches.Rectangle((0.05, 0.8), 0.15, 0.15, linewidth=2, edgecolor='blue', facecolor='lightblue', label='Alpha Producer')
    producer_beta_rect = patches.Rectangle((0.05, 0.55), 0.15, 0.15, linewidth=2, edgecolor='blue', facecolor='lightblue', label='Beta Producer')
    producer_gamma_rect = patches.Rectangle((0.05, 0.3), 0.15, 0.15, linewidth=2, edgecolor='blue', facecolor='lightblue', label='Gamma Producer')

    # Cluster rectangles
    alpha_rect = patches.Rectangle((0.5, 0.8), 0.2, 0.15, linewidth=2, edgecolor='green', facecolor='lightgreen', label='Alpha Cluster')
    beta_rect = patches.Rectangle((0.5, 0.55), 0.2, 0.15, linewidth=2, edgecolor='orange', facecolor='lightsalmon', label='Beta Cluster')
    gamma_rect = patches.Rectangle((0.5, 0.3), 0.2, 0.15, linewidth=2, edgecolor='red', facecolor='lightcoral', label='Gamma Cluster')

    # Consumer rectangles for each cluster
    consumer_alpha_rect = patches.Rectangle((0.8, 0.8), 0.15, 0.15, linewidth=2, edgecolor='purple', facecolor='plum', label='Alpha Consumer')
    consumer_beta_rect = patches.Rectangle((0.8, 0.55), 0.15, 0.15, linewidth=2, edgecolor='purple', facecolor='plum', label='Beta Consumer')
    consumer_gamma_rect = patches.Rectangle((0.8, 0.3), 0.15, 0.15, linewidth=2, edgecolor='purple', facecolor='plum', label='Gamma Consumer')

    # Add rectangles to the plot
    ax1.add_patch(producer_alpha_rect)
    ax1.add_patch(producer_beta_rect)
    ax1.add_patch(producer_gamma_rect)
    ax1.add_patch(alpha_rect)
    ax1.add_patch(beta_rect)
    ax1.add_patch(gamma_rect)
    ax1.add_patch(consumer_alpha_rect)
    ax1.add_patch(consumer_beta_rect)
    ax1.add_patch(consumer_gamma_rect)

    # Add labels
    ax1.text(0.125, 0.875, 'Alpha\nProducer', ha='center')
    ax1.text(0.125, 0.625, 'Beta\nProducer', ha='center')
    ax1.text(0.125, 0.375, 'Gamma\nProducer', ha='center')
    ax1.text(0.6, 0.875, 'Alpha', ha='center')
    ax1.text(0.6, 0.625, 'Beta', ha='center')
    ax1.text(0.6, 0.375, 'Gamma', ha='center')
    ax1.text(0.875, 0.875, 'Alpha\nConsumer', ha='center')
    ax1.text(0.875, 0.625, 'Beta\nConsumer', ha='center')
    ax1.text(0.875, 0.375, 'Gamma\nConsumer', ha='center')

    # Set axis limits and remove ticks
    ax1.set_xlim(0, 1.1)
    ax1.set_ylim(0.3, 1)
    ax1.set_xticks([])
    ax1.set_yticks([])
    ax1.set_title('Message Flow')

    # Create empty line objects for messages
    producer_to_alpha_line, = ax1.plot([], [], 'g-', lw=2, alpha=0.7)
    producer_to_beta_line, = ax1.plot([], [], 'y-', lw=2, alpha=0.7)
    producer_to_gamma_line, = ax1.plot([], [], 'r-', lw=2, alpha=0.7)
    alpha_to_consumer_line, = ax1.plot([], [], 'g-', lw=2, alpha=0.7)
    beta_to_consumer_line, = ax1.plot([], [], 'y-', lw=2, alpha=0.7)
    gamma_to_consumer_line, = ax1.plot([], [], 'r-', lw=2, alpha=0.7)

    # Create empty scatter objects for messages
    producer_to_alpha_msgs = ax1.scatter([], [], c='green', s=50, alpha=0.7)
    producer_to_beta_msgs = ax1.scatter([], [], c='orange', s=50, alpha=0.7)
    producer_to_gamma_msgs = ax1.scatter([], [], c='red', s=50, alpha=0.7)
    alpha_to_consumer_msgs = ax1.scatter([], [], c='green', s=50, alpha=0.7)
    beta_to_consumer_msgs = ax1.scatter([], [], c='orange', s=50, alpha=0.7)
    gamma_to_consumer_msgs = ax1.scatter([], [], c='red', s=50, alpha=0.7)

    # Create text objects for connection status
    producer_alpha_status = ax1.text(0.125, 0.825, "", ha='center', fontsize=8, color='black')
    producer_beta_status = ax1.text(0.125, 0.575, "", ha='center', fontsize=8, color='black')
    producer_gamma_status = ax1.text(0.125, 0.325, "", ha='center', fontsize=8, color='black')
    consumer_alpha_status = ax1.text(0.875, 0.825, "", ha='center', fontsize=8, color='black')
    consumer_beta_status = ax1.text(0.875, 0.575, "", ha='center', fontsize=8, color='black')
    consumer_gamma_status = ax1.text(0.875, 0.325, "", ha='center', fontsize=8, color='black')

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

    # Function to update counter window
    def update_counter(frame):
        # Clear previous text
        for txt in counter_ax.texts:
            txt.remove()

        # Update status text
        status_text = f"Alpha: {'DOWN' if alpha_failed else 'UP'} | "
        status_text += f"Beta: {'DOWN' if beta_failed else 'UP'} | "
        status_text += f"Gamma: {'DOWN' if gamma_failed else 'UP'} | "
        status_text += f"Unique Messages: {message_counts['unique_total']} | "
        status_text += f"Duplicates: {len(messages_data['duplicates'])} | "
        status_text += f"Out-of-order: {len(messages_data['out_of_order'])}"

        # Calculate success rate
        success_rate = 0
        if message_counts['total_sent'] > 0:
            success_rate = (message_counts['end_to_end_success'] / message_counts['total_sent']) * 100

        # Add success rate to status text
        success_text = f"Success Rate: {success_rate:.2f}% ({message_counts['end_to_end_success']}/{message_counts['total_sent']})"

        # Create cluster-specific status text
        alpha_text = f"Alpha: Sent {message_counts['alpha_sent']} | Received {message_counts['alpha_received']}"
        beta_text = f"Beta: Sent {message_counts['beta_sent']} | Received {message_counts['beta_received']}"
        gamma_text = f"Gamma: Sent {message_counts['gamma_sent']} | Received {message_counts['gamma_received']}"

        # Add main status text at the top
        counter_ax.text(0.5, 0.8, status_text, ha='center', fontsize=12, 
                 bbox=dict(facecolor='white', alpha=0.9, boxstyle='round,pad=0.5'))

        # Add success rate text below the main status
        counter_ax.text(0.5, 0.7, success_text, ha='center', fontsize=12, 
                 bbox=dict(facecolor='white', alpha=0.9, boxstyle='round,pad=0.5'))

        # Add cluster-specific status text
        counter_ax.text(0.5, 0.6, alpha_text, ha='center', fontsize=11, 
                 bbox=dict(facecolor='lightgreen', alpha=0.9, boxstyle='round,pad=0.3'))
        counter_ax.text(0.5, 0.4, beta_text, ha='center', fontsize=11, 
                 bbox=dict(facecolor='lightsalmon', alpha=0.9, boxstyle='round,pad=0.3'))
        counter_ax.text(0.5, 0.2, gamma_text, ha='center', fontsize=11, 
                 bbox=dict(facecolor='lightcoral', alpha=0.9, boxstyle='round,pad=0.3'))

        counter_fig.canvas.draw_idle()

    # Animation update function
    def update(frame):
        # Update message flow visualization

        # Define the fixed positions for the components
        producer_alpha_pos = (0.125, 0.875)
        producer_beta_pos = (0.125, 0.625)
        producer_gamma_pos = (0.125, 0.375)
        alpha_pos = (0.6, 0.875)
        beta_pos = (0.6, 0.625)
        gamma_pos = (0.6, 0.375)
        consumer_alpha_pos = (0.875, 0.875)
        consumer_beta_pos = (0.875, 0.625)
        consumer_gamma_pos = (0.875, 0.375)

        # Set the fixed lines for the message paths
        producer_to_alpha_line.set_data([producer_alpha_pos[0], alpha_pos[0]], [producer_alpha_pos[1], alpha_pos[1]])
        producer_to_beta_line.set_data([producer_beta_pos[0], beta_pos[0]], [producer_beta_pos[1], beta_pos[1]])
        producer_to_gamma_line.set_data([producer_gamma_pos[0], gamma_pos[0]], [producer_gamma_pos[1], gamma_pos[1]])
        alpha_to_consumer_line.set_data([alpha_pos[0], consumer_alpha_pos[0]], [alpha_pos[1], consumer_alpha_pos[1]])
        beta_to_consumer_line.set_data([beta_pos[0], consumer_beta_pos[0]], [beta_pos[1], consumer_beta_pos[1]])
        gamma_to_consumer_line.set_data([gamma_pos[0], consumer_gamma_pos[0]], [gamma_pos[1], consumer_gamma_pos[1]])

        # Update connection status text
        # Producer status
        producer_alpha_status.set_text("Connected" if producers['alpha'] is not None else "Disconnected")
        producer_alpha_status.set_color("green" if producers['alpha'] is not None else "red")
        producer_beta_status.set_text("Connected" if producers['beta'] is not None else "Disconnected")
        producer_beta_status.set_color("green" if producers['beta'] is not None else "red")
        producer_gamma_status.set_text("Connected" if producers['gamma'] is not None else "Disconnected")
        producer_gamma_status.set_color("green" if producers['gamma'] is not None else "red")

        # Consumer status
        consumer_alpha_status.set_text("Connected" if consumers['alpha'] is not None else "Disconnected")
        consumer_alpha_status.set_color("green" if consumers['alpha'] is not None else "red")
        consumer_beta_status.set_text("Connected" if consumers['beta'] is not None else "Disconnected")
        consumer_beta_status.set_color("green" if consumers['beta'] is not None else "red")
        consumer_gamma_status.set_text("Connected" if consumers['gamma'] is not None else "Disconnected")
        consumer_gamma_status.set_color("green" if consumers['gamma'] is not None else "red")

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

        # Update the counter-window
        update_counter(frame)

        return (producer_to_alpha_line, producer_to_beta_line, producer_to_gamma_line,
                alpha_to_consumer_line, beta_to_consumer_line, gamma_to_consumer_line,
                producer_to_alpha_msgs, producer_to_beta_msgs, producer_to_gamma_msgs,
                alpha_to_consumer_msgs, beta_to_consumer_msgs, gamma_to_consumer_msgs,
                producer_alpha_status, producer_beta_status, producer_gamma_status,
                consumer_alpha_status, consumer_beta_status, consumer_gamma_status,
                latency_line, duplicate_markers, out_of_order_markers)

    # Create animation for main visualization
    ani = FuncAnimation(fig, update, frames=range(1000), interval=500, blit=False)

    # Create animation for counter window
    counter_ani = FuncAnimation(counter_fig, update_counter, frames=range(1000), interval=500, blit=False)

    plt.tight_layout()

    # Show both windows
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

    # Check if we should use the external chaos script or the internal simulator
    use_external_chaos = os.environ.get('USE_EXTERNAL_CHAOS', 'false').lower() == 'true'

    if use_external_chaos:
        # Start the cluster monitor thread
        log_message("MAIN", "Using external chaos script for failure simulation", "cyan")
        monitor_thread = threading.Thread(target=monitor_clusters)
        monitor_thread.daemon = True
        monitor_thread.start()
    else:
        # Start the failure simulator thread
        log_message("MAIN", "Using internal failure simulator", "cyan")
        failure_thread = threading.Thread(target=simulate_failures)
        failure_thread.daemon = True
        failure_thread.start()

    # Start the visualization
    create_visualization()

    # Wait for threads to complete
    consumer_thread.join()
    producer_thread.join()

    # Join the appropriate thread based on which one we started
    if use_external_chaos:
        monitor_thread.join()
    else:
        failure_thread.join()

if __name__ == "__main__":
    main()
