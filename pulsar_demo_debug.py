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
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def log_message(source, message, color="white"):
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(colored(f"[{timestamp}] [{source}] {message}", color))

def detect_duplicate(message_id):
    if message_id in received_messages:
        log_message("DUPLICATE", f"Detected duplicate message: {message_id}", "red")
        return True
    received_messages.add(message_id)
    return False

def detect_out_of_order(sequence):
    global last_sequence
    if last_sequence != -1 and sequence < last_sequence:
        log_message("OUT-OF-ORDER", f"Detected out-of-order message: expected > {last_sequence}, got {sequence}", "red")
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
        return

    sequence = 0
    while running and sequence < 20:  # Limit to 20 messages for debugging
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

            # Log the message
            if not alpha_failed:
                log_message("PRODUCER", f"Sent message to alpha: {message_data['id']}", "cyan")
            else:
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
    except:
        pass

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

    message_count = 0
    while running and message_count < 25:  # Limit to 25 messages for debugging (more than producer to catch all)
        try:
            # Try to receive from alpha
            if not alpha_failed:
                try:
                    msg_alpha = consumer_alpha.receive(timeout_millis=500)
                    process_message(msg_alpha, consumer_alpha, 'alpha')
                    message_count += 1
                except Exception as e:
                    if "Timeout" not in str(e):
                        log_message("CONSUMER", f"Alpha error: {str(e)}", "red")

            # Try to receive from beta
            try:
                msg_beta = consumer_beta.receive(timeout_millis=500)
                process_message(msg_beta, consumer_beta, 'beta')
                message_count += 1
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
    except:
        pass

    log_message("CONSUMER", "Consumer task completed", "green")
    log_message("SUMMARY", f"Received {len(received_messages)} unique messages", "yellow")
    log_message("SUMMARY", f"Detected {message_count - len(received_messages)} duplicate messages", "yellow")

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

    # Wait a bit before starting to simulate failures
    time.sleep(5)

    # Simulate one failure for debugging
    log_message("SIMULATOR", "Simulating alpha cluster failure", "yellow")
    alpha_failed = True

    # Keep alpha down for 5 seconds
    time.sleep(5)

    log_message("SIMULATOR", "Alpha cluster recovered", "green")
    alpha_failed = False

def main():
    print(colored("Starting Pulsar Demo Debug Application", "cyan"))
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

    # Wait for threads to complete
    producer_thread.join()
    consumer_thread.join()
    failure_thread.join()

    print(colored("\nDebug demo completed!", "cyan"))

if __name__ == "__main__":
    main()
