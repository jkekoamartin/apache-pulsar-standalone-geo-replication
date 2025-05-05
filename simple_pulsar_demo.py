import pulsar
import time
import threading
import argparse
import json
import uuid
from datetime import datetime
from termcolor import colored

# Global variables to track messages
messages_sent = {
    'alpha': [],
    'beta': [],
    'gamma': []
}

messages_received = {
    'alpha': [],
    'beta': [],
    'gamma': []
}

# Flag to control the application
running = True

def log_message(source, message, color="white"):
    """Log a message with timestamp and color"""
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(colored(f"[{timestamp}] [{source}] {message}", color))

def producer_task(cluster_name, run_time):
    """Producer task that sends messages to a specific cluster"""
    # Connect to the cluster
    port_map = {
        'alpha': 6650,
        'beta': 6651,
        'gamma': 6652
    }

    try:
        log_message(f"{cluster_name.upper()} PRODUCER", f"Connecting to {cluster_name} cluster...", "cyan")
        client = pulsar.Client(f'pulsar://localhost:{port_map[cluster_name]}')
        producer = client.create_producer(
            'persistent://public/default/demo',
            properties={
                "producer-name": f"{cluster_name}-producer",
                "producer-id": f"{cluster_name}"
            }
        )

        log_message(f"{cluster_name.upper()} PRODUCER", f"Connected to {cluster_name} cluster", "green")

        # Send messages until the run time is reached
        start_time = time.time()
        message_count = 0

        while running and (time.time() - start_time) < run_time:
            # Create a unique message
            message_id = str(uuid.uuid4())
            message_data = {
                'id': message_id,
                'cluster': cluster_name,
                'count': message_count,
                'timestamp': time.time(),
                'content': f"{cluster_name} message {message_count}"
            }

            # Serialize message to JSON
            message_json = json.dumps(message_data)

            # Send the message
            producer.send(message_json.encode('utf-8'))

            # Store the message for logging
            messages_sent[cluster_name].append(message_data)

            log_message(f"{cluster_name.upper()} PRODUCER", f"Sent message: {message_data['content']}", "cyan")

            message_count += 1
            time.sleep(1)  # Send a message every second

        # Close the producer and client
        producer.close()
        client.close()

        log_message(f"{cluster_name.upper()} PRODUCER", f"Producer task completed. Sent {message_count} messages.", "green")

    except Exception as e:
        log_message(f"{cluster_name.upper()} PRODUCER", f"Error: {str(e)}", "red")

def consumer_task(cluster_name, run_time):
    """Consumer task that receives messages from a specific cluster"""
    # Connect to the cluster
    port_map = {
        'alpha': 6650,
        'beta': 6651,
        'gamma': 6652
    }

    # Track connection state
    connected = False
    client = None
    consumer = None
    max_retries = 3
    retry_count = 0

    # Receive messages until the run time is reached
    start_time = time.time()
    message_count = 0

    while running and (time.time() - start_time) < (run_time + 5):  # Add 5 seconds to ensure we get all messages
        # Try to connect if not connected
        if not connected:
            try:
                # Close previous connections if they exist
                if consumer:
                    try:
                        consumer.close()
                    except:
                        pass
                    consumer = None

                if client:
                    try:
                        client.close()
                    except:
                        pass
                    client = None

                # Create new connection
                log_message(f"{cluster_name.upper()} CONSUMER", f"Connecting to {cluster_name} cluster...", "cyan")
                client = pulsar.Client(f'pulsar://localhost:{port_map[cluster_name]}')

                # Use shared subscription type to avoid ConsumerBusy errors
                consumer = client.subscribe(
                    'persistent://public/default/demo',
                    f'{cluster_name}-subscription',
                    consumer_type=pulsar.ConsumerType.Shared
                )

                log_message(f"{cluster_name.upper()} CONSUMER", f"Connected to {cluster_name} cluster", "green")
                connected = True
                retry_count = 0  # Reset retry count on successful connection

            except Exception as e:
                retry_count += 1
                if "ConsumerBusy" in str(e):
                    log_message(f"{cluster_name.upper()} CONSUMER", f"Error: Pulsar error: ConsumerBusy", "red")
                else:
                    log_message(f"{cluster_name.upper()} CONSUMER", f"Error: {str(e)}", "red")

                # If we've tried too many times, wait longer before retrying
                if retry_count > max_retries:
                    time.sleep(2)
                else:
                    time.sleep(0.5)

                continue  # Try again in the next iteration

        # If connected, try to receive messages
        try:
            # Try to receive a message with timeout
            msg = consumer.receive(timeout_millis=1000)

            # Decode and parse the message
            message_json = msg.data().decode('utf-8')
            message_data = json.loads(message_json)

            # Store the message for logging
            messages_received[cluster_name].append(message_data)

            log_message(f"{cluster_name.upper()} CONSUMER", f"Received message: {message_data['content']}", "green")

            # Acknowledge the message
            consumer.acknowledge(msg)

            message_count += 1

        except Exception as e:
            # Log timeout errors
            if "Timeout" in str(e):
                log_message(f"{cluster_name.upper()} CONSUMER", f"Error: Pulsar error: TimeOut", "red")
            else:
                log_message(f"{cluster_name.upper()} CONSUMER", f"Error: {str(e)}", "red")
                connected = False  # Mark as disconnected for non-timeout errors

    # Clean up
    if consumer:
        try:
            consumer.close()
        except:
            pass

    if client:
        try:
            client.close()
        except:
            pass

    log_message(f"{cluster_name.upper()} CONSUMER", f"Consumer task completed. Received {message_count} messages.", "green")

def print_summary():
    """Print a summary of messages sent and received"""
    print("\n" + "="*80)
    print(colored("SUMMARY", "yellow"))
    print("="*80)

    for cluster in ['alpha', 'beta', 'gamma']:
        sent_count = len(messages_sent[cluster])
        received_count = len(messages_received[cluster])

        print(colored(f"\n{cluster.upper()} CLUSTER", "cyan"))
        print(f"Messages sent: {sent_count}")
        print(f"Messages received: {received_count}")

        # Print sent messages
        print(colored("\nSent Messages:", "cyan"))
        for msg in messages_sent[cluster]:
            print(f"  - {msg['content']} (ID: {msg['id']})")

        # Print received messages
        print(colored("\nReceived Messages:", "green"))
        for msg in messages_received[cluster]:
            print(f"  - {msg['content']} (ID: {msg['id']})")

    print("\n" + "="*80)

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Simple Pulsar Demo')
    parser.add_argument('--run-time', type=int, default=30, help='Run time in seconds (default: 30)')
    args = parser.parse_args()

    global running

    print(colored(f"Starting Simple Pulsar Demo (Run time: {args.run_time} seconds)", "cyan"))
    print(colored("Press Ctrl+C to exit early", "yellow"))

    try:
        # Start producer threads
        producer_threads = []
        for cluster in ['alpha', 'beta', 'gamma']:
            thread = threading.Thread(target=producer_task, args=(cluster, args.run_time))
            thread.daemon = True
            thread.start()
            producer_threads.append(thread)

        # Start consumer threads
        consumer_threads = []
        for cluster in ['alpha', 'beta', 'gamma']:
            thread = threading.Thread(target=consumer_task, args=(cluster, args.run_time))
            thread.daemon = True
            thread.start()
            consumer_threads.append(thread)

        # Wait for the run time to complete
        time.sleep(args.run_time)

        # Set running to False to stop the threads
        running = False

        # Wait for all threads to complete
        for thread in producer_threads + consumer_threads:
            thread.join(timeout=5)

        # Print summary
        print_summary()

    except KeyboardInterrupt:
        print(colored("\nShutting down...", "yellow"))
        running = False

        # Wait for all threads to complete
        for thread in producer_threads + consumer_threads:
            thread.join(timeout=5)

        # Print summary
        print_summary()

if __name__ == "__main__":
    main()
