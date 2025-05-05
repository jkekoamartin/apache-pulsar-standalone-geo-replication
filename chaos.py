#!/usr/bin/env python3.9
import subprocess
import time
import random
import argparse
import signal
import sys
from datetime import datetime

# Flag to control the script
running = True

def signal_handler(sig, frame):
    global running
    print("\nShutting down chaos script...")
    # Make sure all clusters are running before exiting
    ensure_all_clusters_running()
    running = False
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def log_message(message, color=None):
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    # We don't actually use the color parameter in this script, but we accept it
    # for compatibility with calls that include a color
    print(f"[{timestamp}] [CHAOS] {message}")

def check_cluster_status(cluster_name):
    """Check if a cluster is running"""
    try:
        # First check if the container is running
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={cluster_name}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=True
        )
        if cluster_name not in result.stdout.strip():
            return False

        # If the container is running, check if the Pulsar service is ready
        # by trying to get the cluster status
        port_map = {
            "alpha": 8080,
            "beta": 8081,
            "gamma": 8082
        }

        # Use curl to check if the admin API is responding
        try:
            health_check = subprocess.run(
                ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", f"http://localhost:{port_map[cluster_name]}/admin/v2/clusters"],
                capture_output=True,
                text=True,
                timeout=2  # Set a short timeout
            )
            # If we get a 200 response, the service is ready
            return health_check.stdout.strip() == "200"
        except (subprocess.SubprocessError, subprocess.TimeoutExpired):
            # If curl fails or times out, the service is not ready
            return False
    except subprocess.CalledProcessError:
        return False

def stop_cluster(cluster_name):
    """Stop a cluster container"""
    try:
        log_message(f"Stopping {cluster_name} cluster...")
        subprocess.run(
            ["docker", "stop", cluster_name],
            capture_output=True,
            check=True
        )
        log_message(f"{cluster_name} cluster stopped")
        return True
    except subprocess.CalledProcessError as e:
        log_message(f"Failed to stop {cluster_name} cluster: {e}")
        return False

def start_cluster(cluster_name, max_retries=3, retry_delay=5):
    """Start a cluster container with retries"""
    for attempt in range(1, max_retries + 1):
        try:
            log_message(f"Starting {cluster_name} cluster (attempt {attempt}/{max_retries})...")
            subprocess.run(
                ["docker", "start", cluster_name],
                capture_output=True,
                check=True
            )

            # Wait for the container to start
            time.sleep(5)

            # Check if the container is running
            if not check_cluster_status(cluster_name):
                log_message(f"{cluster_name} container started but service not ready, retrying...")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                continue

            log_message(f"{cluster_name} cluster started and service is ready")
            return True
        except subprocess.CalledProcessError as e:
            log_message(f"Failed to start {cluster_name} cluster (attempt {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                log_message(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                log_message(f"Max retries reached for {cluster_name}, giving up")
                return False

    return False

def ensure_all_clusters_running():
    """Make sure all clusters are running before exiting"""
    clusters = ["alpha", "beta", "gamma"]
    all_running = True

    for cluster in clusters:
        if not check_cluster_status(cluster):
            log_message(f"{cluster} cluster is not running or not ready, attempting to start...")
            if not start_cluster(cluster, max_retries=3, retry_delay=5):
                log_message(f"Failed to start {cluster} cluster after multiple attempts")
                all_running = False

    return all_running

def main():
    parser = argparse.ArgumentParser(description='Chaos testing script for Pulsar clusters')
    parser.add_argument('--min-interval', type=int, default=30, 
                        help='Minimum interval between failures in seconds (default: 30)')
    parser.add_argument('--max-interval', type=int, default=90, 
                        help='Maximum interval between failures in seconds (default: 90)')
    parser.add_argument('--min-downtime', type=int, default=10, 
                        help='Minimum downtime for a cluster in seconds (default: 10)')
    parser.add_argument('--max-downtime', type=int, default=30, 
                        help='Maximum downtime for a cluster in seconds (default: 30)')
    parser.add_argument('--failure-probability', type=float, default=0.7, 
                        help='Probability of triggering a failure (default: 0.7)')
    parser.add_argument('--run-time', type=int, default=30,
                        help='Total run time in seconds (default: 30)')
    parser.add_argument('--no-chaos', action='store_true',
                        help='Run without introducing any failures')
    args = parser.parse_args()

    # If no-chaos flag is set, set failure probability to 0
    if args.no_chaos:
        args.failure_probability = 0
        log_message("Starting chaos testing script in NO-CHAOS mode")
        log_message(f"Running for {args.run_time} seconds with no failures")
    else:
        log_message("Starting chaos testing script")
        log_message(f"Interval: {args.min_interval}-{args.max_interval}s, Downtime: {args.min_downtime}-{args.max_downtime}s")
        log_message(f"Failure probability: {args.failure_probability}")

    # Make sure all clusters are running at the start
    ensure_all_clusters_running()

    clusters = ["alpha", "beta", "gamma"]

    # Record start time
    start_time = time.time()

    while running:
        # Check if we've reached the run time limit
        elapsed_time = time.time() - start_time
        if args.run_time > 0 and elapsed_time >= args.run_time:
            log_message(f"Run time of {args.run_time} seconds reached. Exiting...")
            break

        # If in no-chaos mode, actively ensure all clusters remain running
        if args.no_chaos:
            remaining_time = args.run_time - elapsed_time
            if remaining_time <= 0:
                break

            # Check if all clusters are running and ready
            all_running = True
            for cluster in clusters:
                if not check_cluster_status(cluster):
                    log_message(f"{cluster} cluster is down or not ready in no-chaos mode")
                    all_running = False

            # If any cluster is down, try to ensure all are running
            if not all_running:
                log_message("Some clusters are down in no-chaos mode, attempting to restore all clusters")
                if not ensure_all_clusters_running():
                    log_message("WARNING: Failed to restore all clusters in no-chaos mode", "red")
                    # Even if we failed to restore all clusters, continue with the ones that are running

            # Log status with more detail
            status_details = []
            for cluster in clusters:
                status = "READY" if check_cluster_status(cluster) else "DOWN"
                status_details.append(f"{cluster}: {status}")

            status_str = ", ".join(status_details)
            log_message(f"Cluster status: {status_str}. {remaining_time:.1f} seconds remaining...")

            # Check very frequently (every 1 second) to ensure clusters stay up
            time.sleep(min(1, remaining_time))
            continue

        # Normal chaos mode - Wait for a random time between min_interval and max_interval seconds
        sleep_time = random.randint(args.min_interval, args.max_interval)
        log_message(f"Waiting {sleep_time} seconds before next chaos event")
        time.sleep(sleep_time)

        if random.random() < args.failure_probability:
            # Choose a random cluster to fail
            target_cluster = random.choice(clusters)

            # Check if the cluster is running
            if check_cluster_status(target_cluster):
                # Stop the cluster
                if stop_cluster(target_cluster):
                    # Keep the cluster down for a random time
                    downtime = random.randint(args.min_downtime, args.max_downtime)
                    log_message(f"Keeping {target_cluster} down for {downtime} seconds")
                    time.sleep(downtime)

                    # Start the cluster again
                    start_cluster(target_cluster)
            else:
                log_message(f"{target_cluster} cluster is already down, starting it")
                start_cluster(target_cluster)

    # Make sure all clusters are running before exiting
    ensure_all_clusters_running()
    log_message("Chaos script completed successfully")

if __name__ == "__main__":
    main()
