#!/usr/bin/env python3
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

def log_message(message):
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] [CHAOS] {message}")

def check_cluster_status(cluster_name):
    """Check if a cluster is running"""
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

def start_cluster(cluster_name):
    """Start a cluster container"""
    try:
        log_message(f"Starting {cluster_name} cluster...")
        subprocess.run(
            ["docker", "start", cluster_name],
            capture_output=True,
            check=True
        )
        log_message(f"{cluster_name} cluster started")
        return True
    except subprocess.CalledProcessError as e:
        log_message(f"Failed to start {cluster_name} cluster: {e}")
        return False

def ensure_all_clusters_running():
    """Make sure all clusters are running before exiting"""
    clusters = ["alpha", "beta", "gamma"]
    for cluster in clusters:
        if not check_cluster_status(cluster):
            start_cluster(cluster)
            # Give it some time to start up
            time.sleep(5)

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
    args = parser.parse_args()

    log_message("Starting chaos testing script")
    log_message(f"Interval: {args.min_interval}-{args.max_interval}s, Downtime: {args.min_downtime}-{args.max_downtime}s")
    log_message(f"Failure probability: {args.failure_probability}")
    
    # Make sure all clusters are running at the start
    ensure_all_clusters_running()
    
    clusters = ["alpha", "beta", "gamma"]
    
    while running:
        # Wait for a random time between min_interval and max_interval seconds
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

if __name__ == "__main__":
    main()