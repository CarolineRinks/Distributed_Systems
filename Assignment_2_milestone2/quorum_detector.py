import subprocess
import zmq
import logging
import configparser
import json
import time
import socket
import signal
import sys
from CS6381_MW import discovery_pb2
from CS6381_MW.zkclient import ZK_Driver
from CS6381_MW.zk_discovery_client import SequentialLeaderElection
import subprocess
processes = []

# Global flag to indicate shutdown.
shutdown_flag = False

def signal_handler(sig, frame):
    global shutdown_flag
    print("Shutdown signal received. Exiting.")
    shutdown_flag = True
    for p in processes:
        p.terminate()  # or p.kill() if needed
    sys.exit(0)

# Register the signal handler for SIGINT (Ctrl+C)
signal.signal(signal.SIGINT, signal_handler)

def find_free_port(start_port=5555, end_port=5600):
    """Return the first free port in the range [start_port, end_port)."""
    for port in range(start_port, end_port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("localhost", port))
                return port
            except OSError:
                continue
    raise Exception("No free port found in the range.")

zkclient_obj = ZK_Driver()
zkclient_obj.init_driver()
zkclient_obj.start_session()

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

@zkclient_obj.zk.ChildrenWatch("/root/discovery/replicas")
def watch_replicas(children):
    global shutdown_flag
    if shutdown_flag:
        return  # Do nothing if we're shutting down.
    count = len(children)
    try:
        if count < 3:
            free_port = find_free_port(5555, 5600)
            print(f"Quorum lost: only {count} replica(s). Blocking new registrations.")
            # Set quorum_active flag if needed.
            quorum_active = False
            # Spawn a new Discovery replica on a free port.
            p=subprocess.Popen(["python3", "DiscoveryAppln.py", 
                              "-a", "localhost", 
                              "-p", str(free_port)])
            processes.append(p)
        else:
            print(f"Quorum restored with {count} replicas. Accepting registrations.")
            quorum_active = True
    except Exception as e:
        print("Failed to spawn new replica: " + str(e))

# Keep the script running so that the ChildrenWatch remains active.
while not shutdown_flag:
    time.sleep(1)
