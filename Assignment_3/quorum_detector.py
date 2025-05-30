import subprocess
import zmq
import logging
import configparser
import argparse
import json
import time
import socket
import signal
import sys
from CS6381_MW import discovery_pb2
from CS6381_MW.zkclient import ZK_Driver
from CS6381_MW.zk_discovery_client import SequentialLeaderElection
import subprocess



# Global flag to indicate shutdown.


class QuorumDetector:
    def __init__(self, group, processes, shutdown_flag):
        self.group = group
        self.logger = logging.getLogger("QuorumDetector")
        self.processes = processes
        self.shutdown_flag = shutdown_flag
        self.group = group
        self.quorum_active = False
        self.zkclient_obj = ZK_Driver()
        self.zkclient_obj.init_driver()
        self.zkclient_obj.start_session()

        @self.zkclient_obj.zk.ChildrenWatch(f"/root/discovery/group{self.group}/replica")
        def watch_replicas(children):
            if self.shutdown_flag:
                return  # Do nothing if we're shutting down.
            count = len(children)
            try:
                if count < 3:
                    # Set quorum_active flag if needed.
                    self.quorum_active = False
                    free_port = self.find_free_port(5555, 5600)
                    print(f"Quorum of Discovery lost: only {count} replica(s). Blocking new registrations.")
                    
                    # Spawn a new Discovery replica on a free port.
                    p=subprocess.Popen(["python3", "DiscoveryAppln.py", 
                                    "-a", "localhost", 
                                    "-p", str(free_port), "-g", str(self.group)])
                    self.processes.append(p)
                else:
                    print(f"Quorum of Discovery restored with {count} replicas. Accepting registrations.")
                    self.quorum_active = True
            except Exception as e:
                print("Failed to spawn new replica: " + str(e))


        @self.zkclient_obj.zk.ChildrenWatch(f"/root/broker/group{self.group}/replica")
        def watch_replicas(children):
            if self.shutdown_flag:
                return  # Do nothing if we're shutting down.
            count = len(children)
            try:
                if count < 3:
                    free_sub_port = self.find_free_port(6000, 6050)
                    free_pub_port = self.find_free_port(6050, 6100)
                    print(f"Quorum of Brokers lost: only {count} replica(s). Blocking Dissemination")
                    # Set quorum_active flag if needed.
                    quorum_active = False
                    # Spawn a new Discovery replica on a free port.
                    p=subprocess.Popen(["python3", "BrokerAppln.py", 
                                    "-a", "localhost", 
                                    "-sp", str(free_sub_port),
                                    "-pp", str(free_pub_port), "-g", str(self.group)])
                    self.processes.append(p)
                else:
                    print(f"Quorum of Brokers restored with {count} replicas. Dissemination Started Again.")
                    quorum_active = True
            except Exception as e:
                print("Failed to spawn new replica: " + str(e))

    def signal_handler(self, sig, frame):
        print("Shutdown signal received. Exiting.")
        self.shutdown_flag = True
        for p in self.processes:
            p.terminate()  # or p.kill() if needed
        sys.exit(0)

    def find_free_port(self, start_port=5555, end_port=5600):
        """Return the first free port in the range [start_port, end_port)."""
        for port in range(start_port, end_port):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(("localhost", port))
                    return port
                except OSError:
                    continue
        raise Exception("No free port found in the range.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Quorum Detector")
    parser.add_argument("-g", "--group", type=int, required=True, help="Group Number for the Quorum Detector")
    args = parser.parse_args()
    shutdown_flag = False
    processes = []

    quorum_detector = QuorumDetector(args.group, processes, shutdown_flag)
    signal.signal(signal.SIGINT, quorum_detector.signal_handler)

    # Keep the script running so that the ChildrenWatch remains active.
    while not shutdown_flag:
        time.sleep(1)



    
