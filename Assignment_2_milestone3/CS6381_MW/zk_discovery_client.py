# zk_discovery_client.py
import time
import threading
import logging
from kazoo.exceptions import NodeExistsError

class SequentialLeaderElection:
    """
    Implements leader election using ephemeral sequential nodes with a fixed lease.
    When elected, the leader writes its identity to a primary znode and sets a lease
    that will not be renewed. When the lease expires, the leader resigns (by deleting
    its primary pointer and its own election node) and re-joins the election as a new candidate.
    """
    def __init__(self, zk_driver, ip,port, election_path="/root/discovery/replica",
                 primary_path="/root/discovery/primary",
                 lease_path="/root/discovery/lease",
                 lease_duration=10,
                 state_callback=None):
        self.zk = zk_driver.zk  # Use the existing kazoo client from ZK_Driver
        self.ip = ip
        self.port = port
        self.election_path = election_path
        self.primary_path = primary_path
        self.lease_path = lease_path
        self.lease_duration = lease_duration
        self.node_path = None
        self.node_name = None
        self.is_leader = False
        self.logger = logging.getLogger("SequentialLeaderElection")
        self.lease_monitor_thread = None
        self.state_callback = state_callback

    def start(self):
        # Ensure election path exists.
        self.zk.ensure_path(self.election_path)
        value=f"{self.ip}:{self.port}"
        # Create our ephemeral sequential node.
        self.node_path = self.zk.create(f"{self.election_path}/node_", value=value.encode('utf-8'),
                                        ephemeral=True, sequence=True)
        self.node_name = self.node_path.split("/")[-1]
        self.logger.info(f"Created election node: {self.node_path}")
        self.check_leader()
        # Start the lease monitor thread if not already running.
        if self.lease_monitor_thread is None or not self.lease_monitor_thread.is_alive():
            self.lease_monitor_thread = threading.Thread(target=self.monitor_lease)
            self.lease_monitor_thread.daemon = True
            self.lease_monitor_thread.start()

    def check_leader(self):
        """Determine if we are the leader by sorting the ephemeral sequential nodes."""
        children = self.zk.get_children(self.election_path)
        sorted_nodes = sorted(children)
        self.logger.info(f"Election nodes: {sorted_nodes}")
        if sorted_nodes[0] == self.node_name:
            if not self.is_leader:
                self.become_leader()
        else:
            self.is_leader = False
            index = sorted_nodes.index(self.node_name)
            predecessor = sorted_nodes[index - 1]
            predecessor_path = f"{self.election_path}/{predecessor}"
            self.logger.info(f"Not leader; watching predecessor: {predecessor_path}")
            @self.zk.DataWatch(predecessor_path)
            def watch_node(data, stat, event):
                if event and event.type == "DELETED":
                    self.logger.info("Predecessor deleted; rechecking leadership")
                    self.check_leader()
                return True

    def become_leader(self):
        """When elected, mark ourselves as leader, create the primary pointer, and set the lease."""
        self.is_leader = True
        value=f"{self.ip}:{self.port}"
        try:
            # Write the primary pointer.
            
            self.zk.create(self.primary_path, value.encode('utf-8'),
                           ephemeral=True)
            self.logger.info(f"Primary node created at {self.primary_path} with value {value}")
        except NodeExistsError:
            self.zk.set(self.primary_path, value.encode('utf-8'))
            self.logger.info(f"Primary node updated with value {value}")
        # Set the lease expiration (once only, no renewal).
        expiry_time = time.time() + self.lease_duration
        expiry_str = str(expiry_time).encode('utf-8')
        if not self.zk.exists(self.lease_path):
            self.zk.create(self.lease_path, expiry_str, makepath=True)
        else:
            self.zk.set(self.lease_path, expiry_str)
        self.logger.info(f"Lease set to expire at {expiry_time}")

        if self.state_callback:
            self.state_callback()

    def monitor_lease(self):
        """For both leaders and backups, monitor the lease node and trigger re-election when it expires."""
        while True:
            try:
                if self.is_leader and self.zk.exists(self.lease_path):
                    data, stat = self.zk.get(self.lease_path)
                    lease_expiry = float(data.decode('utf-8'))
                    if time.time() > lease_expiry:
                        self.logger.info("Lease expired; resigning leadership")
                        self.resign()
                        # Rejoin the election as a new candidate.
                        self.start()
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"Error in lease monitor: {e}")
                time.sleep(1)

    def resign(self):
        """Resign from leadership by deleting the primary pointer and our election node.
           This allows a new candidate to win the election.
           The process itself does not exit, so it can later rejoin as a replica."""
        self.is_leader = False
        try:
            if self.zk.exists(self.primary_path):
                self.zk.delete(self.primary_path)
                self.logger.info("Primary node deleted; resigned from leadership")
            if self.zk.exists(self.node_path):
                self.zk.delete(self.node_path)
                self.logger.info("Election node deleted; leaving current election round")
        except Exception as e:
            self.logger.error(f"Error during resignation: {e}")
