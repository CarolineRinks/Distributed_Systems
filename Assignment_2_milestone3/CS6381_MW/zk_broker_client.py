# zk_broker_client.py
import time
import threading
import logging
from kazoo.exceptions import NodeExistsError

class BrokerLeaderElection:
    """
    Implements leader election for the Broker using ephemeral sequential nodes with a fixed lease.
    When elected, the leader writes its identity to a primary znode and sets a lease
    that will not be renewed. When the lease expires, the leader resigns (deletes
    the primary pointer and its own election node) and re-joins as a new candidate.
    """

    def __init__(self, zk_driver, broker_mw,
                 ip, front_port, back_port,
                 election_path="/root/broker/replica",
                 primary_path="/root/broker/primary",
                 lease_path="/root/broker/lease",
                 lease_duration=30):
        self.zk = zk_driver.zk
        self.broker_mw = broker_mw  # So we can call back into BrokerMW to register
        self.ip = ip
        self.front_port = front_port
        self.back_port = back_port
        self.election_path = election_path
        self.primary_path = primary_path
        self.lease_path = lease_path
        self.lease_duration = lease_duration

        self.node_path = None
        self.node_name = None
        self.is_leader = False
        self.logger = logging.getLogger("BrokerLeaderElection")
        self.lease_monitor_thread = None

    def start(self):
        """Join the ephemeral-sequential election path and start the lease monitor."""
        self.zk.ensure_path(self.election_path)
        # The ephemeral node value might store "IP:front_port:back_port"
        value_str = f"{self.ip}:{self.front_port}:{self.back_port}"
        self.node_path = self.zk.create(
            f"{self.election_path}/node_",
            value_str.encode('utf-8'),
            ephemeral=True,
            sequence=True
        )
        self.node_name = self.node_path.split("/")[-1]
        self.logger.info(f"Broker election node created: {self.node_path}")

        self.check_leader()
        if not self.lease_monitor_thread or not self.lease_monitor_thread.is_alive():
            self.lease_monitor_thread = threading.Thread(target=self.monitor_lease)
            self.lease_monitor_thread.daemon = True
            self.lease_monitor_thread.start()

    def check_leader(self):
        """Check if this node is the leader (lowest seq)."""
        children = self.zk.get_children(self.election_path)
        sorted_nodes = sorted(children)
        self.logger.info(f"Broker election nodes: {sorted_nodes}")
        if sorted_nodes and sorted_nodes[0] == self.node_name:
            if not self.is_leader:
                self.become_leader()
        else:
            self.is_leader = False
            index = sorted_nodes.index(self.node_name)
            predecessor = sorted_nodes[index - 1]
            predecessor_path = f"{self.election_path}/{predecessor}"
            self.logger.info(f"Broker not leader; watching predecessor: {predecessor_path}")

            @self.zk.DataWatch(predecessor_path)
            def watch_node(data, stat, event):
                if event and event.type == "DELETED":
                    self.logger.info("Broker predecessor deleted; re-checking leadership.")
                    self.check_leader()
                return True

    def become_leader(self):
        """When elected, set the broker as leader in /root/broker/primary, set lease, and register with Discovery."""
        self.is_leader = True
        value_str = f"{self.ip}:{self.front_port}:{self.back_port}"
        try:
            self.zk.create(self.primary_path, value_str.encode('utf-8'), ephemeral=True)
            self.logger.info(f"Broker primary node created at {self.primary_path} with value {value_str}")
        except NodeExistsError:
            self.zk.set(self.primary_path, value_str.encode('utf-8'))
            self.logger.info(f"Broker primary node updated with value {value_str}")

        # Set a single lease (no renewal).
        expiry_time = time.time() + self.lease_duration
        expiry_str = str(expiry_time).encode('utf-8')
        if not self.zk.exists(self.lease_path):
            self.zk.create(self.lease_path, expiry_str, makepath=True)
        else:
            self.zk.set(self.lease_path, expiry_str)
        self.logger.info(f"Broker lease set to expire at {expiry_time}")

        # Now that we're leader, call back to the BrokerMW to register with Discovery.
        self.broker_mw.register_as_leader()
        # self.broker_mw.event_loop()

    def monitor_lease(self):
        """Monitor the broker lease. If it expires, resign and re-join the election."""
        while True:
            try:
                if self.is_leader and self.zk.exists(self.lease_path):
                    data, stat = self.zk.get(self.lease_path)
                    lease_expiry = float(data.decode('utf-8'))
                    if time.time() > lease_expiry:
                        self.logger.info("Broker lease expired; resigning leadership.")
                        self.resign()
                        # Rejoin as a new candidate
                        self.start()
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"BrokerLeaseMonitorError: {e}")
                time.sleep(1)

    def resign(self):
        """Resign from leadership by deleting primary pointer and ephemeral node."""
        self.is_leader = False
        try:
            if self.zk.exists(self.primary_path):
                self.zk.delete(self.primary_path)
                self.logger.info("Broker primary node deleted; resigned leadership.")
            if self.zk.exists(self.node_path):
                self.zk.delete(self.node_path)
                self.logger.info("Broker ephemeral election node deleted.")
        except Exception as e:
            self.logger.error(f"BrokerResignError: {e}")
