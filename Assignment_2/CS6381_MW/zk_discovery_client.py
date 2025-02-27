import sys
import time
import logging
from kazoo.client import KazooClient, KazooState

logging.basicConfig(level=logging.INFO)

class SequentialLeaderElection:
    def __init__(self, zk_hosts, ip, port, election_path="discovery/replicas"):
        self.zk = zk_hosts
        self.election_path = election_path
        self.zk = None
        self.node_path = None  # The path of the ephemeral sequential node we create
        self.leader = False    # Flag indicating if we are the leader
        self.ip = ip
        self.port = port

    def start(self):
        """Start the ZooKeeper client and run the election process."""
        #self.zk = KazooClient(hosts=self.zk_hosts)
        self.zk.add_listener(self.zk_listener)
        self.zk.start()

        # Ensure the parent election path exists (persistent znode).
        self.zk.ensure_path(self.election_path)

        # Create an ephemeral sequential node under the election path.
        value = f"{self.ip}:{self.port}"
        value_bytes = value.encode('utf-8')
        self.node_path = self.zk.create(
            f"{self.election_path}/replica",
            value=value_bytes,              # We can store data if needed
            ephemeral=True,
            sequence=True,
        )
        logging.info(f"Created ephemeral sequential node: {self.node_path}")

        # Now attempt to become the leader or watch the predecessor.
        self.check_leader()

    def zk_listener(self, state):
        """Listener for Kazoo state changes."""
        if state == KazooState.LOST:
            logging.warning("Session lost. We are no longer connected to ZooKeeper.")
        elif state == KazooState.SUSPENDED:
            logging.warning("Connection suspended. Will attempt to reconnect.")
        elif state == KazooState.CONNECTED:
            logging.info("Connected (or reconnected) to ZooKeeper.")

    def check_leader(self):
        """
        Check if we are the leader by comparing our node's sequence number
        to the other sequential nodes.
        """
        # Get all children under the election path
        children = self.zk.get_children(self.election_path)
        # Sort them so the node with the lowest sequence is the leader
        sorted_nodes = sorted(children)
        logging.info(f"Children under {self.election_path}: {sorted_nodes}")

        # Our node name is everything after the parent path + "/"
        node_name = self.node_path.split("/")[-1]

        if sorted_nodes[0] == node_name:
            # We are the leader
            self.leader = True
            logging.info("We are the leader now!")
            # Perform leader-specific tasks here
        else:
            # Find our position in the sorted list
            index = sorted_nodes.index(node_name)
            # Watch the node immediately preceding ours
            predecessor_node = sorted_nodes[index - 1]
            predecessor_path = f"{self.election_path}/{predecessor_node}"

            logging.info(f"Not the leader. Watching predecessor node: {predecessor_path}")

            # Set a watch on the predecessor node. If it disappears, we re-check leadership.
            @self.zk.DataWatch(predecessor_path)
            def watch_node(data, stat, event):
                # If predecessor node is gone, check if we are the new leader
                if event and event.type == "DELETED":
                    logging.info(f"Predecessor {predecessor_node} deleted. Checking if we are now the leader.")
                    self.check_leader()
                return True  # Return True to keep the watch

    def become_primary(self): 
        """ Once elected as leader, write our node name into the dedicated primary znode. This acts as a centralized pointer for clients. """
        try: # Attempt to create the primary node as an ephemeral node. 
            self.zk.create(self.primary_path, value=self.node_name.encode('utf-8'), ephemeral=True)
            logging.info(f"Primary node created at {self.primary_path} with value {self.node_name}")

        except NodeExistsError: # If the node already exists, update its value. 
            self.zk.set(self.primary_path, self.node_name.encode('utf-8')) 
            logging.info(f"Primary node {self.primary_path} updated with value {self.node_name}")

    def stop(self):
        """Stop the ZooKeeper client."""
        if self.zk:
            self.zk.stop()
            self.zk.close()
            logging.info("Stopped ZooKeeper client.")

if __name__ == "__main__":
    zk_hosts = "127.0.0.1:2181"
    if len(sys.argv) > 1:
        zk_hosts = sys.argv[1]

    election = SequentialLeaderElection(zk_hosts)
    election.start()

    try:
        while True:
            # Keep running to maintain the ephemeral node
            time.sleep(2)
    except KeyboardInterrupt:
        election.stop()
