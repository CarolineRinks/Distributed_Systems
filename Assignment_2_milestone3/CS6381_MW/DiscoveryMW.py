# discoverymw.py
import zmq
import logging
import configparser
import json
import time
from CS6381_MW import discovery_pb2
from .zkclient import ZK_Driver
from .zk_discovery_client import SequentialLeaderElection

class DiscoveryMW:
    """Middleware for the Discovery Service."""
    def __init__(self, logger, args):
        self.logger = logger
        self.context = zmq.Context()
        self.rep_socket = self.context.socket(zmq.REP)
        self.poller = zmq.Poller()
        self.discovery_ip = args.addr
        self.discovery_port = args.port

        self.registered_pubs = 0
        self.registered_subs = 0
        self.publishers = {}
        self.subscribers = {}
        self.broker = None
        self.group = args.group
        # Track whether we have a quorum (>=3 replicas).
        self.quorum_active = True

        # Initialize ZooKeeper driver
        self.zkclient_obj = ZK_Driver()
        self.zkclient_obj.init_driver()
        self.zkclient_obj.start_session()

        # State node path (persistent)
        self.state_path = f"/root/discovery/group{str(self.group)}/state"

        # Load existing state from ZooKeeper (if any)
        self.load_state()

        # Start the leader election with a fixed lease.
        self.zk_election = SequentialLeaderElection(
            self.zkclient_obj,
            self.discovery_ip,
            self.discovery_port,
            election_path=f"/root/discovery/group{self.group}/replica",
            primary_path=f"/root/discovery/group{self.group}/primary",
            lease_path=f"/root/discovery/group{self.group}/lease",
            lease_duration=180,  # seconds
            state_callback=self.load_state
        )
        self.zk_election.start()


        # Watch ephemeral nodes under /root/pubs and /root/subs to detect arrivals/departures
        @self.zkclient_obj.zk.ChildrenWatch("/root/pubs")
        def watch_publishers(children):
            self.logger.info("DiscoveryMW::ZK Watch - /root/pubs changed")
            self.handle_publisher_change(children)

        @self.zkclient_obj.zk.ChildrenWatch("/root/subs")
        def watch_subscribers(children):
            self.logger.info("DiscoveryMW::ZK Watch - /root/subs changed")
            self.handle_subscribers_change(children)

        # *** Quorum Watch ***  
        # Watch the ephemeral-sequential election path to see how many replicas are alive.
        @self.zkclient_obj.zk.ChildrenWatch(f"/root/discovery/group{self.group}/replica")
        def watch_replicas(children):
            count = len(children)
            if count < 3:
                self.logger.warning(f"Quorum lost: only {count} replica(s). Blocking new registrations.")
                self.quorum_active = False
                # Optionally spawn or request a new replica to start here.
            else:
                self.logger.info(f"Quorum restored with {count} replicas. Accepting registrations.")
                self.quorum_active = True

    def configure(self, args):
        self.logger.info("DiscoveryMW::configure - Setting up Discovery Service")
        bind_str = f"tcp://{args.addr}:{args.port}"
        self.rep_socket.bind(bind_str)
        self.poller.register(self.rep_socket, zmq.POLLIN)
        self.logger.info(f"DiscoveryMW:: Listening on {bind_str}")

    def event_loop(self):
        """Process incoming requests continuously."""
        self.logger.info("DiscoveryMW::event_loop - Running")
        while True:
            events = dict(self.poller.poll(timeout=1000))
            if self.rep_socket in events:
                self.handle_request()

    def load_state(self):
        """Load the current state (publishers, subscribers, broker) from the state node."""
        try:
            self.logger.info(" [DEBUG] DiscoveryMW::load_state - Loading state from ZooKeeper")
            if self.zkclient_obj.zk.exists(self.state_path):
                data, stat = self.zkclient_obj.zk.get(self.state_path)
                state = json.loads(data.decode('utf-8'))
                self.publishers = state.get("publishers", {})
                self.subscribers = state.get("subscribers", {})
                self.broker = state.get("broker", None)
                self.logger.info("State loaded from ZooKeeper state node")
            else:
                self.update_state()  # Create the state node with the current (likely empty) state.
        except Exception as e:
            self.logger.info(f"[DEBUG] Error loading state: {e}")

    def update_state(self):
        """Write the current state into the state node."""
        try:
            self.logger.info(" [DEBUG] DiscoveryMW::update_state - Updating state in ZooKeeper")
            state = {
                "publishers": self.publishers,
                "subscribers": self.subscribers,
                "broker": self.broker
            }
            state_str = json.dumps(state)
            if self.zkclient_obj.zk.exists(self.state_path):
                self.zkclient_obj.zk.set(self.state_path, state_str.encode('utf-8'))
            else:
                self.zkclient_obj.zk.create(self.state_path, state_str.encode('utf-8'), makepath=True)
            self.logger.info("State updated in ZooKeeper")
        except Exception as e:
            self.logger.error(f"Error updating state: {e}")

    def handle_publisher_change(self, current_children):
        known_pub_ids = set(self.publishers.keys())
        active_pub_ids = set(current_children)
        for pub_id in known_pub_ids - active_pub_ids:
            self.logger.info(f"DiscoveryMW::handle_publisher_change - Publisher {pub_id} removed")
            del self.publishers[pub_id]
        self.update_state()

    def handle_subscribers_change(self, current_children):
        known_sub_ids = set(self.subscribers.keys())
        active_sub_ids = set(current_children)
        for sub_id in known_sub_ids - active_sub_ids:
            self.logger.info(f"DiscoveryMW::handle_subscribers_change - Subscriber {sub_id} removed")
            del self.subscribers[sub_id]
        self.update_state()

    def handle_request(self):
        try:
            self.logger.info("DiscoveryMW::handle_request - Processing request")
            request_bytes = self.rep_socket.recv()
            discovery_request = discovery_pb2.DiscoveryReq()
            discovery_request.ParseFromString(request_bytes)

            if discovery_request.msg_type == discovery_pb2.TYPE_REGISTER:
                self.logger.info("DiscoveryMW::handle_request - Processing Registration")
                response = self.process_registration(discovery_request.register_req)
            elif discovery_request.msg_type == discovery_pb2.TYPE_ISREADY:
                self.logger.info("DiscoveryMW::handle_request - Processing Readiness Check")
                response = self.create_register_response(True)
            elif discovery_request.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                self.logger.info("DiscoveryMW::handle_request - Processing Topic Lookup")
                response = self.process_lookup_request(discovery_request.lookup_req)
            elif discovery_request.msg_type == discovery_pb2.TYPE_LOOKUP_BROKER:
                self.logger.info("DiscoveryMW::handle_request - Processing Broker Lookup")
                response = self.process_lookup_broker()
            elif discovery_request.msg_type == discovery_pb2.TYPE_REGISTER_BROKER:
                self.logger.info("DiscoveryMW::handle_request - Processing Broker Registration")
                response = self.process_register_broker(discovery_request.register_broker_req)
            else:
                self.logger.error("DiscoveryMW::handle_request - Unknown request type")
                response = self.create_error_response("Unknown request type")

            # Send the response back
            self.rep_socket.send(response.SerializeToString())
        except Exception as e:
            self.logger.error(f"DiscoveryMW::handle_request - Exception: {e}")
            error_resp = self.create_error_response(str(e))
            self.rep_socket.send(error_resp.SerializeToString())

    def process_registration(self, register_req):
        """Process a publisher or subscriber registration. Reject if quorum not active."""
        if not self.quorum_active:
            self.logger.info("DiscoveryMW::process_registration - Quorum not active. Rejecting registration.")
            return self.create_register_response(False)

        self.logger.info("DiscoveryMW::process_registration - Storing registration info")
        entity_id = register_req.info.id
        role = register_req.role
        topics = list(register_req.topiclist)
        if role == discovery_pb2.ROLE_PUBLISHER:
            self.publishers[entity_id] = {
                "addr": register_req.info.addr,
                "port": register_req.info.port,
                "topics": topics
            }
            self.logger.info(f"Registered Publisher: {entity_id}")
            self.registered_pubs += 1
        elif role == discovery_pb2.ROLE_SUBSCRIBER:
            self.subscribers[entity_id] = {
                "topics": topics
            }
            self.logger.info(f"Registered Subscriber: {entity_id}")
            self.registered_subs += 1
        else:
            self.logger.error("DiscoveryMW::process_registration - Unknown role")
            return self.create_register_response(False)
        self.update_state()
        return self.create_register_response(True)

    def process_register_broker(self, register_broker_req):
        if not self.quorum_active:
            self.logger.info("DiscoveryMW::process_register_broker - Quorum not active. Rejecting broker registration.")
            return self.create_register_response(False)

        self.logger.info("DiscoveryMW::process_register_broker - Storing Broker info")
        # Remove the old broker if any
        if self.broker is not None:
            self.logger.info("Removing old broker from registry.")
            self.broker = None

        self.broker = {
            "id": register_broker_req.broker.id,
            "addr": register_broker_req.broker.addr,
            "front_port": register_broker_req.broker.front_port,
            "back_port": register_broker_req.broker.back_port,
        }
        self.logger.info(f"Registered Broker: {self.broker}")
        self.update_state()
        return self.create_register_response(True)


    def process_lookup_request(self, lookup_req):
        self.logger.info("DiscoveryMW::process_lookup_request - Looking up publishers")
        response = discovery_pb2.DiscoveryResp()
        response.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
        lookup_resp = discovery_pb2.LookupPubByTopicResp()
        lookup_resp.status = True
        for pub_id, pub_data in self.publishers.items():
            if len(lookup_req.topiclist) == 0 or any(topic in pub_data["topics"] for topic in lookup_req.topiclist):
                pub_info = lookup_resp.publishers.add()
                pub_info.id = pub_id
                pub_info.addr = pub_data["addr"]
                pub_info.port = pub_data["port"]
                pub_info.topics.extend(pub_data["topics"])
        response.lookup_resp.CopyFrom(lookup_resp)
        return response

    def process_lookup_broker(self):
        self.logger.info("DiscoveryMW::process_lookup_broker - Returning Broker info")
        response = discovery_pb2.DiscoveryResp()
        response.msg_type = discovery_pb2.TYPE_LOOKUP_BROKER
        if self.broker:
            response.lookup_resp_broker.broker.id = self.broker["id"]
            response.lookup_resp_broker.broker.addr = self.broker["addr"]
            response.lookup_resp_broker.broker.front_port = self.broker["front_port"]
            response.lookup_resp_broker.broker.back_port = self.broker["back_port"]
            self.logger.info(f"Sending Broker info: {self.broker}")
        else:
            self.logger.warning("DiscoveryMW::process_lookup_broker - No Broker registered!")
            response.lookup_resp_broker.broker.id = "NONE"
            response.lookup_resp_broker.broker.addr = ""
            response.lookup_resp_broker.broker.front_port = 6000
            response.lookup_resp_broker.broker.back_port = 6001
        return response

    def create_register_response(self, success):
        self.logger.info("DiscoveryMW::create_register_response - Creating response")
        response = discovery_pb2.DiscoveryResp()
        response.msg_type = discovery_pb2.TYPE_REGISTER
        response.register_resp.status = (
            discovery_pb2.STATUS_SUCCESS if success else discovery_pb2.STATUS_FAILURE
        )
        return response

    def create_error_response(self, error_message):
        self.logger.error(f"DiscoveryMW::create_error_response - {error_message}")
        response = discovery_pb2.DiscoveryResp()
        response.msg_type = discovery_pb2.TYPE_UNKNOWN
        return response
