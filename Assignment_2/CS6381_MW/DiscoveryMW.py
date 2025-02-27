import zmq
import logging
import configparser
from CS6381_MW import discovery_pb2
from .zkclient import ZK_Driver
from .zk_discovery_client import SequentialLeaderElection


class DiscoveryMW:
    """Middleware for the Discovery Service."""

    def __init__(self, logger):
        """Constructor: initialize context, REP socket, poller, and read config."""
        self.logger = logger
        self.context = zmq.Context()
        self.rep_socket = self.context.socket(zmq.REP)  # Socket to reply to requests
        self.poller = zmq.Poller()

        self.expected_pubs = 0   # Expected number of publishers
        self.expected_subs = 0   # Expected number of subscribers
        self.registered_pubs = 0 # Count of registered publishers
        self.registered_subs = 0 # Count of registered subscribers

        # Read configuration from config.ini
        config_obj = configparser.ConfigParser()
        config_obj.read('config.ini')
        self.discovery_ip = config_obj['Settings']['discovery_ip']
        self.discovery_port = config_obj['Settings']['discovery_port']

        # Dictionaries to track registered entities
        self.publishers = {}   # Format: { "pub1": {"addr": <IP>, "port": <PORT>, "topics": [t1, t2]} }
        self.subscribers = {}  # Format: { "sub1": {"topics": [t1, t2]} }
        self.broker = None     # Will hold broker information when registered
        self.pub_children = [] # List of active publishers

        self.zkclient_obj = ZK_Driver()
        self.zkclient_obj.init_driver()
        self.zkclient_obj.start_session()

        # zk discovery client object
        self.zk_discovery_obj = SequentialLeaderElection(self.zkclient_obj.zk, self.discovery_ip, self.discovery_port)

        @self.zkclient_obj.zk.ChildrenWatch("/root/pubs")
        def watch_publishers(children):
            # 'children' is the current list of child znodes (i.e., active publishers).
            # Compare it to your internal list to see which znode(s) disappeared or appeared.
            self.handle_publisher_change(children)

        @self.zkclient_obj.zk.ChildrenWatch("/root/subs")
        def watch_subscribers(children):
            # 'children' is the current list of child znodes (i.e., active subscribers).
            # Compare it to your internal list to see which znode(s) disappeared or appeared.
            self.handle_subscribers_change(children)

    def configure(self, args):
        """Configure the Discovery Service using command-line arguments."""
        # self.expected_pubs = args.num_pubs
        # self.expected_subs = args.num_subs
        self.logger.info("DiscoveryMW::configure - Setting up Discovery Service")
        # Bind REP socket so that the service listens on the configured IP and port.
        bind_str = f"tcp://{self.discovery_ip}:{self.discovery_port}"
        self.rep_socket.bind(bind_str)
        self.poller.register(self.rep_socket, zmq.POLLIN)
        self.logger.info(f"DiscoveryMW:: Listening on {bind_str}")

        # After configuring, create the znode for this Discovery replica
        self.zk_discovery_obj.start()

        
    def handle_publisher_change(self,current_children):
        # Suppose you track known publishers in a dict: self.publishers = {pub_id: {...}, ...}
        known_pub_ids = set(self.publishers.keys())
        active_pub_ids = set(current_children)  # from the ephemeral znode names

        # Find the difference
        removed_pub_ids = known_pub_ids - active_pub_ids
        for pub_id in removed_pub_ids:
            self.logger.info(f"Publisher {pub_id} is no longer active. Removing from internal state.")
            del self.publishers[pub_id]
            # Optionally notify subscribers or update any relevant state
    
    
    def handle_subscribers_change(self,current_children):
        # Suppose you track known publishers in a dict: self.publishers = {pub_id: {...}, ...}
        known_sub_ids = set(self.subscribers.keys())
        active_sub_ids = set(current_children)  # from the ephemeral znode names


        # Find the difference
        removed_sub_ids = known_sub_ids - active_sub_ids
        for sub_id in removed_sub_ids:
            self.logger.info(f"Subscriber {sub_id} is no longer active. Removing from internal state.")
            del self.subscribers[sub_id]
            # Optionally notify subscribers or update any relevant state
    

    def event_loop(self):
        """Event loop: waits for and processes incoming requests."""
        self.logger.info("DiscoveryMW::event_loop - Running")
        while True:
            events = dict(self.poller.poll())
            if self.rep_socket in events:
                self.handle_request()

    def handle_request(self):
        """Handle an incoming request from a publisher, subscriber, or broker."""
        try:
            self.logger.info("DiscoveryMW::handle_request - Processing request")
            request_bytes = self.rep_socket.recv()
            discovery_request = discovery_pb2.DiscoveryReq()
            discovery_request.ParseFromString(request_bytes)

            # Process the request based on its type.
            if discovery_request.msg_type == discovery_pb2.TYPE_REGISTER:
                self.logger.info("DiscoveryMW::handle_request - Processing Registration")
                response = self.process_registration(discovery_request.register_req)
            elif discovery_request.msg_type == discovery_pb2.TYPE_ISREADY:
                self.logger.info("DiscoveryMW::handle_request - Processing Readiness Check")
                response = self.process_is_ready()
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
                self.logger.error("DiscoveryMW::handle_request - Unknown request type received")
                response = self.create_error_response("Unknown request type")
            
            self.rep_socket.send(response.SerializeToString())
        except Exception as e:
            self.logger.error(f"DiscoveryMW::handle_request - Exception: {e}")
            error_resp = self.create_error_response(str(e))
            self.rep_socket.send(error_resp.SerializeToString())

    def process_registration(self, register_req):
        """Store registration info for a publisher or subscriber."""
        try:
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
                self.logger.info(f"Registered Publisher: {entity_id} with topics {topics}")
                self.registered_pubs += 1


            elif role == discovery_pb2.ROLE_SUBSCRIBER: 
                self.subscribers[entity_id] = {
                    "topics": topics
                }
                self.logger.info(f"Registered Subscriber: {entity_id} with topics {topics}")
                self.registered_subs += 1
            else:
                self.logger.error("DiscoveryMW::process_registration - Unknown role in registration")
                return self.create_register_response(False)

            return self.create_register_response(True)
        except Exception as e:
            self.logger.error(f"DiscoveryMW::process_registration - Exception: {e}")
            raise e

    def process_register_broker(self, register_broker_req):
        """Store broker information."""
        try:
            self.logger.info("DiscoveryMW::process_register_broker - Storing Broker info")
            self.broker = {
                "id": register_broker_req.broker.id,
                "addr": register_broker_req.broker.addr,
                "front_port": register_broker_req.broker.front_port,
                "back_port": register_broker_req.broker.back_port,
            }
            self.logger.info(f"Registered Broker: {self.broker}")

            return self.create_register_response(True)
        except Exception as e:
            self.logger.error(f"DiscoveryMW::process_register_broker - Exception: {e}")
            raise e

    def process_lookup_request(self, lookup_req):
        """Return a list of publishers that publish at least one requested topic.
        If the topic list is empty, return all publishers."""
        try:
            self.logger.info("DiscoveryMW::process_lookup_request - Finding matching publishers")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            # Create a LookupPubByTopicResp message and explicitly set status to True.
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
        except Exception as e:
            self.logger.error(f"DiscoveryMW::process_lookup_request - Exception: {e}")
            raise e
    
    # def update_subscribers(self,entity_id,topics):
    #     """Return a list of publishers that publish at least one requested topic.
    #     If the topic list is empty, return all publishers."""
    #     try:
    #         self.logger.info("DiscoveryMW:: updating Subscribers - Finding matching publishers")
    #         response = discovery_pb2.DiscoveryResp()
    #         response.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
    #         # Create a LookupPubByTopicResp message and explicitly set status to True.
    #         lookup_resp = discovery_pb2.LookupPubByTopicResp()
    #         lookup_resp.status = True

    #         for sub_id, sub_data in self.subscribers.items():
    #             sub_addr = 
    #             if any(topic in topics for topic in sub_data["topics"]):
    #                 pub_info = lookup_resp.publishers.add()
    #                 pub_info.id = entity_id
    #                 pub_info.addr = self.publishers[entity_id]["addr"]
    #                 pub_info.port = self.publishers[entity_id]["port"]
    #                 pub_info.topics.extend(topics)
    #         response.lookup_resp.CopyFrom(lookup_resp)
    #         return response

    #     except Exception as e:
    #         self.logger.error(f"DiscoveryMW:: Update Subscribers - Exception: {e}")
    #         raise e


    def process_lookup_broker(self):
        """Return the broker information if a broker has been registered."""
        try:
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
                response.lookup_resp_broker.broker.front_port=6000
                response.lookup_resp_broker.broker.back_port=6001
            return response
        except Exception as e:
            self.logger.error(f"DiscoveryMW::process_lookup_broker - Exception: {e}")
            raise e

    # def process_is_ready(self):
    #     """Check if the expected number of publishers and subscribers are registered."""
    #     try:
    #         self.logger.info("DiscoveryMW::process_is_ready - Checking if system is ready")
    #         ready = (self.registered_pubs >= self.expected_pubs) and (self.registered_subs >= self.expected_subs)
    #         if ready:
    #             self.logger.info("DiscoveryMW::process_is_ready - System is READY")
    #         else:
    #             self.logger.info(f"DiscoveryMW::process_is_ready - Not ready yet (Registered: {self.registered_pubs}/{self.expected_pubs} pubs, {self.registered_subs}/{self.expected_subs} subs)")
    #         response = discovery_pb2.DiscoveryResp()
    #         response.msg_type = discovery_pb2.TYPE_ISREADY
    #         response.isready_resp.status = ready
    #         return response
    #     except Exception as e:
    #         self.logger.error(f"DiscoveryMW::process_is_ready - Exception: {e}")
    #         raise e

    def create_register_response(self, success):
        """Create a registration response message."""
        self.logger.info("DiscoveryMW::create_register_response - Creating response")
        response = discovery_pb2.DiscoveryResp()
        response.msg_type = discovery_pb2.TYPE_REGISTER
        response.register_resp.status = discovery_pb2.STATUS_SUCCESS if success else discovery_pb2.STATUS_FAILURE
        return response

    def create_error_response(self, error_message):
        """Create an error response message."""
        self.logger.error(f"DiscoveryMW::create_error_response - {error_message}")
        response = discovery_pb2.DiscoveryResp()
        response.msg_type = discovery_pb2.TYPE_UNKNOWN
        return response
