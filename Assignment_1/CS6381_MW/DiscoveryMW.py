import zmq
import logging
import configparser
from CS6381_MW import discovery_pb2

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

    def configure(self, args):
        """Configure the Discovery Service using command-line arguments."""
        self.expected_pubs = args.num_pubs
        self.expected_subs = args.num_subs
        self.logger.info("DiscoveryMW::configure - Setting up Discovery Service")
        # Bind REP socket so that the service listens on the configured IP and port.
        bind_str = f"tcp://{self.discovery_ip}:{self.discovery_port}"
        self.rep_socket.bind(bind_str)
        self.poller.register(self.rep_socket, zmq.POLLIN)
        self.logger.info(f"DiscoveryMW:: Listening on {bind_str}")

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

    def process_is_ready(self):
        """Check if the expected number of publishers and subscribers are registered."""
        try:
            self.logger.info("DiscoveryMW::process_is_ready - Checking if system is ready")
            ready = (self.registered_pubs >= self.expected_pubs) and (self.registered_subs >= self.expected_subs)
            if ready:
                self.logger.info("DiscoveryMW::process_is_ready - System is READY")
            else:
                self.logger.info(f"DiscoveryMW::process_is_ready - Not ready yet (Registered: {self.registered_pubs}/{self.expected_pubs} pubs, {self.registered_subs}/{self.expected_subs} subs)")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.TYPE_ISREADY
            response.isready_resp.status = ready
            return response
        except Exception as e:
            self.logger.error(f"DiscoveryMW::process_is_ready - Exception: {e}")
            raise e

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
