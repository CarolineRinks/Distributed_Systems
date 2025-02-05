import zmq
import logging
from CS6381_MW import discovery_pb2

class DiscoveryMW:
    """ Middleware for Discovery Service """
    def __init__(self, logger):
        """ Constructor """
        self.logger = logger  # Logger for debugging
        self.rep = None  # ZMQ REP socket to handle incoming requests
        self.poller = None  # ZMQ Poller for handling events
        self.expected_pubs = 0  # Expected number of publishers
        self.expected_subs = 0  # Expected number of subscribers
        self.registered_pubs = 0  # Counter for registered publishers
        self.registered_subs = 0  # Counter for registered subscribers
        self.publishers = {}  # Dictionary to store registered publishers
        self.subscribers = {}  # Dictionary to store registered subscribers
        self.handle_events = True  # Controls the event loop

    def configure(self, args):
        """ Configure ZMQ sockets and initialize the Discovery service """
        try:
            self.logger.info("DiscoveryMW::configure")
            # Set expected number of publishers and subscribers from arguments
            self.expected_pubs = args.num_pubs
            self.expected_subs = args.num_subs
            # Set up ZMQ context
            context = zmq.Context()
            # Create a REP socket (used for handling registration requests)
            self.rep = context.socket(zmq.REP)
            # Bind the REP socket to listen on the specified port
            bind_str = f"tcp://*:{args.port}"
            self.rep.bind(bind_str)
            # Register the REP socket with a poller
            self.poller = zmq.Poller()
            self.poller.register(self.rep, zmq.POLLIN)
            self.logger.info(f"DiscoveryMW:: Listening on {bind_str}")
            self.logger.info(f"Expecting {self.expected_pubs} publishers and {self.expected_subs} subscribers")
        except Exception as e:
            raise e
        
    def event_loop(self, timeout=None):
        """ Main event loop to handle incoming requests """
        try:
            self.logger.info("DiscoveryMW::event_loop - Running")
            while self.handle_events:
                # Poll for incoming requests
                events = dict(self.poller.poll(timeout=timeout))
                if self.rep in events:
                    self.handle_request()
            self.logger.info("DiscoveryMW::event_loop - Exiting")
        except Exception as e:
            raise e
        
    def handle_request(self):
        """ Process incoming registration and readiness requests """
        try:
            self.logger.info("DiscoveryMW::handle_request - Processing request")
            # Receive the request from the publisher or subscriber
            request_bytes = self.rep.recv()
            # Deserialize the request using Protobuf
            discovery_request = discovery_pb2.DiscoveryReq()
            discovery_request.ParseFromString(request_bytes)
            # Determine the request type
            if discovery_request.msg_type == discovery_pb2.TYPE_REGISTER:
                self.logger.info("DiscoveryMW::handle_request - Processing registration")
                response = self.process_registration(discovery_request.register_req)
            elif discovery_request.msg_type == discovery_pb2.TYPE_ISREADY:
                self.logger.info("DiscoveryMW::handle_request - Processing readiness check")
                response = self.process_is_ready()
            else:
                self.logger.error("DiscoveryMW::handle_request - Unknown request type received")
                response = self.create_error_response("Unknown request type")
            # Send response back to the requester
            self.rep.send(response.SerializeToString())
        except Exception as e:
            raise e
        
    def process_registration(self, register_req):
        """ Process registration from publishers or subscribers """
        try:
            self.logger.info("DiscoveryMW::process_registration - Storing registration info")
            # Extract entity details
            entity_id = register_req.info.id
            role = register_req.role
            entity_topics = list(register_req.topiclist)
            if role == discovery_pb2.ROLE_PUBLISHER:
                if entity_id in self.publishers:
                    return self.create_register_response(False, "Publisher already registered")
                # Store publisher details
                self.publishers[entity_id] = entity_topics
                self.registered_pubs += 1
            elif role == discovery_pb2.ROLE_SUBSCRIBER:
                if entity_id in self.subscribers:
                    return self.create_register_response(False, "Subscriber already registered")
                # Store subscriber details
                self.subscribers[entity_id] = entity_topics
                self.registered_subs += 1
            self.logger.info(f"Registered {entity_id} as {'Publisher' if role == discovery_pb2.ROLE_PUBLISHER else 'Subscriber'}")
            # Return success response
            return self.create_register_response(True)
        except Exception as e:
            raise e
        
    def process_is_ready(self):
            """ Check if all expected publishers and subscribers are registered """
            self.logger.info("DiscoveryMW::process_is_ready - Checking if system is ready")
            # Check if all expected publishers and subscribers have registered
            ready = (self.registered_pubs >= self.expected_pubs) and (self.registered_subs >= self.expected_subs)
            if ready:
                self.logger.info("DiscoveryMW::process_is_ready - System is READY")
            else:
                self.logger.info(f"DiscoveryMW::process_is_ready - Not ready yet (Registered: {self.registered_pubs}/{self.expected_pubs} pubs, {self.registered_subs}/{self.expected_subs} subs)")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.TYPE_ISREADY
            response.isready_resp.status = ready
            return response
    
    def create_register_response(self, success, reason=""):
        """ Create a registration response message """
        self.logger.info("DiscoveryMW::create_register_response - Creating response")
        response = discovery_pb2.DiscoveryResp()
        response.msg_type = discovery_pb2.TYPE_REGISTER
        response.register_resp.status = discovery_pb2.STATUS_SUCCESS if success else discovery_pb2.STATUS_FAILURE
        response.register_resp.reason = reason
        return response
    
    def create_error_response(self, error_message):
        """ Create an error response message """
        self.logger.error(f"DiscoveryMW::create_error_response - {error_message}")
        response = discovery_pb2.DiscoveryResp()
        response.msg_type = discovery_pb2.TYPE_UNKNOWN
        return response
    
    def disable_event_loop(self):
        """ Stop the event loop """
        self.handle_events = False
        