import zmq
import logging
import configparser
from CS6381_MW import discovery_pb2

class BrokerMW:
    """ Middleware for the broker, used in ViaBroker mode with topic matching """

    def __init__(self, logger):
        """ Constructor """
        self.logger = logger
        self.context = zmq.Context()

        # Sockets for communication

        self.poller = zmq.Poller()

        # Read configuration
        # config = configparser.ConfigParser()
        # config.read('config.ini')
        

        # Track topic subscriptions
        self.topic_subscribers = {}  # { "temperature": [sub1, sub2], "humidity": [sub3] }
        self.publisher_topics = {}  # { "pub1": ["temperature", "humidity"] }

    def configure(self,config):
        """ Configure the broker to forward messages from publishers to subscribers """
        self.logger.info("BrokerMW::configure - Setting up Broker")

        self.frontend = self.context.socket(zmq.SUB)  # Receives from publishers
        self.backend = self.context.socket(zmq.PUB)  # Forwards to subscribers
        self.discovery_req = self.context.socket(zmq.REQ)  # Talks to Discovery Service

        self.broker_ip = config['Settings']['broker_ip']
        self.frontend_port = config['Settings']['frontend_port']  # Where publishers connect
        self.backend_port = config['Settings']['backend_port']  # Where subscribers connect
        self.discovery_addr = config['Settings']['discovery_addr']

        # Connect to Discovery Service
        self.discovery_req.connect(f"tcp://{self.discovery_addr}")
        self.logger.info(f"BrokerMW::configure - Connected to Discovery at {self.discovery_addr}")

        # Bind frontend (SUB socket) for publishers
        self.frontend.bind(f"tcp://{self.broker_ip}:{self.frontend_port}")
        self.frontend.setsockopt_string(zmq.SUBSCRIBE, "")  # Subscribe to all topics

        # Bind backend (PUB socket) for subscribers
        self.backend.bind(f"tcp://{self.broker_ip}:{self.backend_port}")

        # Register frontend socket with poller to listen for publisher messages
        self.poller.register(self.frontend, zmq.POLLIN)

    def wait_for_discovery_ready(self):
        """ Wait until Discovery Service confirms readiness """
        self.logger.info("BrokerMW::wait_for_discovery_ready - Checking Discovery status")

        while True:
            # Send isReady request to Discovery
            req_msg = discovery_pb2.DiscoveryReq()
            req_msg.msg_type = discovery_pb2.TYPE_ISREADY
            self.discovery_req.send(req_msg.SerializeToString())

            # Wait for response
            resp_bytes = self.discovery_req.recv()
            resp_msg = discovery_pb2.DiscoveryResp()
            resp_msg.ParseFromString(resp_bytes)

            if resp_msg.isready_resp.status:
                self.logger.info("BrokerMW::wait_for_discovery_ready - Discovery is READY")
                break
            else:
                self.logger.info("BrokerMW::wait_for_discovery_ready - Not ready, retrying...")

    def fetch_publishers(self):
        """ Request publisher information from Discovery Service """
        self.logger.info("BrokerMW::fetch_publishers - Retrieving publishers from Discovery")

        req_msg = discovery_pb2.DiscoveryReq()
        req_msg.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS  # New message type for all publishers

        self.discovery_req.send(req_msg.SerializeToString())
        resp_bytes = self.discovery_req.recv()
        resp_msg = discovery_pb2.DiscoveryResp()
        publisher_list = resp_msg.lookup_resp.publishers  # Extract registered publishers

        for pub in publisher_list:
            for topic in pub.topics:
                if topic not in self.publisher_topics:
                    self.publisher_topics[topic] = []
                self.publisher_topics[topic].append({"id": pub.id, "addr": pub.addr, "port": pub.port})

        self.logger.info(f"BrokerMW::fetch_publishers - Publishers registered: {self.publisher_topics}")

    def register_subscriber(self, sub_id, topic_list):
        """ Register a subscriber's interest in topics """
        self.logger.info(f"BrokerMW::register_subscriber - {sub_id} subscribed to {topic_list}")
        for topic in topic_list:
            if topic not in self.topic_subscribers:
                self.topic_subscribers[topic] = []
            self.topic_subscribers[topic].append(sub_id)  # Store subscriber info

    def event_loop(self):
        """ Main event loop to filter and forward messages from publishers to subscribers """
        self.logger.info("BrokerMW::event_loop - Waiting for Discovery to be ready")
        self.wait_for_discovery_ready()  # Ensure Discovery confirms all pubs/subs are ready

        self.logger.info("BrokerMW::event_loop - Fetching registered publishers")
        self.fetch_publishers()

        self.logger.info("BrokerMW::event_loop - Running broker loop")

        while True:
            events = dict(self.poller.poll())
            if self.frontend in events:
                # Receive a message from a publisher
                message = self.frontend.recv_string()  # Format: "topic:data"
                topic, data = message.split(":", 1)

                # Check if the topic has interested subscribers
                if topic in self.topic_subscribers:
                    self.logger.info(f"Forwarding '{topic}' data to subscribers: {self.topic_subscribers[topic]}")
                    self.backend.send_string(f"{topic}:{data}")  # Forward only to interested subscribers
                else:
                    self.logger.info(f"No subscribers for topic '{topic}', dropping message")
