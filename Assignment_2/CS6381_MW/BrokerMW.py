import zmq
import logging
import configparser
import time
from CS6381_MW import discovery_pb2
from .zkclient import ZK_Driver

class BrokerMW:
    """Middleware for the broker (ViaBroker mode). This implementation forwards every
    message received on its frontend socket. Subscribers apply their own topic filters via
    ZMQ's native subscription mechanism."""

    def __init__(self, logger):
        """Constructor."""
        self.logger = logger
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.discovery_req = None
        self.zk_obj = ZK_Driver()
        self.zk_obj.init_driver() 
        self.zk_obj.start_session()

        # @self.zk_obj.zk.ChildrenWatch("/root/pubs")
        # def watch_publishers(children):
        #     self.logger.info("Zookeeper Watch: Detected change in /root/pubs")
        #     if self.discovery_req is not None:  # Ensure self.req is initialized before calling lookup
        #         self.event_loop()
        #     else:
        #         self.logger.warning("Skipping lookup: self.discovery_req is not initialized yet.")

        # @self.zk_obj.zk.ChildrenWatch("/root/subs")
        # def watch_subscribers(children):
        #     self.logger.info("Zookeeper Watch: Detected change in /root/subs")
        #     if self.discovery_req is not None:  # Ensure self.req is initialized before calling lookup
        #         self.event_loop()
        #     else:
        #         self.logger.warning("Skipping lookup: self.discovery_req is not initialized yet.")

    def configure(self, config):
        """Configure the broker using parameters from the config file."""
        self.logger.info("BrokerMW::configure - Setting up Broker")

        # Create and connect a REQ socket to the Discovery service.
        self.discovery_req = self.context.socket(zmq.REQ)
        self.discovery_req.connect(f"tcp://{config['Settings']['discovery_addr']}")
        self.logger.info(f"BrokerMW::configure - Connected to Discovery at {config['Settings']['discovery_addr']}")

        # Create the frontend (SUB) socket for receiving publisher messages and backend (PUB) socket for sending to subscribers.
        self.frontend = self.context.socket(zmq.SUB)
        self.backend = self.context.socket(zmq.PUB)

        # Read broker connection parameters.
        self.broker_ip = config['Settings']['broker_ip']
        self.frontend_port = config['Settings']['frontend_port']  # For publisher messages.
        self.backend_port = config['Settings']['backend_port']    # For subscriber messages.

        # Bind the frontend socket and subscribe to all topics.
        self.frontend.bind(f"tcp://{self.broker_ip}:{self.frontend_port}")
        self.frontend.setsockopt_string(zmq.SUBSCRIBE, "")
        # Bind the backend socket.
        self.backend.bind(f"tcp://{self.broker_ip}:{self.backend_port}")

        # Register the frontend socket with the poller.
        self.poller.register(self.frontend, zmq.POLLIN)

        self.logger.info("BrokerMW::configure completed")

    def register_broker(self, broker_id, addr, broker_front_end_port, broker_back_end_port):
        """Register the broker with the Discovery service."""
        broker_info = discovery_pb2.BrokerInfo()
        broker_info.id = broker_id
        broker_info.addr = addr
        broker_info.front_port = int(broker_front_end_port)
        broker_info.back_port = int(broker_back_end_port)
        
        register_broker_req = discovery_pb2.RegisterBrokerReq()
        register_broker_req.broker.CopyFrom(broker_info)
        
        disc_req = discovery_pb2.DiscoveryReq()
        disc_req.msg_type = discovery_pb2.TYPE_REGISTER_BROKER
        disc_req.register_broker_req.CopyFrom(register_broker_req)
        
        self.discovery_req.send(disc_req.SerializeToString())
        self.logger.info("BrokerMW::register_broker - Broker registration sent to Discovery.")
        # Receive the reply to clear the REQ socket.
        reply = self.discovery_req.recv()
        self.logger.info("BrokerMW::register_broker - Received registration reply.")

    # def wait_for_discovery_ready(self):
    #     """Wait until the Discovery Service confirms readiness."""
    #     self.logger.info("BrokerMW::wait_for_discovery_ready - Checking Discovery status")
    #     while True:
    #         req_msg = discovery_pb2.DiscoveryReq()
    #         req_msg.msg_type = discovery_pb2.TYPE_ISREADY
    #         self.discovery_req.send(req_msg.SerializeToString())
    #         resp_bytes = self.discovery_req.recv()
    #         resp_msg = discovery_pb2.DiscoveryResp()
    #         resp_msg.ParseFromString(resp_bytes)
    #         if resp_msg.isready_resp.status:
    #             self.logger.info("BrokerMW::wait_for_discovery_ready - Discovery is READY")
    #             break
    #         else:
    #             self.logger.info("BrokerMW::wait_for_discovery_ready - Not ready, retrying...")
    #             time.sleep(1)

    def fetch_publishers(self):
        """Fetch publisher information from Discovery (optional for filtering)."""
        self.logger.info("BrokerMW::fetch_publishers - Retrieving publishers from Discovery")
        req_msg = discovery_pb2.DiscoveryReq()
        req_msg.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
        lookup_req = discovery_pb2.LookupPubByTopicReq()
        lookup_req.topiclist[:] = []  # empty list returns all publishers
        req_msg.lookup_req.CopyFrom(lookup_req)
        self.discovery_req.send(req_msg.SerializeToString())
        resp_bytes = self.discovery_req.recv()
        resp_msg = discovery_pb2.DiscoveryResp()
        resp_msg.ParseFromString(resp_bytes)
        publisher_list = resp_msg.lookup_resp.publishers
        self.logger.info(f"BrokerMW::fetch_publishers - Publishers: {[pub.id for pub in publisher_list]}")

    def event_loop(self):
        """Main event loop to forward publisher messages to subscribers."""
        self.logger.info("BrokerMW::event_loop - Waiting for Discovery to be ready")
        
        # self.wait_for_discovery_ready()
        self.logger.info("BrokerMW::event_loop - Fetching registered publishers")
        self.fetch_publishers()
        self.logger.info("BrokerMW::event_loop - Running broker loop")
        while True:
            events = dict(self.poller.poll())
            if self.frontend in events:
                message = self.frontend.recv_string()
                self.logger.info(f"BrokerMW::event_loop - Received message: {message}")
                # Forward the message unconditionally.
                self.backend.send_string(message)
                self.logger.info("BrokerMW::event_loop - Forwarded message to subscribers")
