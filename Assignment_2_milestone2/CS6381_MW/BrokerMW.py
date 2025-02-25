import zmq
import logging
import configparser
import time
from CS6381_MW import discovery_pb2
from .zkclient import ZK_Driver

class BrokerMW:
    """Middleware for the broker (ViaBroker mode). This implementation forwards every
    message received on its frontend socket. Subscribers apply their own topic filters via
    ZMQ's native subscription mechanism.
    It now also connects to the current primary discovery service by reading and watching the
    /root/discovery/primary znode.
    """

    def __init__(self, logger):
        """Constructor."""
        self.logger = logger
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.discovery_req = None
        self.discovery_primary_endpoint = None  # To hold the current discovery primary endpoint
        self.zk_obj = ZK_Driver()
        self.zk_obj.init_driver() 
        self.zk_obj.start_session()

        # (Optional: if you want to watch for publishers/subscribers changes, you can add those watches.)

    def configure(self, broker_id, broker_ip, broker_front_end_port, broker_back_end_port):
        """Configure the broker using parameters from the config file."""
        self.logger.info("BrokerMW::configure - Setting up Broker")

        # Create and connect a REQ socket to the Discovery service.
        self.discovery_req = self.context.socket(zmq.REQ)
        try:
            # Retrieve the primary discovery pointer from ZooKeeper.
            primary_data, _ = self.zk_obj.zk.get("/root/discovery/primary")
            primary_str = primary_data.decode('utf-8')
            new_connect_str = f"tcp://{primary_str}"
            self.discovery_primary_endpoint = new_connect_str
            self.discovery_req.connect(new_connect_str)
            self.logger.info("BrokerMW::configure - Connected to Discovery at " + new_connect_str)
        except Exception as e:
            self.logger.error("BrokerMW::configure - error retrieving primary discovery pointer: " + str(e))
            # raise e

        # Set a watch on the primary discovery node so that if it changes, we reconnect.
        @self.zk_obj.zk.DataWatch("/root/discovery/primary")
        def watch_primary(data, stat, event):
            if data:
                new_primary = data.decode('utf-8')
                new_connect_str = f"tcp://{new_primary}"
                if new_connect_str != self.discovery_primary_endpoint:
                    self.logger.info("BrokerMW::watch_primary - primary updated to " + new_connect_str)
                    if self.discovery_primary_endpoint:
                        try:
                            self.discovery_req.disconnect(self.discovery_primary_endpoint)
                        except Exception as ex:
                            self.logger.error("BrokerMW::watch_primary - error disconnecting: " + str(ex))
                    self.discovery_req.connect(new_connect_str)
                    self.discovery_primary_endpoint = new_connect_str

        # Create the frontend (SUB) socket for receiving publisher messages and backend (PUB) socket for sending to subscribers.
        self.frontend = self.context.socket(zmq.SUB)
        self.backend = self.context.socket(zmq.PUB)

        # Read broker connection parameters.
        self.broker_ip = broker_ip
        self.frontend_port = broker_front_end_port  # For publishers to connect.
        self.backend_port = broker_back_end_port      # For subscribers to connect.

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
        from CS6381_MW import discovery_pb2  # Ensure proper import if needed
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


    def event_loop(self):
        """Main event loop to forward publisher messages to subscribers."""
        self.logger.info("BrokerMW::event_loop - Waiting for Discovery to be ready")
        self.logger.info("BrokerMW::event_loop - Running broker loop")
        while True:
            events = dict(self.poller.poll())
            if self.frontend in events:
                message = self.frontend.recv_string()
                self.logger.info("BrokerMW::event_loop - Received message: " + message)
                # Forward the message unconditionally.
                self.backend.send_string(message)
                self.logger.info("BrokerMW::event_loop - Forwarded message to subscribers")
