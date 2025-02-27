# broker_mw.py
import zmq
import logging
import time
from CS6381_MW import discovery_pb2
from .zkclient import ZK_Driver
from .zk_broker_client import BrokerLeaderElection

class BrokerMW:
    """Middleware for the broker with warm-passive replication and quorum logic."""

    def __init__(self, logger, args):
        self.logger = logger
        self.context = zmq.Context()
        self.poller = zmq.Poller()

        self.discovery_req = None
        self.discovery_primary_endpoint = None
        self.quorum_active = True   # If < 3 broker replicas, block dissemination

        self.broker_ip = args.addr
        self.frontend_port = args.pub_port
        self.backend_port = args.sub_port

        
        self.zk_obj = ZK_Driver()
        self.zk_obj.init_driver() 
        self.zk_obj.start_session()

        self.configure(self.broker_ip, self.frontend_port, self.backend_port)

        # Start ephemeral-sequential election for Broker
        self.broker_election = BrokerLeaderElection(
            self.zk_obj,
            self,  # pass self so that BrokerLeaderElection can call broker_mw.register_as_leader()
            ip=self.broker_ip,
            front_port=self.frontend_port,
            back_port=self.backend_port,
            election_path="/root/broker/replicas",
            primary_path="/root/broker/primary",
            lease_path="/root/broker/lease",
            lease_duration=30
        )
        self.broker_election.start()

        # Watch ephemeral-sequential broker replicas to maintain quorum
        @self.zk_obj.zk.ChildrenWatch("/root/broker/replicas")
        def watch_broker_replicas(children):
            count = len(children)
            if count < 3:
                self.logger.warning(f"Broker Quorum lost: only {count} replica(s). Stopping dissemination.")
                self.quorum_active = False
            else:
                self.logger.info(f"Broker Quorum restored with {count} replicas. Resuming dissemination.")
                self.quorum_active = True

    def configure(self, broker_ip, broker_front_end_port, broker_back_end_port):
        """Configure the broker and connect to Discovery."""
        self.logger.info("BrokerMW::configure - Setting up Broker")

        # Create REQ socket to Discovery
        self.discovery_req = self.context.socket(zmq.REQ)
        try:
            # Retrieve the primary discovery pointer from ZooKeeper
            primary_data, _ = self.zk_obj.zk.get("/root/discovery/primary")
            primary_str = primary_data.decode('utf-8')
            new_connect_str = f"tcp://{primary_str}"
            self.discovery_primary_endpoint = new_connect_str
            self.discovery_req.connect(new_connect_str)
            self.logger.info(f"BrokerMW::configure - Connected to Discovery at {new_connect_str}")
        except Exception as e:
            self.logger.error("BrokerMW::configure - error retrieving primary discovery pointer: " + str(e))

        # Watch for changes to the Discovery primary
        @self.zk_obj.zk.DataWatch("/root/discovery/primary")
        def watch_primary(data, stat, event):
            if data:
                new_primary = data.decode('utf-8')
                new_connect_str = f"tcp://{new_primary}"
                if new_connect_str != self.discovery_primary_endpoint:
                    self.logger.info("BrokerMW::watch_primary - discovery primary updated to " + new_connect_str)
                    if self.discovery_primary_endpoint:
                        try:
                            self.discovery_req.disconnect(self.discovery_primary_endpoint)
                        except Exception as ex:
                            self.logger.error("BrokerMW::watch_primary - error disconnecting: " + str(ex))
                    self.discovery_req.connect(new_connect_str)
                    self.discovery_primary_endpoint = new_connect_str

        # Create the frontend (SUB) socket for receiving publisher messages
        # and backend (PUB) socket for sending to subscribers
        self.frontend = self.context.socket(zmq.SUB)
        self.backend = self.context.socket(zmq.PUB)

        self.frontend.bind(f"tcp://{broker_ip}:{broker_front_end_port}")
        self.frontend.setsockopt_string(zmq.SUBSCRIBE, "")
        self.backend.bind(f"tcp://{broker_ip}:{broker_back_end_port}")

        self.poller.register(self.frontend, zmq.POLLIN)
        self.logger.info("BrokerMW::configure completed")

    def register_as_leader(self):
        """
        Called by BrokerLeaderElection when this broker becomes the new leader.
        Sends a REGISTER_BROKER request to Discovery.
        """
        self.logger.info("BrokerMW::register_as_leader - Registering new leader broker with Discovery.")
        
        # # Lazy initialization: if discovery_req is None, create it.
        # if self.discovery_req is None:
        #     self.discovery_req = self.context.socket(zmq.REQ)
        #     try:
        #         primary_data, _ = self.zk_obj.zk.get("/root/discovery/primary")
        #         primary_str = primary_data.decode('utf-8')
        #         new_connect_str = f"tcp://{primary_str}"
        #         self.discovery_req.connect(new_connect_str)
        #         self.discovery_primary_endpoint = new_connect_str
        #         self.logger.info("BrokerMW::register_as_leader - Created REQ socket and connected to Discovery at " + new_connect_str)
        #     except Exception as e:
        #         self.logger.error("BrokerMW::register_as_leader - error creating discovery REQ socket: " + str(e))
        #         return

        # Build the REGISTER_BROKER message.
        broker_info = discovery_pb2.BrokerInfo()
        broker_info.id = "LeaderBroker"  # or assign a unique ID as needed
        broker_info.addr = self.broker_ip
        broker_info.front_port = int(self.frontend_port)
        broker_info.back_port = int(self.backend_port)

        register_broker_req = discovery_pb2.RegisterBrokerReq()
        register_broker_req.broker.CopyFrom(broker_info)

        disc_req = discovery_pb2.DiscoveryReq()
        disc_req.msg_type = discovery_pb2.TYPE_REGISTER_BROKER
        disc_req.register_broker_req.CopyFrom(register_broker_req)

        # Retry loop: keep trying until registration succeeds.
        attempt = 1
        disc_resp = discovery_pb2.DiscoveryResp()  # initialize an empty response

        while True:
            self.logger.info(f"BrokerMW::register_as_leader - Attempt {attempt} to register broker.")
            self.discovery_req.send(disc_req.SerializeToString())
            reply_bytes = self.discovery_req.recv()
            disc_resp.ParseFromString(reply_bytes)
            
            if disc_resp.register_resp.status == discovery_pb2.STATUS_SUCCESS:
                self.logger.info("BrokerMW::register_as_leader - Broker registration successful!")
                break
            else:
                self.logger.warning("BrokerMW::register_as_leader - Broker registration failed. Retrying...")
                attempt += 1
                time.sleep(5)
        else:
            self.logger.error("BrokerMW::register_as_leader - All broker registration attempts failed.")



    def event_loop(self):
        """Forward publisher messages to subscribers, if quorum is active."""
        self.logger.info("BrokerMW::event_loop - in the broker event loop")
        while True:
            events = dict(self.poller.poll())
            if self.frontend in events:
                # If quorum is not active, skip forwarding
                self.logger.info("BrokerMW::event_loop - Checking quorum status")
                if not self.quorum_active:
                    self.logger.info("BrokerMW::event_loop - Quorum lost, dropping publisher message.")
                    # Read and discard the message
                    self.frontend.recv_string()
                    continue

                # Otherwise, forward the message
                message = self.frontend.recv_string()
                self.logger.info("BrokerMW::event_loop - Received message: " + message)
                self.backend.send_string(message)
                self.logger.info("BrokerMW::event_loop - Forwarded message to subscribers")
