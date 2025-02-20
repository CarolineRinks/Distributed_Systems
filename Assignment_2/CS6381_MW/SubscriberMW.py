import zmq
import os
import sys
import time
import logging
from CS6381_MW import discovery_pb2
from .zkclient import ZK_Driver


class SubscriberMW:
    def __init__(self, logger, topiclist):
        self.logger = logger
        self.req = None       # REQ socket for Discovery service
        self.sub = None       # SUB socket for receiving messages (from publishers or broker)
        self.poller = None    # ZMQ poller for events
        self.addr = None      # Local address
        self.port = None      # Subscriber’s port
        self.topiclist = topiclist  # Topics of interest
        self.upcall_obj = None      # Upcall handle to application
        self.handle_events = True   # Event loop control
        self.lookup_strategy = None  # Renamed attribute (avoids conflict with lookup() method)
        self.dissemination = None

        self.zk_obj = ZK_Driver()
        self.zk_obj.init_driver() 
        self.zk_obj.start_session()

    def configure(self, args, dissemination, lookup_strategy):
        """Initialize the SubscriberMW."""
        self.dissemination = dissemination
        self.lookup_strategy = lookup_strategy
        try:
            self.logger.info("SubscriberMW::configure")
            self.addr = args.addr
            self.port = args.port
            self.logger.debug("SubscriberMW::configure - obtaining ZMQ context")
            context = zmq.Context()
            self.poller = zmq.Poller()
            self.logger.debug("SubscriberMW::configure - creating REQ and SUB sockets")
            self.req = context.socket(zmq.REQ)
            self.sub = context.socket(zmq.SUB)
            self.poller.register(self.req, zmq.POLLIN)
            self.poller.register(self.sub, zmq.POLLIN)
            self.logger.debug("SubscriberMW::configure - connecting to Discovery service")
            connect_str = f"tcp://{args.discovery}"
            self.req.connect(connect_str)
            for topic in self.topiclist:
                self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)
            self.logger.info("SubscriberMW::configure completed")

            # Send message to Zookeeper to add new znode to the tree
            self.logger.info(f"Zookeeper: Creating znode for Subscriber {args.name} with addr {self.addr}, port {self.port}")
            self.zk_obj.create_znode("subscriber", args.name, self.addr, self.port)
            self.logger.info(f"Zookeeper: Created znode for Subscriber {args.name} !!")
            
        except Exception as e:
            raise e

    def disable_lookup(self):
        """Unregister the REQ socket to stop further lookup requests."""
        try:
            self.poller.unregister(self.req)
            self.logger.info("SubscriberMW::disable_lookup - REQ socket unregistered")
        except Exception as e:
            self.logger.error("SubscriberMW::disable_lookup - " + str(e))

    def configure_sources(self, sources):
        """Configure sources for Direct mode (if used)."""
        try:
            self.logger.info("SubscriberMW::configure_sources")
            for source in sources:
                connect_str = f"tcp://{source.addr}:{source.port}"
                self.sub.connect(connect_str)
            self.logger.info("SubscriberMW::configure_sources - completed")
        except Exception as e:
            raise e

    def event_loop(self, timeout=None):
        """Event loop to process incoming responses and messages."""
        try:
            self.logger.info("SubscriberMW::event_loop - starting event loop")
            while self.handle_events:
                events = dict(self.poller.poll(timeout=timeout))
                if not events:
                    timeout = self.upcall_obj.invoke_operation()
                elif self.req in events:
                    timeout = self.handle_reply()
                elif self.sub in events:
                    timeout = self.handle_message()
                else:
                    raise Exception("Unknown event received.")
            self.logger.info("SubscriberMW::event_loop - event loop terminated")
        except Exception as e:
            raise e

    def handle_reply(self):
        """Handle a reply from the Discovery service."""
        try:
            self.logger.info("SubscriberMW::handle_reply")
            bytesRcvd = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesRcvd)
            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                self.logger.info("SubscriberMW::handle_reply - Registration response received.")
                timeout = self.upcall_obj.register_response(disc_resp.register_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_ISREADY:
                self.logger.info("SubscriberMW::handle_reply - Ready response received.")
                timeout = self.upcall_obj.isready_response(disc_resp.isready_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                self.logger.info("SubscriberMW::handle_reply - Publisher lookup response received.")
                timeout = self.upcall_obj.lookup_response(disc_resp.lookup_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_BROKER:
                self.logger.info("SubscriberMW::handle_reply - Broker lookup response received.")
                broker_info = disc_resp.lookup_resp_broker.broker
                # Connect using the back-end port provided by the broker.
                connect_str = f"tcp://{broker_info.addr}:{broker_info.back_port}"
                self.logger.info(f"SubscriberMW::handle_reply - Connecting to broker at {connect_str}")
                self.sub.connect(connect_str)
                if hasattr(self.upcall_obj, "broker_lookup_response"):
                    timeout = self.upcall_obj.broker_lookup_response(broker_info)
                else:
                    timeout = 0
            else:
                raise ValueError("Unrecognized response message")
            return timeout
        except Exception as e:
            raise e

    def handle_message(self):
        """Handle an incoming message from a publisher or broker."""
        try:
            message = self.sub.recv_string()
            self.logger.info(f"SubscriberMW::handle_message - received: {message}")
            parts = message.split(":", 2)
            if len(parts) < 3:
                self.logger.error("Invalid message format")
                return 0
            topic, sent_timestamp_str, payload = parts
            sent_timestamp = float(sent_timestamp_str)
            latency = time.time() - sent_timestamp
            self.logger.info(f"Latency for topic '{topic}': {latency:.6f} seconds")
            self.log_latency(topic, latency)
            return 0
        except Exception as e:
            raise e

    def log_latency(self, topic, latency):
        # Append a record to a CSV file.
        try:
            with open("latency_results.csv", "a") as f:
                # Format: timestamp, topic, latency
                f.write(f"{time.time()},{topic},{latency}\n")
        except Exception as e:
            self.logger.error("Error logging latency: " + str(e))

    def register(self, name):
        """Register with the Discovery service as a subscriber."""
        try:
            self.logger.info("SubscriberMW::register")
            reg_info = discovery_pb2.RegistrantInfo()
            reg_info.id = name
            reg_info.addr = self.addr
            reg_info.port = self.port
            register_req = discovery_pb2.RegisterReq()
            register_req.role = discovery_pb2.ROLE_SUBSCRIBER
            register_req.info.CopyFrom(reg_info)
            register_req.topiclist[:] = self.topiclist
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER
            disc_req.register_req.CopyFrom(register_req)
            buf2send = disc_req.SerializeToString()
            self.logger.debug(f"SubscriberMW::register - sending: {buf2send}")
            self.req.send(buf2send)
            self.logger.info("SubscriberMW::register - registration message sent; awaiting reply")
        except Exception as e:
            raise e

    def lookup(self, name):
        """Perform a lookup (for publisher info in Direct mode or broker info in ViaBroker)."""
        try:
            self.logger.info("SubscriberMW::lookup")
            lookup_req = discovery_pb2.LookupPubByTopicReq()
            lookup_req.topiclist[:] = self.topiclist
            self.logger.debug("SubscriberMW::lookup - building outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq()
            if self.dissemination == "ViaBroker":
                disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_BROKER
            else:
                disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            disc_req.lookup_req.CopyFrom(lookup_req)
            buf2send = disc_req.SerializeToString()
            self.logger.debug(f"SubscriberMW::lookup - sending lookup: {buf2send}")
            self.req.send(buf2send)
            self.logger.info("SubscriberMW::lookup - lookup message sent; awaiting reply")
        except Exception as e:
            raise e

    def disable_event_loop(self):
        """Disable the event loop."""
        self.handle_events = False

    def set_upcall_handle(self, upcall_obj):
        """Set the application upcall handle."""
        self.upcall_obj = upcall_obj
