import zmq
import logging
import time
from CS6381_MW import discovery_pb2
from .zkclient import ZK_Driver

class SubscriberMW():
    def __init__(self, logger, topiclist, dissemination, discovery_group):
        self.logger = logger
        self.req = None                   # REQ socket for Discovery service
        self.sub = None                   # SUB socket for receiving messages
        self.poller = None                # ZMQ poller for events
        self.name = None                  # Subscriber name
        self.addr = None                  # Local address
        self.port = None                  # Subscriber’s port
        self.topiclist = topiclist        # Topics of interest
        self.upcall_obj = None            # Upcall handle to application
        self.handle_events = True         # Event loop control
        self.dissemination = dissemination
        self.lookup_pending = False
        self.discovery_group = discovery_group

        # New attribute: current broker endpoint for dissemination (tcp://IP:port)
        self.current_broker_endpoint = None
        # Attribute to hold current primary discovery endpoint.
        self.discovery_primary_endpoint = None

        self.zk_obj = ZK_Driver()
        self.zk_obj.init_driver() 
        self.zk_obj.start_session()

        if self.dissemination == "Direct":
            @self.zk_obj.zk.ChildrenWatch("/root/pubs")
            def watch_publishers(children):
                self.logger.info("SubscriberMW::watch_publishers - Detected change in /root/pubs")
                if self.req is not None:
                    self.lookup(self.name)
                else:
                    self.logger.warning("SubscriberMW::watch_publishers - Skipping lookup: self.req not initialized.")

    def configure(self, args, dissemination):
        self.dissemination = dissemination
        try:
            self.logger.info("SubscriberMW::configure")
            self.addr = args.addr
            self.port = args.port
            self.name = args.name
            self.logger.info("SubscriberMW::configure - obtaining ZMQ context")
            context = zmq.Context()
            self.poller = zmq.Poller()
            self.logger.info("SubscriberMW::configure - creating REQ and SUB sockets")
            self.req = context.socket(zmq.REQ)
            self.sub = context.socket(zmq.SUB)
            self.poller.register(self.req, zmq.POLLIN)
            self.poller.register(self.sub, zmq.POLLIN)
            
            # Retrieve the primary discovery pointer from ZooKeeper.
            try:
                primary_data, _ = self.zk_obj.zk.get(f"/root/discovery/group{self.discovery_group}/primary")
                primary_str = primary_data.decode('utf-8')
                new_connect_str = f"tcp://{primary_str}"
                self.discovery_primary_endpoint = new_connect_str
                self.logger.info("SubscriberMW::configure - connecting to primary discovery at " + new_connect_str)
                self.req.connect(new_connect_str)
            except Exception as e:
                self.logger.error("SubscriberMW::configure - error retrieving primary discovery pointer: " + str(e))

            # Set subscriptions on the SUB socket.
            for topic in self.topiclist:
                self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)
            self.logger.info("SubscriberMW::configure completed")

            # Set a watch on the primary discovery node.
            @self.zk_obj.zk.DataWatch(f"/root/discovery/group{self.discovery_group}/primary")
            def watch_primary(data, stat, event):
                if data:
                    new_primary = data.decode('utf-8')
                    new_connect_str = f"tcp://{new_primary}"
                    if new_connect_str != self.discovery_primary_endpoint:
                        self.logger.info("SubscriberMW::watch_primary - primary updated to " + new_connect_str)
                        if self.discovery_primary_endpoint:
                            try:
                                self.req.disconnect(self.discovery_primary_endpoint)
                            except Exception as ex:
                                self.logger.error("SubscriberMW::watch_primary - error disconnecting: " + str(ex))
                        self.req.connect(new_connect_str)
                        self.discovery_primary_endpoint = new_connect_str

            # *** NEW: Watch for Broker Primary Changes ***
            @self.zk_obj.zk.DataWatch(f"/root/broker/group{str(self.discovery_group)}/primary")
            def watch_broker_primary(data, stat, event):
                if data:
                    new_broker_str = data.decode('utf-8')
                    # Assume format "IP:front_port:back_port" and use back_port for subscriber connection.
                    parts = new_broker_str.split(":")
                    if len(parts) >= 3:
                        new_broker_endpoint = f"tcp://{parts[0]}:{parts[2]}"
                    else:
                        new_broker_endpoint = f"tcp://{new_broker_str}"
                    self.logger.info("SubscriberMW::watch_broker_primary - New broker primary: " + new_broker_endpoint)
                    if self.current_broker_endpoint is None:
                        self.logger.info("SubscriberMW::watch_broker_primary - No current broker endpoint; connecting to " + new_broker_endpoint)
                        self.sub.connect(new_broker_endpoint)
                        self.current_broker_endpoint = new_broker_endpoint
                    elif new_broker_endpoint != self.current_broker_endpoint:
                        self.logger.info("SubscriberMW::watch_broker_primary - Broker endpoint changed from " +
                                         self.current_broker_endpoint + " to " + new_broker_endpoint)
                        try:
                            self.sub.disconnect(self.current_broker_endpoint)
                        except Exception as ex:
                            self.logger.error("SubscriberMW::watch_broker_primary - error disconnecting from old broker: " + str(ex))
                        self.sub.connect(new_broker_endpoint)
                        self.current_broker_endpoint = new_broker_endpoint

            # Register subscriber in ZooKeeper.
            self.logger.info(f"Zookeeper: Creating znode for Subscriber {args.name} with addr {self.addr}, port {self.port}")
            self.zk_obj.create_znode("subscriber", args.name, self.addr, self.port)
            self.logger.info(f"Zookeeper: Created znode for Subscriber {args.name} !!")
            
        except Exception as e:
            raise e

    def disable_lookup(self):
        try:
            self.poller.unregister(self.req)
            self.logger.info("SubscriberMW::disable_lookup - REQ socket unregistered")
        except Exception as e:
            self.logger.error("SubscriberMW::disable_lookup - " + str(e))

    def configure_sources(self, sources):
        try:
            self.logger.info("SubscriberMW::configure_sources")
            for source in sources:
                self.logger.info(f"SubscriberMW::configure_sources - configuring {source} !!!!!!!!")
                connect_str = f"tcp://{source.addr}:{source.port}"
                self.sub.connect(connect_str)
            self.logger.info("SubscriberMW::configure_sources - completed")
        except Exception as e:
            raise e

    def event_loop(self, timeout=None):
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
        try:
            self.logger.info("SubscriberMW::handle_reply")
            bytesRcvd = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesRcvd)
            self.lookup_pending = False
            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                self.logger.info("SubscriberMW::handle_reply - Registration response received.")
                timeout = self.upcall_obj.register_response(disc_resp.register_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                self.logger.info("SubscriberMW::handle_reply - Publisher lookup response received.")
                timeout = self.upcall_obj.lookup_response(disc_resp.lookup_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_BROKER:
                self.logger.info("SubscriberMW::handle_reply - Broker lookup response received.")
                broker_info = disc_resp.lookup_resp_broker.broker
                # Use broker's back port for subscribers.
                connect_str = f"tcp://{broker_info.addr}:{broker_info.back_port}"
                self.logger.info("SubscriberMW::handle_reply - Connecting to broker at " + connect_str)
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
        try:
            message = self.sub.recv_string()
            self.logger.info("SubscriberMW::handle_message - received: " + message)
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
        try:
            with open("latency_results.csv", "a") as f:
                f.write(f"{time.time()},{topic},{latency}\n")
        except Exception as e:
            self.logger.error("Error logging latency: " + str(e))

    def register(self, name):
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
            self.logger.info("SubscriberMW::register - sending: " + str(buf2send))
            self.req.send(buf2send)
            self.logger.info("SubscriberMW::register - registration message sent; awaiting reply")
        except Exception as e:
            raise e

    def lookup(self, name):
        try:
            if self.lookup_pending:
                self.logger.info("SubscriberMW::lookup - lookup already pending; not sending new request")
                return
            self.lookup_pending = True
            self.logger.info("SubscriberMW::lookup")
            lookup_req = discovery_pb2.LookupPubByTopicReq()
            lookup_req.topiclist[:] = self.topiclist
            self.logger.info("SubscriberMW::lookup - building outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq()
            if self.dissemination == "ViaBroker":
                disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_BROKER
            else:
                disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            disc_req.lookup_req.CopyFrom(lookup_req)
            buf2send = disc_req.SerializeToString()
            self.logger.info("SubscriberMW::lookup - sending lookup: " + str(buf2send))
            self.req.send(buf2send)
            self.logger.info("SubscriberMW::lookup - lookup message sent; awaiting reply")
        except Exception as e:
            raise e

    def disable_event_loop(self):
        self.handle_events = False

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj
