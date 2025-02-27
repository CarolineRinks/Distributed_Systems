import zmq
import logging
import time
from CS6381_MW import discovery_pb2
from .zkclient import ZK_Driver

class PublisherMW():
    def __init__(self, logger):
        self.logger = logger                # Logger instance
        self.req = None                     # REQ socket for Discovery service
        self.pub = None                     # PUB socket for dissemination
        self.poller = None                  # ZMQ Poller for event loop
        self.addr = None                    # Local advertised IP address
        self.port = None                    # Port number for publishing
        self.upcall_obj = None              # Pointer to the application-level object
        self.handle_events = True           # Controls the event loop
        self.dissemination = None           # Dissemination strategy ("Direct" or "ViaBroker")
        
        # Attribute to hold current discovery primary endpoint (tcp://IP:port)
        self.discovery_primary_endpoint = None

        self.zk_obj = ZK_Driver()
        self.zk_obj.init_driver() 
        self.zk_obj.start_session()

    def configure(self, args, dissemination=None):
        '''Initialize the PublisherMW.'''
        try:
            self.logger.info("PublisherMW::configure")
            self.port = args.port
            self.addr = args.addr
            self.dissemination = dissemination

            # Get the ZMQ context and poller.
            self.logger.info("PublisherMW::configure - obtaining ZMQ context")
            context = zmq.Context()
            self.poller = zmq.Poller()

            # Create the REQ and PUB sockets.
            self.logger.info("PublisherMW::configure - creating REQ and PUB sockets")
            self.req = context.socket(zmq.REQ)
            self.pub = context.socket(zmq.PUB)
            self.poller.register(self.req, zmq.POLLIN)

            # Retrieve the primary discovery pointer from ZooKeeper.
            try:
                primary_data, _ = self.zk_obj.zk.get("/root/discovery/primary")
                primary_str = primary_data.decode('utf-8')
                new_connect_str = f"tcp://{primary_str}"
                self.discovery_primary_endpoint = new_connect_str
                self.logger.info("PublisherMW::configure - connecting to primary discovery at " + new_connect_str)
                self.req.connect(new_connect_str)
            except Exception as e:
                self.logger.error("PublisherMW::configure - error retrieving primary discovery pointer: " + str(e))
                # raise e

            # Set a watch on the primary pointer so that if it changes, we reconnect.
            @self.zk_obj.zk.DataWatch("/root/discovery/primary")
            def watch_primary(data, stat, event):
                if data:
                    new_primary = data.decode('utf-8')
                    new_connect_str = f"tcp://{new_primary}"
                    if new_connect_str != self.discovery_primary_endpoint:
                        self.logger.info("PublisherMW::watch_primary - primary updated to " + new_connect_str)
                        if self.discovery_primary_endpoint:
                            try:
                                self.req.disconnect(self.discovery_primary_endpoint)
                            except Exception as ex:
                                self.logger.error("PublisherMW::watch_primary - error disconnecting from old endpoint: " + str(ex))
                        self.req.connect(new_connect_str)
                        self.discovery_primary_endpoint = new_connect_str

            # In Direct mode, bind the PUB socket so that subscribers can connect.
            if self.dissemination == "ViaBroker":
                self.logger.info("PublisherMW::configure - ViaBroker mode: publisher will connect to broker for dissemination.")
            else:
                bind_string = "tcp://*:" + str(self.port)
                self.pub.bind(bind_string)
                self.logger.info("PublisherMW::configure - Direct mode: bound PUB socket to " + bind_string)

            self.logger.info("PublisherMW::configure completed")

            # Register publisher in ZooKeeper.
            self.logger.info(f"Zookeeper: Creating znode for Publisher {args.name} with addr {self.addr}, port {self.port}")
            self.zk_obj.create_znode("publisher", args.name, self.addr, self.port)
            self.logger.info(f"Zookeeper: Created znode for Publisher {args.name}")

        except Exception as e:
            raise e

    # (Remaining methods (event_loop, handle_reply, register, lookup_broker, disseminate, etc.) remain unchanged.)
    def event_loop(self, timeout=None):
        try:
            self.logger.info("PublisherMW::event_loop - run the event loop")
            while self.handle_events:
                events = dict(self.poller.poll(timeout=timeout))
                if not events:
                    timeout = self.upcall_obj.invoke_operation()
                elif self.req in events:
                    timeout = self.handle_reply()
                else:
                    raise Exception("Unknown event after poll")
            self.logger.info("PublisherMW::event_loop - out of the event loop")
        except Exception as e:
            raise e

    def handle_reply(self):
        try:
            self.logger.info("PublisherMW::handle_reply")
            bytesRcvd = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesRcvd)

            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                timeout = self.upcall_obj.register_response(disc_resp.register_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_BROKER:
                self.logger.info("PublisherMW::handle_reply - Broker lookup response received.")
                broker_info = disc_resp.lookup_resp_broker.broker
                connect_str = f"tcp://{broker_info.addr}:{broker_info.front_port}"
                self.logger.info("PublisherMW::handle_reply - connecting PUB socket to broker at " + connect_str)
                self.pub.connect(connect_str)
                if hasattr(self.upcall_obj, "broker_lookup_response"):
                    timeout = self.upcall_obj.broker_lookup_response(broker_info)
                else:
                    timeout = 0
            else:
                raise ValueError("Unrecognized response message")
            return timeout
        except Exception as e:
            raise e

    def register(self, name, topiclist):
        try:
            self.logger.info("PublisherMW::register")
            reg_info = discovery_pb2.RegistrantInfo()
            reg_info.id = name
            reg_info.addr = self.addr
            reg_info.port = self.port
            register_req = discovery_pb2.RegisterReq()
            register_req.role = discovery_pb2.ROLE_PUBLISHER
            register_req.info.CopyFrom(reg_info)
            register_req.topiclist[:] = topiclist
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER
            disc_req.register_req.CopyFrom(register_req)
            buf2send = disc_req.SerializeToString()
            self.logger.info("PublisherMW::register - sending: {}".format(buf2send))
            self.req.send(buf2send)
            self.logger.info("PublisherMW::register - registration message sent; awaiting reply")
        except Exception as e:
            raise e

    def lookup_broker(self):
        try:
            self.logger.info("PublisherMW::lookup_broker")
            req_msg = discovery_pb2.DiscoveryReq()
            req_msg.msg_type = discovery_pb2.TYPE_LOOKUP_BROKER
            buf2send = req_msg.SerializeToString()
            self.logger.info("PublisherMW::lookup_broker - sending lookup broker request: {}".format(buf2send))
            self.req.send(buf2send)
        except Exception as e:
            raise e

    def disseminate(self, id, topic, data):
        try:
            timestamp = time.time()
            self.logger.info("PublisherMW::disseminate")
            send_str = f"{topic}:{timestamp}:{data}"
            self.logger.info("PublisherMW::disseminate - sending: {}".format(send_str))
            self.pub.send(bytes(send_str, "utf-8"))
            self.logger.info("PublisherMW::disseminate complete")
        except Exception as e:
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        self.handle_events = False
