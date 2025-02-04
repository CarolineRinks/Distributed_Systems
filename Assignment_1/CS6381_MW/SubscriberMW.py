###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# PublisherMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the subscriber side of things.
#
# Remember that the subscriber middleware does not do anything on its own.
# It must be invoked by the application level logic. This middleware object maintains
# the ZMQ sockets and knows how to talk to Discovery service, etc.
#
# Here is what this middleware should do
# (1) it must maintain the ZMQ sockets, one in the REQ role to talk to the Discovery service
# and one in the SUB role to receive topic data
# (2) It must, on behalf of the application logic, register the subscriber application with the
# discovery service. To that end, it must use the protobuf-generated serialization code to
# send the appropriate message with the contents to the discovery service.
# (3) On behalf of the subscriber appln, it must use the ZMQ setsockopt method to subscribe to all the
# user-supplied topics of interest. 
# (4) Since it is a receiver, the middleware object will maintain a poller and even loop waiting for some
# subscription to show up (or response from Discovery service).
# (5) On receipt of a subscription, determine which topic it is and let the application level
# handle the incoming data. To that end, you may need to make an upcall to the application-level
# object.
#

import zmq  # ZMQ sockets
from CS6381_MW import discovery_pb2

class SubscriberMW:
    def __init__(self, logger):
        self.logger = logger
        self.req = None  # ZMQ REQ socket to talk to Discovery service
        self.sub = None  # ZMQ SUB socket for receiving data from publishers
        self.poller = None  # ZMQ poller for handling events
        self.addr = None  # Local address
        self.port = None  # Port number for subscription
        self.handle_events = True  # Controls event loop

    def configure(self, args):
        """ Initialize the SubscriberMW """
        try:
            self.logger.info("SubscriberMW::configure")
            self.addr = args.addr
            self.port = args.port
            context = zmq.Context()  # ZMQ context
            self.poller = zmq.Poller()  # Poller for managing events
            self.req = context.socket(zmq.REQ)  # REQ socket for communication with discovery service
            self.sub = context.socket(zmq.SUB)  # SUB socket for receiving data from publishers

            # Connect to the discovery service
            connect_str = f"tcp://{args.discovery}"
            self.req.connect(connect_str)

            # Bind the SUB socket to receive messages
            bind_str = f"tcp://*:{self.port}"
            self.sub.bind(bind_str)

            # Subscribe to all topics
            self.sub.setsockopt_string(zmq.SUBSCRIBE, "")

            # Register the socket for event handling
            self.poller.register(self.req, zmq.POLLIN)
            self.poller.register(self.sub, zmq.POLLIN)

            self.logger.info("SubscriberMW::configure completed")

        except Exception as e:
            raise e

    def event_loop(self, timeout=None):
        """ Event loop to handle incoming responses from Discovery service and messages from publishers """
        try:
            self.logger.info("SubscriberMW::event_loop - running")
            while self.handle_events:
                events = dict(self.poller.poll(timeout=timeout))
                if not events:
                    continue  # timeout, so continue waiting
                elif self.req in events:
                    self.handle_reply()
                elif self.sub in events:
                    self.handle_message()
                else:
                    raise Exception("Unknown event received.")
            self.logger.info("SubscriberMW::event_loop - completed")
        except Exception as e:
            raise e

    def handle_reply(self):
        """ Handle reply from the Discovery service """
        try:
            self.logger.info("SubscriberMW::handle_reply")
            bytesRcvd = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesRcvd)

            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                self.logger.info("SubscriberMW::handle_reply - Registration response received.")
            elif disc_resp.msg_type == discovery_pb2.TYPE_ISREADY:
                self.logger.info("SubscriberMW::handle_reply - Ready response received.")
            else:
                raise ValueError("Unknown message type received.")
        except Exception as e:
            raise e

    def handle_message(self):
        """ Handle incoming message from a publisher """
        try:
            message = self.sub.recv_string()
            self.logger.info(f"SubscriberMW::handle_message - received: {message}")
        except Exception as e:
            raise e

    def register(self, role, name, topiclist):
        """ Register with the discovery service as a publisher or subscriber """
        try:
            self.logger.info("SubscriberMW::register")

            # Create RegisterReq message
            register_req = discovery_pb2.RegisterReq()
            register_req.role = role
            register_req.info.id = name
            register_req.info.addr = self.addr
            register_req.info.port = self.port
            register_req.topiclist[:] = topiclist

            # Wrap in a DiscoveryReq message
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER
            disc_req.register_req.CopyFrom(register_req)

            # Send the register request
            buf2send = disc_req.SerializeToString()
            self.req.send(buf2send)
            self.logger.info("SubscriberMW::register - sent register message")
        except Exception as e:
            raise e

    def is_ready(self):
        """ Check if the system is ready to proceed """
        try:
            self.logger.info("SubscriberMW::is_ready")
            isready_req = discovery_pb2.IsReadyReq()
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY
            disc_req.isready_req.CopyFrom(isready_req)

            # Send the "is ready" request
            buf2send = disc_req.SerializeToString()
            self.req.send(buf2send)
            self.logger.info("SubscriberMW::is_ready - sent is_ready message")
        except Exception as e:
            raise e

    def disable_event_loop(self):
        """ Disable the event loop """
        self.handle_events = False


