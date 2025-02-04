###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the discovery middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student.
#
# The discovery service is a server. So at the middleware level, we will maintain
# a REP socket binding it to the port on which we expect to receive requests.
#
# There will be a forever event loop waiting for requests. Each request will be parsed
# and the application logic asked to handle the request. To that end, an upcall will need
# to be made to the application logic.

import zmq  # ZMQ sockets
from CS6381_MW import discovery_pb2

class DiscoveryMW:
    def __init__(self, logger):
        self.logger = logger
        self.req = None  # ZMQ REQ socket for communication with discovery service
        self.poller = None  # ZMQ poller for handling events
        self.addr = None  # Local address
        self.port = None  # Port number for discovery service
        self.handle_events = True  # Controls event loop

    def configure(self, args):
        """ Initialize the DiscoveryMW """
        try:
            self.logger.info("DiscoveryMW::configure")
            self.addr = args.addr
            self.port = args.port
            context = zmq.Context()  # ZMQ context
            self.poller = zmq.Poller()  # Poller for managing events
            self.req = context.socket(zmq.REQ)  # REQ socket for sending requests

            # Connect to the discovery service
            connect_str = f"tcp://{args.discovery}"
            self.req.connect(connect_str)

            # Register the socket for event handling
            self.poller.register(self.req, zmq.POLLIN)
            self.logger.info("DiscoveryMW::configure completed")

        except Exception as e:
            raise e

    def event_loop(self, timeout=None):
        """ Event loop to handle incoming responses from Discovery service """
        try:
            self.logger.info("DiscoveryMW::event_loop - running")
            while self.handle_events:
                events = dict(self.poller.poll(timeout=timeout))
                if not events:
                    continue  # timeout, so continue waiting
                elif self.req in events:
                    self.handle_reply()
                else:
                    raise Exception("Unknown event received.")
            self.logger.info("DiscoveryMW::event_loop - completed")
        except Exception as e:
            raise e

    def handle_reply(self):
        """ Handle reply from the Discovery service """
        try:
            self.logger.info("DiscoveryMW::handle_reply")
            bytesRcvd = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesRcvd)

            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                self.logger.info("DiscoveryMW::handle_reply - Registration response received.")
            elif disc_resp.msg_type == discovery_pb2.TYPE_ISREADY:
                self.logger.info("DiscoveryMW::handle_reply - Ready response received.")
            else:
                raise ValueError("Unknown message type received.")
        except Exception as e:
            raise e

    def register(self, role, name, topiclist):
        """ Register with the discovery service as a publisher or subscriber """
        try:
            self.logger.info("DiscoveryMW::register")

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
            self.logger.info("DiscoveryMW::register - sent register message")
        except Exception as e:
            raise e

    def is_ready(self):
        """ Check if the system is ready to proceed """
        try:
            self.logger.info("DiscoveryMW::is_ready")
            isready_req = discovery_pb2.IsReadyReq()
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY
            disc_req.isready_req.CopyFrom(isready_req)

            # Send the "is ready" request
            buf2send = disc_req.SerializeToString()
            self.req.send(buf2send)
            self.logger.info("DiscoveryMW::is_ready - sent is_ready message")
        except Exception as e:
            raise e

    def disable_event_loop(self):
        """ Disable the event loop """
        self.handle_events = False
