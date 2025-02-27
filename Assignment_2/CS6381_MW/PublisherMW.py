###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Revised for ViaBroker dissemination support.
#
###############################################

import os
import sys
import time
import logging
import zmq
import time

from CS6381_MW import discovery_pb2
from .zkclient import ZK_Driver


class PublisherMW():
    def __init__(self, logger):
        self.logger = logger                # Logger instance
        self.req = None                     # REQ socket for Discovery service
        self.pub = None                     # PUB socket for dissemination (either bound or connected)
        self.poller = None                  # ZMQ Poller for event loop
        self.addr = None                    # Local advertised IP address
        self.port = None                    # Port number for publishing (used in Direct mode)
        self.upcall_obj = None              # Pointer to the application-level object
        self.handle_events = True           # Controls the event loop
        self.dissemination = None           # Dissemination strategy ("Direct" or "ViaBroker")

        self.zk_obj = ZK_Driver()
        self.zk_obj.init_driver() 
        self.zk_obj.start_session()

    def configure(self, args, dissemination=None, lookup=None):
        '''Initialize the PublisherMW. In Direct mode the PUB socket is bound,
           but in ViaBroker mode the publisher will later connect to the broker.'''
        try:
            self.logger.info("PublisherMW::configure")
            self.port = args.port
            self.addr = args.addr
            self.dissemination = dissemination

            # Get the ZMQ context and poller
            self.logger.info("PublisherMW::configure - obtaining ZMQ context")
            context = zmq.Context()
            self.poller = zmq.Poller()

            # Create the REQ and PUB sockets
            self.logger.info("PublisherMW::configure - creating REQ and PUB sockets")
            self.req = context.socket(zmq.REQ)
            self.pub = context.socket(zmq.PUB)
            self.poller.register(self.req, zmq.POLLIN)

            # Connect to the Discovery service using the given address/port.
            self.logger.info("PublisherMW::configure - connecting to Discovery service")
            connect_str = "tcp://" + args.discovery
            self.req.connect(connect_str)

            # In Direct mode, bind the PUB socket so that subscribers can connect.
            # In ViaBroker mode, we will later connect the PUB socket to the broker.
            if self.dissemination == "ViaBroker":
                self.logger.info("PublisherMW::configure - ViaBroker mode: publisher will connect to broker for dissemination.")
            else:
                bind_string = "tcp://*:" + str(self.port)
                self.pub.bind(bind_string)
                self.logger.info("PublisherMW::configure - Direct mode: bound PUB socket to " + bind_string)

            self.logger.info("PublisherMW::configure completed")

            # Send message to Zookeeper to add new znode to the tree
            self.logger.info(f"Zookeeper: Creating znode for Publisher {args.name} with, addr {self.addr}, port {self.port}")
            self.zk_obj.create_znode("publisher", args.name, self.addr, self.port)
            self.logger.info(f"Zookeeper: Created znode for Publisher {args.name}")

        except Exception as e:
            raise e

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
                # Registration reply
                timeout = self.upcall_obj.register_response(disc_resp.register_resp)
            # elif disc_resp.msg_type == discovery_pb2.TYPE_ISREADY:
            #     # IsReady reply
            #     timeout = self.upcall_obj.isready_response(disc_resp.isready_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_BROKER:
                self.logger.info("PublisherMW::handle_reply - Broker lookup response received.")
                # Extract broker information and connect our PUB socket to the broker’s frontend
                broker_info = disc_resp.lookup_resp_broker.broker
                connect_str = f"tcp://{broker_info.addr}:{broker_info.front_port}"
                self.logger.info(f"PublisherMW::handle_reply - connecting PUB socket to broker at {connect_str}")
                self.pub.connect(connect_str)
                # Notify the application (upcall) that broker lookup is complete.
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
        '''Register the publisher with the Discovery service.'''
        try:
            self.logger.info("PublisherMW::register")
            self.logger.info("PublisherMW::register - populate the Registrant Info")
            reg_info = discovery_pb2.RegistrantInfo()
            reg_info.id = name
            reg_info.addr = self.addr
            reg_info.port = self.port
            self.logger.info("PublisherMW::register - done populating Registrant Info")

            self.logger.info("PublisherMW::register - populate the nested RegisterReq")
            register_req = discovery_pb2.RegisterReq()
            register_req.role = discovery_pb2.ROLE_PUBLISHER
            register_req.info.CopyFrom(reg_info)
            register_req.topiclist[:] = topiclist
            self.logger.info("PublisherMW::register - done populating nested RegisterReq")

            self.logger.info("PublisherMW::register - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER
            disc_req.register_req.CopyFrom(register_req)
            buf2send = disc_req.SerializeToString()
            self.logger.info("PublisherMW::register - serialized buf: {}".format(buf2send))

            self.logger.info("PublisherMW::register - sending buffer to Discovery service")
            self.req.send(buf2send)
            self.logger.info("PublisherMW::register - sent register message; waiting for reply")
        except Exception as e:
            raise e

    # def is_ready(self):
    #     '''Send an is_ready request to the Discovery service.'''
    #     try:
    #         self.logger.info("PublisherMW::is_ready")
    #         self.logger.info("PublisherMW::is_ready - populate the nested IsReady msg")
    #         isready_req = discovery_pb2.IsReadyReq()
    #         self.logger.info("PublisherMW::is_ready - done populating nested IsReady msg")

    #         self.logger.info("PublisherMW::is_ready - build the outer DiscoveryReq message")
    #         disc_req = discovery_pb2.DiscoveryReq()
    #         disc_req.msg_type = discovery_pb2.TYPE_ISREADY
    #         disc_req.isready_req.CopyFrom(isready_req)
    #         self.logger.info("PublisherMW::is_ready - done building the outer message")
    #         buf2send = disc_req.SerializeToString()
    #         self.logger.info("PublisherMW::is_ready - serialized buf: {}".format(buf2send))

    #         self.logger.info("PublisherMW::is_ready - sending buffer to Discovery service")
    #         self.req.send(buf2send)
    #         self.logger.info("PublisherMW::is_ready - request sent; awaiting reply")
    #     except Exception as e:
    #         raise e

    def lookup_broker(self):
        '''Send a lookup request to get broker information.'''
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
        '''Disseminate data on our PUB socket.
           In ViaBroker mode, the PUB socket is connected to the broker;
           in Direct mode, the PUB socket is bound and subscribers connect directly.'''
        try:
            timestamp = time.time()
        # Format the message as "topic:timestamp:data"
            
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


