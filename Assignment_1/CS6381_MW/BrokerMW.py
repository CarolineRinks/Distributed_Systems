###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the broker middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# BrokerMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the broker side of things.
#
# As mentioned earlier, a broker serves as a proxy and hence has both
# publisher and subscriber roles. So in addition to the REQ socket to talk to the
# Discovery service, it will have both PUB and SUB sockets as it must work on
# behalf of the real publishers and subscribers. So this will have the logic of
# both publisher and subscriber middleware.

import os       # for OS functions
import sys      # for syspath and system exception
import time     # for sleep
import logging  # for logging. Use it in place of print statements.
import zmq      # ZMQ sockets

# import serialization logic
from CS6381_MW import discovery_pb2

class BrokerMW ():

    def __init__ (self, logger):
        self.logger = logger  # internal logger for print statements
        self.req = None # will be a ZMQ REQ socket to talk to Discovery service
        self.pub = None # will be a ZMQ PUB socket to publish on behalf of publishers
        self.sub = None # will be a ZMQ SUB socket to subscribe on behalf of subscribers
        self.poller = None # used to wait on incoming replies
        self.addr = None # our advertised IP address
        self.port = None # port num where we are going to publish our topics
        self.upcall_obj = None # handle to appln obj to handle appln-specific data
        self.handle_events = True # in general we keep going thru the event loop

    def configure (self, args):
        ''' Initialize the object '''
        try:
            # Here we initialize any internal variables
            self.logger.info ("BrokerMW::configure")

            # First retrieve our advertised IP addr and the publication port num
            self.port = args.port
            self.addr = args.addr
            
            # Next get the ZMQ context
            self.logger.debug ("BrokerMW::configure - obtain ZMQ context")
            context = zmq.Context ()  # returns a singleton object

            # get the ZMQ poller object
            self.logger.debug ("BrokerMW::configure - obtain the poller")
            self.poller = zmq.Poller ()
            
            # Now acquire the REQ and PUB sockets
            # REQ is needed because we are the client of the Discovery service
            # PUB is needed because we publish topic data
            # SUB is needed because we subscribe to all topics
            self.logger.debug ("BrokerMW::configure - obtain REQ and PUB sockets")
            self.req = context.socket(zmq.REQ)
            self.pub = context.socket(zmq.PUB)
            self.sub = context.socket(zmq.SUB)

            # Since are using the event loop approach, register the REQ socket for incoming events
            self.logger.debug ("BrokerMW::configure - register the REQ socket for incoming replies")
            self.poller.register(self.req, zmq.POLLIN)
            
            # Now connect ourselves to the discovery service. Recall that the IP/port were
            # supplied in our argument parsing. Best practices of ZQM suggest that the
            # one who maintains the REQ socket should do the "connect"
            self.logger.debug ("BrokerMW::configure - connect to Discovery service")
            connect_str = "tcp://" + args.discovery
            self.req.connect(connect_str)
            
        except Exception as e:
            raise e
        
    def configure_publishers (self):
        ''' Configures the PUB socket after receiving lookup response from Discovery service '''
        pass

    def configure_subscribers (self):
        ''' Configures the SUB socket after receiving lookup response from Discovery service '''
        pass
        
    def register(self, name):
        ''' Register with the discovery service '''
        ''' I THINK THIS IS DONE -C '''
        try:
            self.logger.info("BrokerMW::register")
            # as part of registration with the discovery service, we send
            # what role we are playing, the list of topics we are publishing,
            # and our whereabouts, e.g., name, IP and port
            # The following code shows serialization using the protobuf generated code.
            # Build the Registrant Info message first.
            self.logger.debug ("BrokerMW::register - populate the Registrant Info")
            reg_info = discovery_pb2.RegistrantInfo() # allocate
            reg_info.id = name
            reg_info.addr = self.addr
            reg_info.port = self.port
            self.logger.debug ("BrokerMW::register - done populating the Registrant Info")
            # Next build a RegisterReq message
            self.logger.debug ("BrokerMW::register - populate the nested register req")
            register_req = discovery_pb2.RegisterReq()  # allocate
            register_req.role = discovery_pb2.ROLE_BOTH  # the broker plays both pub and sub roles!
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            register_req.info.CopyFrom(reg_info)    # copy contents of inner structure
            #register_req.topiclist[:] = topiclist   # this is how repeated entries are added (or use append() or extend ()
            self.logger.debug ("BrokerMW::register - done populating nested RegisterReq")
            # Finally, build the outer layer DiscoveryReq Message
            self.logger.debug ("BrokerMW::register - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq()  # allocate
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.register_req.CopyFrom(register_req)
            self.logger.debug ("BrokerMW::register - done building the outer message")
            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString()
            self.logger.debug ("Stringified serialized buf = {}".format (buf2send))
            # now send this to our discovery service
            self.logger.debug ("BrokerMW::register - send stringified buffer to Discovery service")
            self.req.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes
            # now go to our event loop to receive a response to this request
            self.logger.info ("BrokerMW::register - sent register message and now wait for reply")
        except Exception as e:
            raise e
        
    def lookup_pubs(self):
        ''' Sends a TYPE_LOOKUP_ALL_PUBS message to the discovery service '''
        pass

    def lookup_subs(self):
        ''' Sends a TYPE_LOOKUP_ALL_SUBS message to the discovery service '''
        pass

    def handle_message(self):
        ''' Handle incoming message from a publisher '''
        try:
            message = self.sub.recv_string()
            self.logger.info(f"BrokerMW::handle_message - received: {message}")
        except Exception as e:
            raise e
        
    def handle_reply(self):
        ''' Handle reply from the Discovery service '''
        try:
            self.logger.info("BrokerMW::handle_reply")
            # let us first receive all the bytes
            bytesRcvd = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesRcvd)
            if (disc_resp.msg_type == discovery_pb2.TYPE_REGISTER):
                self.logger.info("BrokerMW::handle_reply - Registration response received.")
                # let the appln level object decide what to do
                timeout = self.upcall_obj.register_response(disc_resp.register_resp)
            elif (disc_resp.msg_type == discovery_pb2.TYPE_ISREADY):
                self.logger.info("BrokerMW::handle_reply - Ready response received.")
                # this is a response to is ready request. let the appln level object decide what to do
                timeout = self.upcall_obj.isready_response(disc_resp.isready_resp)
            elif (disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS):
                self.logger.info("BrokerMW::handle_reply - Lookup response received.")
                # this is a response to lookup request. let the appln level object decide what to do
                timeout = self.upcall_obj.lookup_response(disc_resp.lookup_resp)
            else: # anything else is unrecognizable by this object
                raise ValueError ("Unrecognized response message")
            return timeout
        except Exception as e:
            raise e

    def disable_event_loop(self):
        ''' Disable the event loop '''
        self.handle_events = False

    def set_upcall_handle (self, upcall_obj):
        ''' set upcall handle '''
        self.upcall_obj = upcall_obj


