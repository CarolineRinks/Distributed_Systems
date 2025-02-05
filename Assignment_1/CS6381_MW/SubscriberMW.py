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
# (4) Since it is a receiver, the middleware object will maintain a poller and event loop waiting for some
# subscription to show up (or response from Discovery service).
# (5) On receipt of a subscription, determine which topic it is and let the application level
# handle the incoming data. To that end, you may need to make an upcall to the application-level
# object.
#

import zmq  # ZMQ sockets
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import logging # for logging. Use it in place of print statements.
from CS6381_MW import discovery_pb2

class SubscriberMW:
    def __init__(self, logger):
        self.logger = logger
        self.req = None  # ZMQ REQ socket to talk to Discovery service
        self.sub = None  # ZMQ SUB socket for receiving data from publishers
        self.poller = None  # ZMQ poller for handling events
        self.addr = None  # Local address
        self.port = None  # Port number for subscription
        self.upcall_obj = None # handle to appln obj to handle appln-specific data
        self.handle_events = True  # Controls event loop

    def configure(self, args):
        """ Initialize the SubscriberMW """
        try:
            self.logger.info("SubscriberMW::configure")

            # First retrieve our advertised IP addr and the publication port num
            self.addr = args.addr
            self.port = args.port

            # Next get the ZMQ context
            self.logger.debug ("SubscriberMW::configure - obtain ZMQ context")
            context = zmq.Context ()  # returns a singleton object

            # get the ZMQ poller object
            self.logger.debug ("SubscriberMW::configure - obtain the poller")
            self.poller = zmq.Poller () # Poller for managing events

            # Now acquire the REQ and SUB sockets
            self.logger.debug ("SubscriberMW::configure - obtain REQ and SUB sockets")
            self.req = context.socket(zmq.REQ)  # REQ socket for communication with discovery service
            self.sub = context.socket(zmq.SUB)  # SUB socket for receiving data from publishers

            # Since are using the event loop approach, register both REQ and SUB sockets for incoming events
            self.logger.debug ("SubscriberrMW::configure - register the REQ socket for incoming replies")
            self.poller.register(self.req, zmq.POLLIN)
            self.poller.register(self.sub, zmq.POLLIN)

            # Now connect ourselves to the discovery service. Recall that the IP/port were
            # supplied in our argument parsing. Best practices of ZQM suggest that the
            # one who maintains the REQ socket should do the "connect"
            self.logger.debug ("SubscriberMW::configure - connect to Discovery service")
            # For our assignments we will use TCP. The connect string is made up of
            # tcp:// followed by IP addr:port number.
            connect_str = f"tcp://{args.discovery}"
            self.req.connect(connect_str)

            # ------------------ NEW CODE:

            # Bind the SUB socket to receive messages
            bind_str = f"tcp://*:{self.port}"
            self.sub.bind(bind_str)

            # Subscribe to all topics
            self.sub.setsockopt_string(zmq.SUBSCRIBE, "")

            # ------------------ NEW CODE END:

            self.logger.info("SubscriberMW::configure completed")

        except Exception as e:
            raise e

    ##########################################################
    # run the event loop where we expect to receive a replies
    ##########################################################
    def event_loop(self, timeout=None):
        """ Event loop to handle incoming responses from Discovery service and messages from publishers """
        try:
            self.logger.info("SubscriberMW::event_loop - run the event loop")

            while self.handle_events:
                # poll for events. We give it an infinite timeout.
                # The return value is a socket to event mask mapping
                events = dict(self.poller.poll(timeout=timeout))

                # check if a timeout has occurred. We know this is the case when
                # the event mask is empty
                if not events:
                    # timeout has occurred so it is time for us to make appln-level
                    # method invocation. Make an upcall to the generic "invoke_operation"
                    # which takes action depending on what state the application
                    # object is in.
                    timeout = self.upcall_obj.invoke_operation ()

                elif self.req in events:
                    # handle the incoming reply from discovery service
                    timeout = self.handle_reply()

                elif self.sub in events:
                    # handle the incoming reply from publishers
                    timeout = self.handle_message()

                else:
                    raise Exception("Unknown event received.")
                
            self.logger.info("SubscriberMW::event_loop - completed")
        except Exception as e:
            raise e

    def handle_reply(self):
        """ Handle reply from the Discovery service """
        try:
            self.logger.info("SubscriberMW::handle_reply")

            # let us first receive all the bytes
            bytesRcvd = self.req.recv()

            # now use protobuf to deserialize the bytes
            # The way to do this is to first allocate the space for the
            # message we expect, here DiscoveryResp and then parse
            # the incoming bytes and populate this structure (via protobuf code)
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesRcvd)

            # demultiplex the message based on the message type but let the application
            # object handle the contents as it is best positioned to do so. See how we make
            # the upcall on the application object by using the saved handle to the appln object.
            #
            # Note also that we expect the return value to be the desired timeout to use
            # in the next iteration of the poll.
            if (disc_resp.msg_type == discovery_pb2.TYPE_REGISTER):
                self.logger.info("SubscriberMW::handle_reply - Registration response received.")
                # let the appln level object decide what to do
                timeout = self.upcall_obj.register_response(disc_resp.register_resp)
                
            elif (disc_resp.msg_type == discovery_pb2.TYPE_ISREADY):
                self.logger.info("SubscriberMW::handle_reply - Ready response received.")
                # this is a response to is ready request. let the appln level object decide what to do
                timeout = self.upcall_obj.isready_response (disc_resp.isready_resp)

            else: # anything else is unrecognizable by this object
                # raise an exception here
                raise ValueError ("Unrecognized response message")

            return timeout

        except Exception as e:
            raise e

    def handle_message(self):
        """ Handle incoming message from a publisher """
        try:
            message = self.sub.recv_string()
            self.logger.info(f"SubscriberMW::handle_message - received: {message}")
        except Exception as e:
            raise e

    ########################################
    # register with the discovery service
    #
    # this method is invoked by application object passing the necessary
    # details but then as a middleware object it is our job to do the
    # serialization using the protobuf generated code
    #
    # No return value from this as it is handled in the invoke_operation
    # method of the application object.
    ########################################
    def register(self, name):
        """ Register with the discovery service as a subscriber """
        try:
            self.logger.info("SubscriberMW::register")

            # as part of registration with the discovery service, we send
            # what role we are playing, the list of topics we are publishing,
            # and our whereabouts, e.g., name, IP and port
            # The following code shows serialization using the protobuf generated code.

            # Build the Registrant Info message first.
            self.logger.debug ("SubscriberMW::register - populate the Registrant Info")
            reg_info = discovery_pb2.RegistrantInfo() # allocate
            reg_info.id = name  # our id
            reg_info.addr = self.addr  # our advertised IP addr where we are subscribing ?????
            reg_info.port = self.port # port on which we are subscribing ?????
            self.logger.debug ("SubscriberMW::register - done populating the Registrant Info")

            # Next build a RegisterReq message
            self.logger.debug ("SubscriberMW::register - populate the nested register req")
            register_req = discovery_pb2.RegisterReq()  # allocate 
            register_req.role = discovery_pb2.ROLE_SUBSCRIBER  # we are a subscriber
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            register_req.info.CopyFrom(reg_info)    # copy contents of inner structure
            #register_req.topiclist[:] = topiclist   # this is how repeated entries are added (or use append() or extend ()
            self.logger.debug ("SubscriberMW::register - done populating nested RegisterReq")

            # Finally, build the outer layer DiscoveryReq Message
            self.logger.debug ("SubscriberMW::register - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq ()  # allocate
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.register_req.CopyFrom(register_req)
            self.logger.debug ("SubscriberMW::register - done building the outer message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString ()
            self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

            # now send this to our discovery service
            self.logger.debug ("SubscriberMW::register - send stringified buffer to Discovery service")
            self.req.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes

            # now go to our event loop to receive a response to this request
            self.logger.info ("SubscriberMW::register - sent register message and now wait for reply")

        except Exception as e:
            raise e

    def disable_event_loop(self):
        """ Disable the event loop """
        self.handle_events = False

    ########################################
    # here we save a pointer (handle) to the application object
    ########################################
    def set_upcall_handle (self, upcall_obj):
        ''' set upcall handle '''
        self.upcall_obj = upcall_obj

