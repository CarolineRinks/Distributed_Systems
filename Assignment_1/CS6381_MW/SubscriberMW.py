import zmq
import os
import sys
import time
import logging
from CS6381_MW import discovery_pb2
class SubscriberMW:
    def __init__(self, logger, topiclist):
        self.logger = logger
        self.req = None  # ZMQ REQ socket to talk to Discovery service
        self.sub = None  # ZMQ SUB socket for receiving data from publishers
        self.poller = None  # ZMQ poller for handling events
        self.addr = None  # Local address
        self.port = None  # Port number for subscription
        self.topiclist = topiclist  # List of topics to subscribe to
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
            self.logger.debug ("SubscriberMW::configure - connect to Discovery service")
            connect_str = f"tcp://{args.discovery}"
            self.req.connect(connect_str)
            # Subscribe to only the topics of interest
            for topic in self.topiclist:
                self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)
            self.logger.info("SubscriberMW::configure completed")
        except Exception as e:
            raise e
        
    def configure_sources(self, sources):
        """ Configure sources for the SubscriberMW """
        try:
            self.logger.info("SubscriberMW::configure_sources")
            # Configure sources for the SubscriberMW
            # For each source in the sources list, connect to the source
            for source in sources:
                id = source.id
                addr = source.addr
                port = source.port
                topics = source.topics
                connect_str = f"tcp://{addr}:{port}"
                self.sub.connect(connect_str)
            self.logger.info("SubscriberMW::configure_sources - completed")
        except Exception as e:
            raise e
        
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
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesRcvd)
            if (disc_resp.msg_type == discovery_pb2.TYPE_REGISTER):
                self.logger.info("SubscriberMW::handle_reply - Registration response received.")
                # let the appln level object decide what to do
                timeout = self.upcall_obj.register_response(disc_resp.register_resp)
            elif (disc_resp.msg_type == discovery_pb2.TYPE_ISREADY):
                self.logger.info("SubscriberMW::handle_reply - Ready response received.")
                # this is a response to is ready request. let the appln level object decide what to do
                timeout = self.upcall_obj.isready_response(disc_resp.isready_resp)
            elif (disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
                self.logger.info("SubscriberMW::handle_reply - Lookup response received.")
                # this is a response to lookup request. let the appln level object decide what to do
                timeout = self.upcall_obj.lookup_response(disc_resp.lookup_resp)
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
            register_req.topiclist[:] = self.topiclist
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
        
    def lookup(self, name):
        """ Retrieve Publisher info OR Broker info from Discovery Service as a subscriber """
        try:
            self.logger.info("SubscriberMW::lookup")
            # as part of registration with the discovery service, we send
            # what role we are playing, the list of topics we are publishing,
            # and our whereabouts, e.g., name, IP and port
            # The following code shows serialization using the protobuf generated code.
            # Build a LookupPubByTopicReq message
            self.logger.debug ("SubscriberMW::lookup - building the Lookup Req message")
            lookup_req = discovery_pb2.LookupPubByTopicReq()  # allocate
            lookup_req.topiclist[:] = self.topiclist
            print(f"Topiclist in Subscriber lookup {lookup_req.topiclist[:]}")
            self.logger.debug ("SubscriberMW::lookup - done building Lookup Req message")
            # Finally, build the outer layer DiscoveryReq Message
            self.logger.debug ("SubscriberMW::lookup - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq ()  # allocate
            disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC  # set message type
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.lookup_req.CopyFrom(lookup_req)
            self.logger.debug ("SubscriberMW::lookup - done building the outer message")
            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString ()
            self.logger.debug ("Stringified serialized buf = {}".format (buf2send))
            # now send this to our discovery service
            self.logger.debug ("SubscriberMW::lookup - send stringified buffer to Discovery service")
            self.req.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes
            # now go to our event loop to receive a response to this request
            self.logger.info ("SubscriberMW::lookup - sent lookup message and now wait for reply")
        except Exception as e:
            raise e
        
    def disable_event_loop(self):
        """ Disable the event loop """
        self.handle_events = False

    def set_upcall_handle (self, upcall_obj):
        ''' set upcall handle '''
        self.upcall_obj = upcall_obj