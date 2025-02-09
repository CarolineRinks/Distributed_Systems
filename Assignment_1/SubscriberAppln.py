import os     # for OS functions
import sys    # for syspath and system exception
import time
import argparse
import logging
import configparser # for configuration parsing
from enum import Enum
from CS6381_MW import discovery_pb2
from CS6381_MW.SubscriberMW import SubscriberMW
from topic_selector import TopicSelector
class SubscriberAppln():
    # these are the states through which our subscriber appln object goes thru.
    # We maintain the state so we know where we are in the lifecycle and then
    # take decisions accordingly
    class State (Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        ISREADY = 3,
        RECEIVING = 4,
        COMPLETED = 5
        
    def __init__(self, logger):
        self.state = self.State.INITIALIZE # state that are we in
        self.logger = logger
        self.name = None
        self.topiclist = []
        self.lookup = None # one of the diff ways we do lookup
        self.dissemination = None # direct or via broker
        self.mw_obj = None  # SubscriberMW middleware object

    def configure(self, args):
        """ Initialize the SubscriberAppln object """
        try:
            self.logger.info("SubscriberAppln::configure")
            # set our current state to CONFIGURE state
            self.state = self.State.CONFIGURE
            # initialize our variables
            self.name = args.name
            # Now, get the configuration object
            self.logger.debug ("SubscriberAppln::configure - parsing config.ini")
            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]
            # [TO DO: SET LIST OF TOPICS from args]

            self.topiclist.append(args.topic)
            # Now setup up our underlying middleware object to which we delegate everything
            self.logger.debug ("SubscriberAppln::configure - initialize the middleware object")
            self.mw_obj = SubscriberMW(self.logger, self.topiclist)
            self.mw_obj.configure (args) # pass remainder of the args to the m/w object
            self.logger.info("SubscriberAppln::configure - configuration complete")
        except Exception as e:
            raise e
        
    def driver(self):
        """ Driver function for SubscriberApp """
        try:
            self.logger.info("SubscriberAppln::driver")
            # dump our contents (debugging purposes)
            self.dump ()
            # First ask our middleware to keep a handle to us to make upcalls.
            self.logger.debug ("SubscriberAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle(self)
            # Next step is to register with Discovery service. Change state for event handler
            self.state = self.State.REGISTER
            self.mw_obj.event_loop(timeout=0)  # start the event loop
            self.logger.info ("SubscriberAppln::driver completed")
        except Exception as e:
            raise e
        
    def invoke_operation (self):
        ''' Invoke operation depending on state  '''
        try:
            self.logger.info("SubscriberAppln::invoke_operation")
            # check what state we are in. If we are in REGISTER state,
            # we send register request to discovery service. If we are in
            # ISREADY state, then we keep checking with the discovery
            # service.
            if (self.state == self.State.REGISTER):
                # Send register message to Discovery service
                self.logger.debug ("SubscriberAppln::invoke_operation - register with the discovery service")
                self.mw_obj.register(self.name)
                # Remember that we were invoked by the event loop as part of the upcall.
                # So we are going to return back to it for its next iteration. Because
                # we have just now sent a register request, the very next thing we expect is
                # to receive a response from remote entity. So we need to set the timeout
                # for the next iteration of the event loop to a large num and so return a None.
                return None
            elif (self.state == self.State.ISREADY):
                # Send lookup message to get Publisher information
                self.logger.debug ("SubscriberAppln::invoke_operation - lookup with the discovery service")
                self.mw_obj.lookup(self.name)
                # wait for the response from the discovery service
                return None
            elif (self.state == self.State.RECEIVING):
                self.logger.debug ("SubscriberAppln::invoke_operation - receiving topics")
                # wait to receive topics
                return None
            elif (self.state == self.State.COMPLETED):
                # we are done. Time to break the event loop. So we created this special method on the
                # middleware object to kill its event loop
                self.mw_obj.disable_event_loop ()
                return None
            else:
                raise ValueError ("Undefined state of the appln object")
            self.logger.info ("SubscriberAppln::invoke_operation completed")
        except Exception as e:
            raise e
        
    def register_response (self, reg_resp):
        ''' handle register response '''
        try:
            self.logger.info ("SubscriberAppln::register_response")
            if (reg_resp.status == discovery_pb2.STATUS_SUCCESS):
                self.logger.debug ("SubscriberAppln::register_response - registration is a success")
                # set our next state to isready so that we can then send the isready message right away
                self.state = self.State.ISREADY
                # return a timeout of zero so that the event loop in its next iteration will immediately make
                # an upcall to us
                return 0
            else:
                self.logger.debug ("SubscriberAppln::register_response - registration is a failure with reason {}".format (reg_resp.reason))
                raise ValueError ("Subscriber needs to have unique id")
        except Exception as e:
            raise e
        
    def lookup_response (self, lookup_resp):
        ''' handle lookup response from Discovery service'''
        try:
            self.logger.info ("SubscriberAppln::lookup_response")
            # Parse response and connect to publishers
            if lookup_resp.status:
                # we got the lookup response
                # connect to the publishers
                self.logger.debug ("SubscriberAppln::lookup_response - connecting to publishers")
                self.logger.info(f"Lookup response: {lookup_resp}")
                self.logger.info(f"Lookup response publishers: {lookup_resp.publishers}")
                self.mw_obj.configure_sources(lookup_resp.publishers)       # change this as needed - discovery service will return the list of publishers OR the broker
                # set the next state to listen for publications
                self.state = self.State.RECEIVING
            else:
                self.logger.debug ("SubscriberAppln::lookup_response - lookup is a failure with reason {}".format (lookup_resp.reason))
                raise ValueError ("Could not lookup pub info")
            self.logger.info ("SubscriberAppln::lookup_response - complete")
        except Exception as e:
            raise e
        
    def isready_response (self, isready_resp):
        ''' handle isready response from Discovery service'''
        try:
            self.logger.info ("SubscriberAppln::isready_response")
            # Notice how we get that loop effect with the sleep (10)
            # by an interaction between the event loop and these
            # upcall methods.
            if not isready_resp.status:
                # discovery service is not ready yet
                self.logger.debug ("SubscriberAppln::driver - Not ready yet; check again")
                time.sleep (10)  # sleep between calls so that we don't make excessive calls
            else:
                # we got the go ahead
                # set the state to disseminate
                self.state = self.State.RECEIVING
            # return timeout of 0 so event loop calls us back in the invoke_operation
            # method, where we take action based on what state we are in.
            return 0
        except Exception as e:
            raise e
        
    def dump(self):
        """ Dump the contents of the application state """
        try:
            self.logger.info ("**********************************")
            self.logger.info("SubscriberApp::dump")
            self.logger.info ("------------------------------")
            self.logger.info ("     Name: {}".format (self.name))
            self.logger.info ("     Dissemination: {}".format (self.dissemination))
            self.logger.info ("     TopicList: {}".format (self.topiclist))
            self.logger.info ("**********************************")
        except Exception as e:
            raise e
        
def parseCmdLineArgs():
    """ Command line argument parser """
    parser = argparse.ArgumentParser(description="Subscriber Application")
    parser.add_argument("-n", "--name", default="sub", help="Unique name for the subscriber")
    parser.add_argument("-t", "--topic", default="weather", help="Topics the subscriber is interested in")
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="Discovery service IP:Port")
    parser.add_argument ("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")
    parser.add_argument("-a", "--addr", default="localhost", help="IP address to advertise")
    parser.add_argument("-p", "--port", type=int, default=5578, help="Port number for the service")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], help="Logging level")
    return parser.parse_args()

def main():
    try:
        logger = logging.getLogger("SubscriberAppln")
        #logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger.debug ("Main: parse command line arguments")
        args = parseCmdLineArgs()
        # reset the log level to as specified
        logger.debug ("Main: resetting log level to {}".format (args.loglevel))
        logger.setLevel (args.loglevel)
        logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))
        # Obtain SubscriberAppln object
        logger.debug ("Main: obtain the subscriber appln object")
        app = SubscriberAppln(logger)
        # configure the object
        logger.debug ("Main: configure the publisher appln object")
        app.configure(args)
        # Run the driver to start the application
        logger.debug ("Main: invoke the subscriber appln driver")
        app.driver()
    except Exception as e:
        logger.error ("Exception caught in main - {}".format (e))
        return
    
if __name__ == "__main__":
    # set underlying default logging capabilities
    logging.basicConfig (level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main ()