###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Broker application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Broker is involved only when
# the dissemination strategy is via the broker.
#
# A broker is an intermediary; thus it plays both the publisher and Broker roles
# but in the form of a proxy. For instance, it serves as the single Broker to
# all publishers. On the other hand, it serves as the single publisher to all the Brokers. 

# Broker serves as single subscriber to all publishers
# Broker also serves as single publisher to all subscribers

import os     # for OS functions
import sys    # for syspath and system exception
import time
import argparse
import logging
import configparser # for configuration parsing
from enum import Enum
from CS6381_MW import discovery_pb2
from CS6381_MW.BrokerMW import BrokerMW

class BrokerAppln():

    class State (Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        ISREADY = 3,
        RECEIVING = 4,
        COMPLETED = 5

    def __init__(self, logger):
        self.name = None    # name of the broker
        self.state = self.State.INITIALIZE
        self.logger = logger
        self.mw_obj = None  # BrokerMW middleware object
        # Booleans to indicate that Broker setup is complete:
        self.pubs_found = False
        self.subs_found = False

    def configure(self, args):
        ''' Initialize the BrokerAppln object '''
        try:
            self.logger.info("BrokerAppln::configure")
            # set variables
            self.state = self.State.CONFIGURE
            self.name = args.name

            # Now, get the configuration object
            self.logger.debug ("BrokerAppln::configure - parsing config.ini")
            config = configparser.ConfigParser()
            config.read(args.config)

            # Now setup up our underlying middleware object to which we delegate everything
            self.logger.debug ("BrokerAppln::configure - initialize the middleware object")
            self.mw_obj = BrokerMW(self.logger, self.topiclist)
            # pass remainder of the args to the m/w object
            self.mw_obj.configure(args) 
            self.logger.info("BrokerAppln::configure - configuration complete")

        except Exception as e:
            raise e
        
    def driver(self):
        """ Driver function for BrokerApp """
        try:
            self.logger.info("BrokerAppln::driver")
            # dump our contents (debugging purposes)
            self.dump ()
            # First ask our middleware to keep a handle to us to make upcalls.
            self.logger.debug ("BrokerAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle(self)
            # Next step is to register with Discovery service. Change state for event handler
            self.state = self.State.REGISTER
            self.mw_obj.event_loop(timeout=0)  # start the event loop
            self.logger.info ("BrokerAppln::driver completed")

        except Exception as e:
            raise e
        
    def invoke_operation (self):
        ''' Invoke operation depending on state '''
        try:
            self.logger.info("BrokerAppln::invoke_operation")
       
            if (self.state == self.State.REGISTER):
                # Send register message to Discovery service
                self.logger.debug ("BrokerAppln::invoke_operation - register with the discovery service")
                self.mw_obj.register(self.name)
                return None
            
            elif (self.state == self.State.ISREADY and self.pubs_found == False):
                # Send lookup request to Discovery Service to get all Publishers info
                self.logger.debug ("BrokerAppln::invoke_operation - lookup pubs with the discovery service")
                self.mw_obj.lookup_pubs()
                return None             # wait for response from discovery service
            
            elif (self.state == self.State.ISREADY and self.subs_found == False):
                # Send lookup request to Discovery Service to get all Subscribers info
                self.logger.debug ("BrokerAppln::invoke_operation - lookup subs with the discovery service")
                self.mw_obj.lookup_subs()
                return None            # wait for response from discovery service
            
            elif (self.state == self.State.RECEIVING):
                self.logger.debug ("BrokerAppln::invoke_operation - receiving topic data from pubs")
                return None             # wait to receive data
            
            elif (self.state == self.State.COMPLETED):
                # we are done. Time to break the event loop. So we created this special method on the
                # middleware object to kill its event loop
                self.mw_obj.disable_event_loop()
                return None
            
            else:
                raise ValueError ("Undefined state of the appln object")
            self.logger.info ("BrokerAppln::invoke_operation completed")
        except Exception as e:
            raise e
        
    def register_response (self, reg_resp):
        ''' handle register response '''
        try:
            self.logger.info ("BrokerAppln::register_response")
            if (reg_resp.status == discovery_pb2.STATUS_SUCCESS):
                self.logger.debug ("BrokerAppln::register_response - registration is a success")
                # set our next state to isready so that we can then send the isready message right away
                self.state = self.State.ISREADY
                # return a timeout of zero so that the event loop in its next iteration will immediately make
                # an upcall to us
                return 0
            else:
                self.logger.debug ("BrokerAppln::register_response - registration is a failure with reason {}".format (reg_resp.reason))
                raise ValueError ("Broker needs to have unique id")
        except Exception as e:
            raise e
        
    def lookup_response (self, lookup_resp):
        ''' handle lookup response from Discovery service'''
        try:
            self.logger.info ("BrokerAppln::lookup_response")
            # Parse response and connect to publishers
            if lookup_resp.status:

                # check what type of lookup response we have received
                if (lookup_resp.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS):
                    self.pubs_found = True
                    # connect to the publishers
                    self.logger.debug ("BrokerAppln::lookup_response - connecting to publishers")
                    self.logger.info(f"Lookup response: {lookup_resp}")
                    self.mw_obj.configure_publishers(lookup_resp.publishers)
                elif (lookup_resp.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_SUBS):
                    self.subs_found = True
                    self.logger.debug ("BrokerAppln::lookup_response - connecting to subscribers")
                    self.logger.info(f"Lookup response: {lookup_resp}")
                    self.mw_obj.configure_subscribers(lookup_resp.subscribers)
                else:
                    raise ValueError ("Invalid lookup response type")
                
                if (self.pubs_found and self.subs_found):
                    # we are ready for broker to start receiving topic data from publishers
                    self.state = self.State.RECEIVING
            else:
                self.logger.debug ("BrokerAppln::lookup_response - lookup is a failure with reason {}".format (lookup_resp.reason))
                raise ValueError ("Could not lookup pub info")
            
            self.logger.info ("BrokerAppln::lookup_response - complete")
        except Exception as e:
            raise e
        
    def isready_response (self, isready_resp):
        ''' handle isready response from Discovery service'''
        try:
            self.logger.info ("BrokerAppln::isready_response")

            if not isready_resp.status:
                # discovery service is not ready yet
                self.logger.debug ("BrokerAppln::driver - Not ready yet; check again")
                time.sleep (10)
            else:
                # we got the go ahead
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
            self.logger.info("BrokerApp::dump")
            self.logger.info ("------------------------------")
            self.logger.info ("     pubs_found: {}".format (self.pubs_found))
            self.logger.info ("     subs_found: {}".format (self.subs_found))
            self.logger.info ("**********************************")
        except Exception as e:
            raise e
        
def parseCmdLineArgs():
    """ Command line argument parser """
    parser = argparse.ArgumentParser(description="Broker Application")
    parser.add_argument("-n", "--name", default="broker", help="Unique name for the Broker")
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="Discovery service IP:Port")
    parser.add_argument ("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")
    parser.add_argument("-a", "--addr", default="localhost", help="IP address to advertise")
    parser.add_argument("-p", "--port", type=int, default=5581, help="Port number for the service")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], help="Logging level")
    return parser.parse_args()

def main():
    try:
        logger = logging.getLogger("BrokerAppln")
        #logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger.debug ("Main: parse command line arguments")
        args = parseCmdLineArgs()
        # reset the log level to as specified
        logger.debug ("Main: resetting log level to {}".format (args.loglevel))
        logger.setLevel (args.loglevel)
        logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))
        # Obtain BrokerAppln object
        logger.debug ("Main: obtain the Broker appln object")
        app = BrokerAppln(logger)
        # configure the object
        logger.debug ("Main: configure the publisher appln object")
        app.configure(args)
        # Run the driver to start the application
        logger.debug ("Main: invoke the Broker appln driver")
        app.driver()
    except Exception as e:
        logger.error ("Exception caught in main - {}".format (e))
        return
    
if __name__ == "__main__":
    # set underlying default logging capabilities
    logging.basicConfig (level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main ()