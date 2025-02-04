###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Discovery application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Discovery service is a server
# and hence only responds to requests. It should be able to handle the register,
# is_ready, the different variants of the lookup methods. etc.
#
# The key steps for the discovery application are
# (1) parse command line and configure application level parameters. One
# of the parameters should be the total number of publishers and subscribers
# in the system.
# (2) obtain the discovery middleware object and configure it.
# (3) since we are a server, we always handle events in an infinite event loop.
# See publisher code to see how the event loop is written. Accordingly, when a
# message arrives, the middleware object parses the message and determines
# what method was invoked and then hands it to the application logic to handle it
# (4) Some data structure or in-memory database etc will need to be used to save
# the registrations.
# (5) When all the publishers and subscribers in the system have registered with us,
# then we are in a ready state and will respond with a true to is_ready method. Until then
# it will be false.

import argparse
import logging
from CS6381_MW import discovery_pb2
from CS6381_MW.DiscoveryMW import DiscoveryMW

class DiscoveryApp:
    def __init__(self, logger):
        self.logger = logger
        self.name = None
        self.topiclist = None
        self.mw_obj = None  # DiscoveryMW middleware object

    def configure(self, args):
        """ Initialize the DiscoveryApp """
        try:
            self.logger.info("DiscoveryApp::configure")
            self.name = args.name
            self.topiclist = args.topiclist

            # Set up middleware
            self.mw_obj = DiscoveryMW(self.logger)
            self.mw_obj.configure(args)
            self.logger.info("DiscoveryApp::configure - completed")
        except Exception as e:
            raise e

    def driver(self):
        """ Driver function for DiscoveryApp """
        try:
            self.logger.info("DiscoveryApp::driver")
            self.mw_obj.register("publisher", self.name, self.topiclist)

            # Now, run the event loop to wait for responses
            self.mw_obj.event_loop()
        except Exception as e:
            raise e

    def dump(self):
        """ Dump the contents of the application state """
        try:
            self.logger.info("DiscoveryApp::dump")
            self.logger.info(f"Name: {self.name}")
            self.logger.info(f"Topics: {self.topiclist}")
        except Exception as e:
            raise e

def parseCmdLineArgs():
    """ Command line argument parser """
    parser = argparse.ArgumentParser(description="Discovery Application")

    parser.add_argument("-n", "--name", required=True, help="Unique name for the publisher/subscriber")
    parser.add_argument("-t", "--topiclist", nargs='+', required=True, help="List of topics")
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="Discovery service IP:Port")
    parser.add_argument("-a", "--addr", default="localhost", help="IP address to advertise")
    parser.add_argument("-p", "--port", type=int, default=5577, help="Port number for the service")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], help="Logging level")

    return parser.parse_args()

def main():
    try:
        logger = logging.getLogger("DiscoveryApp")
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        args = parseCmdLineArgs()
        logger.setLevel(args.loglevel)

        # Set up and configure the DiscoveryApp
        app = DiscoveryApp(logger)
        app.configure(args)

        # Run the driver to start the application
        app.driver()
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    main()
