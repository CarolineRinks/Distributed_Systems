###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise to the student. Design the logic in a manner similar
# to the PublisherAppln. As in the publisher application, the subscriber application
# will maintain a handle to the underlying subscriber middleware object.
#
# The key steps for the subscriber application are
# (1) parse command line and configure application level parameters
# (2) obtain the subscriber middleware object and configure it.
# (3) As in the publisher, register ourselves with the discovery service
# (4) since we are a subscriber, we need to ask the discovery service to
# let us know of each publisher that publishes the topic of interest to us. Then
# our middleware object will connect its SUB socket to all these publishers
# for the Direct strategy else connect just to the broker.
# (5) Subscriber will always be in an event loop waiting for some matching
# publication to show up. We also compute the latency for dissemination and
# store all these time series data in some database for later analytics.

import argparse
import logging
from CS6381_MW import discovery_pb2
from CS6381_MW.SubscriberMW import SubscriberMW

class SubscriberApp:
    def __init__(self, logger):
        self.logger = logger
        self.name = None
        self.topiclist = None
        self.mw_obj = None  # SubscriberMW middleware object

    def configure(self, args):
        """ Initialize the SubscriberApp """
        try:
            self.logger.info("SubscriberApp::configure")
            self.name = args.name
            self.topiclist = args.topiclist

            # Set up middleware
            self.mw_obj = SubscriberMW(self.logger)
            self.mw_obj.configure(args)
            self.logger.info("SubscriberApp::configure - completed")
        except Exception as e:
            raise e

    def driver(self):
        """ Driver function for SubscriberApp """
        try:
            self.logger.info("SubscriberApp::driver")
            self.mw_obj.register("subscriber", self.name, self.topiclist)

            # Now, run the event loop to wait for responses and messages
            self.mw_obj.event_loop()
        except Exception as e:
            raise e

    def dump(self):
        """ Dump the contents of the application state """
        try:
            self.logger.info("SubscriberApp::dump")
            self.logger.info(f"Name: {self.name}")
            self.logger.info(f"Topics: {self.topiclist}")
        except Exception as e:
            raise e

def parseCmdLineArgs():
    """ Command line argument parser """
    parser = argparse.ArgumentParser(description="Subscriber Application")

    parser.add_argument("-n", "--name", required=True, help="Unique name for the subscriber")
    parser.add_argument("-t", "--topiclist", nargs='+', required=True, help="List of topics to subscribe to")
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="Discovery service IP:Port")
    parser.add_argument("-a", "--addr", default="localhost", help="IP address to advertise")
    parser.add_argument("-p", "--port", type=int, default=5578, help="Port number for the service")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], help="Logging level")

    return parser.parse_args()

def main():
    try:
        logger = logging.getLogger("SubscriberApp")
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        args = parseCmdLineArgs()
        logger.setLevel(args.loglevel)

        # Set up and configure the SubscriberApp
        app = SubscriberApp(logger)
        app.configure(args)

        # Run the driver to start the application
        app.driver()
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    main()
