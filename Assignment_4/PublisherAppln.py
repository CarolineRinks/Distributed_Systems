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
import argparse
import configparser
import logging
from CS6381_MW import discovery_pb2
from CS6381_MW.PublisherMW import PublisherMW
from google.protobuf import text_format
from topic_selector import TopicSelector
from enum import Enum

class PublisherAppln():
    # Define application states. A new state LOOKUP_BROKER is added.
    class State(Enum):
        INITIALIZE   = 0
        CONFIGURE    = 1
        REGISTER     = 2
        ISREADY      = 3
        LOOKUP_BROKER = 4  # New state for broker lookup in ViaBroker mode
        DISSEMINATE  = 5
        COMPLETED    = 6

    def __init__(self, logger):
        self.state = self.State.INITIALIZE  # current state
        self.name = None                    # publisher's unique name
        self.topiclist = None               # list of topics to publish
        self.iters = None                   # number of publication iterations
        self.frequency = None               # rate of dissemination
        self.history_size = None
        self.num_topics = None              # number of topics to publish
        self.lookup = None                  # lookup strategy (from config)
        self.dissemination = None           # dissemination strategy ("Direct" or "ViaBroker")
        self.mw_obj = None                  # middleware object handle
        self.logger = logger                # internal logger


    def configure(self, args):
        '''Initialize the PublisherAppln object.'''
        try:
            self.logger.info("PublisherAppln::configure")
            self.state = self.State.CONFIGURE

            # Read commandline args
            self.name = args.name
            self.iters = args.iters
            self.frequency = args.frequency
            self.num_topics = args.num_topics
            history_list = args.history

            # Read configuration from config.ini
            self.logger.info("PublisherAppln::configure - parsing config.ini")
            config = configparser.ConfigParser()
            config.read(args.config)
            self.dissemination = config["Dissemination"]["Strategy"]

            # Obtain topics to publish
            self.logger.info("PublisherAppln::configure - selecting our topic list")
            ts = TopicSelector()
            if args.topics:
                self.topiclist = args.topics
            else:
                self.topiclist = ts.interest(self.num_topics)

            # Configure dictionary of history sizes
            self.history_sizes = {}
            if len(history_list) != len(self.topiclist):
                for topic in self.topiclist:
                    # assign same history size to all topics if user didn't specify multiple sizes
                    self.history_sizes[topic] = int(history_list[0])
            else:
                i = 0
                for topic in self.topiclist:
                    self.history_sizes[topic] = int(history_list[i])
                    i += 1

            # Determine load balancing group (based on topics)
            og_topiclist = ["weather", "humidity", "airquality", "light", "pressure", "temperature", "sound", "altitude", "location"]
            groups = []
            for topic in self.topiclist:
                self.logger.info("PublisherAppln::configure - topic: %s", topic)
                if topic in og_topiclist[:3]:
                    groups.append(1)
                elif topic in og_topiclist[3:6]:
                    groups.append(2)
                elif topic in og_topiclist[6:]:
                    groups.append(3)
            # Assign load balancing group
            discovery_group = ""
            if len(groups) == 1:
                discovery_group = groups[0]
            else:
                discovery_group = random.choice(groups)     # pick random group if eligible for multiple groups
            
            self.logger.info("PublisherAppln::configure - Load Balancing group: %s", discovery_group)
                    
            # Initialize the middleware object and pass dissemination info.
            self.logger.info("PublisherAppln::configure - initializing the middleware object")
            self.mw_obj = PublisherMW(self.logger, discovery_group, self.history_sizes)
            self.mw_obj.set_upcall_handle(self)
            self.mw_obj.configure(args, self.dissemination)
            self.logger.info("PublisherAppln::configure - configuration complete")
        except Exception as e:
            raise e

    def driver(self):
        '''Driver function to start the publisher application.'''
        try:
            self.logger.info("PublisherAppln::driver")
            self.dump()
            self.logger.info("PublisherAppln::driver - setting up upcall handle")
            
            self.state = self.State.REGISTER
            self.mw_obj.event_loop(timeout=0)
            self.logger.info("PublisherAppln::driver completed")
        except Exception as e:
            raise e

    def invoke_operation(self):
        '''Invokes operations based on the current state.'''
        try:
            self.logger.info("PublisherAppln::invoke_operation")
            if self.state == self.State.REGISTER:
                self.logger.info("PublisherAppln::invoke_operation - registering with Discovery service")
                self.mw_obj.register(self.name, self.topiclist)
                return None
            elif self.state == self.State.ISREADY:
                self.logger.info("PublisherAppln::invoke_operation - starting dissemination")
                ts = TopicSelector()
                for i in range(self.iters):
                    for topic in self.topiclist:
                        dissemination_data = ts.gen_publication(topic)
                        self.mw_obj.disseminate(self.name, topic, dissemination_data)
                    time.sleep(1/float(self.frequency))
                self.logger.info("PublisherAppln::invoke_operation - dissemination completed")
                self.state = self.State.COMPLETED
                return 0
                return None
            elif self.state == self.State.LOOKUP_BROKER:
                self.logger.info("PublisherAppln::invoke_operation - looking up broker")
                self.mw_obj.lookup_broker()
                return None
            elif self.state == self.State.DISSEMINATE:
                self.logger.info("PublisherAppln::invoke_operation - starting dissemination")
                ts = TopicSelector()
                for i in range(self.iters):
                    for topic in self.topiclist:
                        dissemination_data = ts.gen_publication(topic)
                        self.mw_obj.disseminate(self.name, topic, dissemination_data)
                    time.sleep(1/float(self.frequency))
                    #time.sleep(10)
                self.logger.info("PublisherAppln::invoke_operation - dissemination completed")
                self.state = self.State.COMPLETED
                return 0
            elif self.state == self.State.COMPLETED:
                self.mw_obj.disable_event_loop()
                return None
            else:
                raise ValueError("Undefined state of the appln object")
        except Exception as e:
            raise e

    def register_response(self, reg_resp):
        '''Handle the registration response from Discovery service.'''
        try:
            self.logger.info("PublisherAppln::register_response")
            
            if reg_resp.status == discovery_pb2.STATUS_SUCCESS:
                self.logger.info("PublisherAppln::register_response - registration successful")
                if self.dissemination == "ViaBroker":
                    self.state = self.State.LOOKUP_BROKER
                else:
                    self.state = self.State.DISSEMINATE
                return 0
            else:
                self.logger.info("PublisherAppln::register_response - registration failure: {}".format(reg_resp.reason))
                self.state=self.State.REGISTER
                return 0
        except Exception as e:
            self.logger.error("Error printing reg_resp: %s", e)

    def broker_lookup_response(self, broker_info):
        '''Upcall from the middleware after a successful broker lookup.
           Sets the state to DISSEMINATE so that publishing can begin.'''
        try:
            self.logger.info("PublisherAppln::broker_lookup_response - broker lookup complete")
            self.state = self.State.DISSEMINATE
            return 0
        except Exception as e:
            raise e

    def dump(self):
        '''Prints out the publisher application state for debugging purposes.'''
        try:
            self.logger.info("**********************************")
            self.logger.info("PublisherAppln::dump")
            self.logger.info("------------------------------")
            self.logger.info("     Name: {}".format(self.name))
            # self.logger.info("     Lookup: {}".format(self.lookup))
            self.logger.info("     Dissemination: {}".format(self.dissemination))
            self.logger.info("     Num Topics: {}".format(self.num_topics))
            self.logger.info("     TopicList: {}".format(self.topiclist))
            self.logger.info("     Iterations: {}".format(self.iters))
            self.logger.info("     Frequency: {}".format(self.frequency))
            self.logger.info("     History Sizes: {}".format(self.history_size))
            self.logger.info("**********************************")
        except Exception as e:
            raise e

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Publisher Application")
    parser.add_argument("-n", "--name", default="pub", help="Unique name for the publisher")
    parser.add_argument("-a", "--addr", default="localhost", help="IP address to advertise")
    parser.add_argument("-p", "--port", type=int, default=5577, help="Port for the publisher's ZMQ service")
    parser.add_argument("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics to publish")
    parser.add_argument("-c", "--config", default="config.ini", help="Configuration file")
    parser.add_argument("-f", "--frequency", type=int, default=1, help="Dissemination frequency (per second)")
    parser.add_argument("-i", "--iters", type=int, default=1000, help="Number of publication iterations")
    parser.add_argument("-z", "--history", type=int, nargs="+", default=[10], help="Number(s) of publications to retain in history for topics")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO,
                        choices=[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL],
                        help="Logging level")
    parser.add_argument("-t", "--topics", type=str, nargs="+", default=None, help="Explicit topic(s) to publish")
    return parser.parse_args()

def main():
    try:
        logging.info("Main - acquiring logger")
        logger = logging.getLogger("PublisherAppln")
        logger.info("Main: parse command line arguments")
        args = parseCmdLineArgs()
        logger.info("Main: resetting log level to {}".format(args.loglevel))
        logger.setLevel(args.loglevel)
        logger.info("Main: effective log level is {}".format(logger.getEffectiveLevel()))
        logger.info("Main: obtaining PublisherAppln object")
        pub_app = PublisherAppln(logger)
        logger.info("Main: configuring PublisherAppln object")
        pub_app.configure(args)
        logger.info("Main: invoking PublisherAppln driver")
        pub_app.driver()
    except Exception as e:
        logger.error("Exception caught in main - {}".format(e))
        return

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()
