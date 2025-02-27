import os
import sys
import time
import argparse
import logging
import configparser
from enum import Enum
from CS6381_MW import discovery_pb2
from CS6381_MW.SubscriberMW import SubscriberMW
from topic_selector import TopicSelector

class SubscriberAppln():
    class State(Enum):
        INITIALIZE = 0
        CONFIGURE = 1
        REGISTER = 2
        ISREADY = 3
        RECEIVING = 4
        COMPLETED = 5
        
    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.logger = logger
        self.name = None
        self.topiclist = []
        self.lookup_strategy = None
        self.dissemination = None
        self.mw_obj = None

    def configure(self, args):
        try:
            self.logger.info("SubscriberAppln::configure")
            self.state = self.State.CONFIGURE
            self.name = args.name
            self.logger.info("SubscriberAppln::configure - parsing config.ini")
            config = configparser.ConfigParser()
            config.read('config.ini')
            self.lookup_strategy = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]
            self.topiclist.append(args.topic)
            self.logger.info("SubscriberAppln::configure - initializing middleware object")
            self.mw_obj = SubscriberMW(self.logger, self.topiclist, self.dissemination)
            self.mw_obj.configure(args, self.dissemination, self.lookup_strategy)
            self.logger.info("SubscriberAppln::configure - configuration complete")
        except Exception as e:
            raise e
        
    def driver(self):
        try:
            self.logger.info("SubscriberAppln::driver")
            self.dump()
            self.logger.info("SubscriberAppln::driver - setting up upcall handle")
            self.mw_obj.set_upcall_handle(self)
            self.state = self.State.REGISTER
            self.mw_obj.event_loop(timeout=0)
            self.logger.info("SubscriberAppln::driver completed")
        except Exception as e:
            raise e
        
    def invoke_operation(self):
        try:
            self.logger.info("SubscriberAppln::invoke_operation")
            if self.state == self.State.REGISTER:
                self.logger.info("SubscriberAppln::invoke_operation - registering with Discovery")
                self.mw_obj.register(self.name)
                return None
            elif self.state == self.State.RECEIVING:
                self.logger.info("SubscriberAppln::invoke_operation - in RECEIVING state; waiting for publications")
                time.sleep(1)
                return None
            elif self.state == self.State.COMPLETED:
                self.mw_obj.disable_event_loop()
                return None
            else:
                raise ValueError("Undefined state of the appln object")
        except Exception as e:
            raise e

        
    def register_response(self, reg_resp):
        try:
            self.logger.info("SubscriberAppln::register_response")
            if reg_resp.status == discovery_pb2.STATUS_SUCCESS:
                self.logger.info("SubscriberAppln::register_response - registration successful")
                self.mw_obj.lookup(self.name)
                self.state = self.State.RECEIVING
                return 0
            else:
                self.logger.info(f"SubscriberAppln::register_response - registration failure: {reg_resp.reason}")
                raise ValueError("Subscriber needs a unique id")
        except Exception as e:
            raise e
        
    def lookup_response(self, lookup_resp):
        try:
            self.logger.info("SubscriberAppln::lookup_response")
            if lookup_resp.status:
                self.logger.info("SubscriberAppln::lookup_response - lookup successful")
                if self.dissemination == "Direct":
                    # In Direct mode, configure the SUB socket to connect to each publisher.
                    self.logger.info("SubscriberAppln::lookup_response - configuring sources for Direct mode")
                    self.mw_obj.configure_sources(lookup_resp.publishers)
                    self.state = self.State.RECEIVING
                else:
                    # In ViaBroker mode, the middleware already connects to the broker.
                    self.state = self.State.RECEIVING
            else:
                self.logger.info(f"SubscriberAppln::lookup_response - lookup failure: {lookup_resp.reason}")
                raise ValueError("Could not lookup publisher info")
            return 0
        except Exception as e:
            raise e

    # def isready_response(self, isready_resp):
    #     try:
    #         self.logger.info("SubscriberAppln::isready_response")
    #         if not isready_resp.status:
    #             self.logger.info("SubscriberAppln::isready_response - not ready; retrying")
    #             time.sleep(10)
    #         else:
    #             self.state = self.State.ISREADY
    #         return 0
    #     except Exception as e:
    #         raise e

    def broker_lookup_response(self, broker_info):
        """Upcall invoked when broker lookup is complete. Disable further lookups."""
        try:
            self.logger.info("SubscriberAppln::broker_lookup_response - broker lookup complete")
            self.state = self.State.RECEIVING
            # self.mw_obj.disable_lookup()
            return 0
        except Exception as e:
            raise e

    def dump(self):
        try:
            self.logger.info("**********************************")
            self.logger.info("SubscriberApp::dump")
            self.logger.info("------------------------------")
            self.logger.info(f"     Name: {self.name}")
            self.logger.info(f"     Dissemination: {self.dissemination}")
            self.logger.info(f"     TopicList: {self.topiclist}")
            self.logger.info("**********************************")
        except Exception as e:
            raise e

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Subscriber Application")
    parser.add_argument("-n", "--name", default="sub", help="Unique name for the subscriber")
    parser.add_argument("-t", "--topic", default="weather", help="Topic the subscriber is interested in")
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="Discovery service IP:Port")
    parser.add_argument("-c", "--config", default="config.ini", help="Configuration file")
    parser.add_argument("-a", "--addr", default="localhost", help="IP address to advertise")
    parser.add_argument("-p", "--port", type=int, default=5578, help="Port for the subscriber")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR], help="Logging level")
    return parser.parse_args()

def main():
    try:
        logger = logging.getLogger("SubscriberAppln")
        logger.info("Main: parse command line arguments")
        args = parseCmdLineArgs()
        logger.info(f"Main: resetting log level to {args.loglevel}")
        logger.setLevel(args.loglevel)
        logger.info(f"Main: effective log level is {logger.getEffectiveLevel()}")
        logger.info("Main: obtaining SubscriberAppln object")
        app = SubscriberAppln(logger)
        logger.info("Main: configuring SubscriberAppln object")
        app.configure(args)
        logger.info("Main: invoking SubscriberAppln driver")
        app.driver()
    except Exception as e:
        logger.error(f"Exception caught in main - {e}")
        return
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()
