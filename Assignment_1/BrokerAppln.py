import logging
import argparse
import configparser
from CS6381_MW.BrokerMW import BrokerMW

class BrokerAppln:
    """ Broker application that manages communication between publishers and subscribers """

    def __init__(self, logger):
        """ Constructor """
        self.logger = logger
        self.mw_obj = BrokerMW(logger)  # Initialize Broker Middleware

    def configure(self, args):
        """ Configure middleware and read necessary configurations """
        self.logger.info("BrokerAppln::configure")

        # Read config file for dissemination mode
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.dissemination_mode = config['Dissemination']['Strategy']

        if self.dissemination_mode != "ViaBroker":
            self.logger.error("BrokerAppln::configure - Broker should not be running in Direct mode")
            exit(1)

        self.logger.info("BrokerAppln::configure - Configuring Broker Middleware")
        self.mw_obj.configure(config)

    def register_publisher(self, pub_id, topic_list):
        """ Register a publisher with the broker """
        self.logger.info(f"BrokerAppln::register_publisher - Registering publisher {pub_id} with topics {topic_list}")
        self.mw_obj.register_publishers(pub_id, topic_list)

    def register_subscriber(self, sub_id, topic_list):
        """ Register a subscriber with the broker """
        self.logger.info(f"BrokerAppln::register_subscriber - Registering subscriber {sub_id} with topics {topic_list}")
        self.mw_obj.register_subscriber(sub_id, topic_list)

    def run(self):
        """ Start the broker event loop """
        self.logger.info("BrokerAppln::run - Starting event loop")
        self.mw_obj.event_loop()

def main():
    """ Main function to start the broker """
    logger = logging.getLogger("BrokerAppln")
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Broker Application")
    parser.add_argument("--port", type=int, required=False, help="Port for the broker (should be defined in config.ini)")
    args = parser.parse_args()

    app = BrokerAppln(logger)
    app.configure(args)
    app.run()

if __name__ == "__main__":
    main()
