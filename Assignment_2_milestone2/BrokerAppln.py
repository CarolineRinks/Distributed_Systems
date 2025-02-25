import logging
import argparse
import configparser
from CS6381_MW.BrokerMW import BrokerMW

class BrokerAppln:
    """Broker application that manages communication between publishers and subscribers."""

    def __init__(self, logger):
        """Constructor."""
        self.logger = logger
        self.mw_obj = BrokerMW(logger)  # Initialize Broker Middleware

    def configure(self, args):
        """Configure middleware and read necessary configurations."""
        self.logger.info("BrokerAppln::configure")
        # Read config file.
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.dissemination_mode = config['Dissemination']['Strategy']

        if self.dissemination_mode != "ViaBroker":
            self.logger.error("BrokerAppln::configure - Broker should not be running in Direct mode")
            exit(1)

        self.logger.info("BrokerAppln::configure - Configuring Broker Middleware")
        

        # Register the broker with the Discovery service.

        broker_id=args.name
        broker_ip = args.addr
        broker_front_end_port=args.pub_port
        broker_back_end_port = args.sub_port
        self.mw_obj.configure(broker_id, broker_ip, broker_front_end_port,broker_back_end_port)
        self.mw_obj.register_broker(broker_id, broker_ip, broker_front_end_port,broker_back_end_port)
        self.logger.info("BrokerAppln::configure - Broker registration complete")

    # def register_subscriber(self, sub_id, topic_list):
    #     """Register a subscriber with the broker."""
    #     self.logger.info(f"BrokerAppln::register_subscriber - Registering subscriber {sub_id} with topics {topic_list}")
    #     self.mw_obj.register_subscriber(sub_id, topic_list)

    def run(self):
        """Start the broker event loop."""
        self.logger.info("BrokerAppln::run - Starting event loop")
        self.mw_obj.event_loop()

def main():
    """Main function to start the broker."""
    logger = logging.getLogger("BrokerAppln")
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Broker Application")
    parser.add_argument("-a", "--addr", default= "localhost", required=False, help="IP address to advertise")
    parser.add_argument("-c", "--config", default="config.ini", help="Configuration file")
    parser.add_argument("-n", "--name", default="broke", help="Unique name for the broker")
    parser.add_argument("-sp", "--sub_port", type=int, default = 6000, required=True, help="port that subscriber will connect to")
    parser.add_argument("-pp", "--pub_port", type=int, default = 6001, required=True, help="port that publisher will connect to")
    args = parser.parse_args()

    app = BrokerAppln(logger)
    app.configure(args)
    app.run()

if __name__ == "__main__":
    main()
