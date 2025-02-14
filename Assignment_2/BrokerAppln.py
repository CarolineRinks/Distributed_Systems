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
        self.mw_obj.configure(config)

        # Register the broker with the Discovery service.
        # Assume the config file has a [Broker] section with an id.
        if 'Broker' in config and 'id' in config['Broker']:
            broker_id = config['Broker']['id']
        else:
            broker_id = "broker1"  # default id

        # We use the broker_ip from the [Settings] section and advertise the backend port.
        broker_ip = config['Settings']['broker_ip']
        broker_front_end_port=config['Settings']['frontend_port']
        broker_back_end_port = config['Settings']['backend_port']
        self.mw_obj.register_broker(broker_id, broker_ip, broker_front_end_port,broker_back_end_port)
        self.logger.info("BrokerAppln::configure - Broker registration complete")

    def register_subscriber(self, sub_id, topic_list):
        """Register a subscriber with the broker."""
        self.logger.info(f"BrokerAppln::register_subscriber - Registering subscriber {sub_id} with topics {topic_list}")
        self.mw_obj.register_subscriber(sub_id, topic_list)

    def run(self):
        """Start the broker event loop."""
        self.logger.info("BrokerAppln::run - Starting event loop")
        self.mw_obj.event_loop()

def main():
    """Main function to start the broker."""
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
