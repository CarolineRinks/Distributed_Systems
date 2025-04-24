import logging
import argparse
import configparser
from CS6381_MW.BrokerMW import BrokerMW

class BrokerAppln:
    """Broker application that manages communication between publishers and subscribers."""

    def __init__(self, logger,args):
        """Constructor."""
        self.logger = logger
        self.configure(args)
        self.mw_obj = BrokerMW(logger,args)  # Initialize Broker Middleware

    def configure(self, args):
        """Configure middleware and read necessary configurations."""
        self.logger.info("BrokerAppln::configure")
        # Read config file.
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.dissemination_mode = config['Dissemination']['Strategy']
        self.logger.info("BrokerAppln::configure - Dissemination strategy:" + self.dissemination_mode)

        if self.dissemination_mode != "ViaBroker":
            self.logger.error("BrokerAppln::configure - Broker should not be running in Direct mode")
            exit(1)

        self.logger.info("BrokerAppln::configure - Configuring Broker Middleware")
        
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
    parser.add_argument("-g", "--group", type=int, required=True, help="Group Number for the Discovery Service")
    args = parser.parse_args()

    app = BrokerAppln(logger,args)
    # app.configure(args)
    app.run()

if __name__ == "__main__":
    main()
