import argparse
import logging
import sys
from CS6381_MW.DiscoveryMW import DiscoveryMW

class DiscoveryApp:
    """Discovery Service Application."""
    def __init__(self, logger):
        self.logger = logger
        self.mw_obj = None  # The Discovery middleware object

    def configure(self, args):
        """Configure the Discovery application."""
        try:
            self.logger.info("DiscoveryApp::configure")
            self.mw_obj = DiscoveryMW(self.logger)
            self.mw_obj.configure(args)
            self.logger.info("DiscoveryApp::configure - configuration complete")
        except Exception as e:
            self.logger.error(f"DiscoveryApp::configure - Exception: {e}")
            raise e

    def run(self):
        """Run the discovery service event loop."""
        try:
            self.logger.info("DiscoveryApp::run - starting event loop")
            self.mw_obj.event_loop()
        except Exception as e:
            self.logger.error(f"DiscoveryApp::run - Exception: {e}")
            raise e

def parseCmdLineArgs():
    """Command line argument parser."""
    parser = argparse.ArgumentParser(description="Discovery Service")
    parser.add_argument("-a", "--addr", default="localhost", help="IP address to advertise for Discovery (default: localhost)")
    parser.add_argument("-p", "--port", default=5555, type=int, help="Port for the Discovery Service")
    parser.add_argument("--num_pubs", type=int, required=True, help="Expected number of publishers")
    parser.add_argument("--num_subs", type=int, required=True, help="Expected number of subscribers")
    return parser.parse_args()

def main():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("DiscoveryApp")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    args = parseCmdLineArgs()
    app = DiscoveryApp(logger)
    app.configure(args)
    app.run()

if __name__ == "__main__":
    main()
