"""
Supplier Stock-Checking Application
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
import sys
import logging
import signal
from endpoint_tester import EndpointTester

# Test parameters
ENDPOINT_URL = "http://localhost:5000/stocks/graphql-query"
# From within a container in the same network: http://log430-a25-labo3-store_manager:5000/stocks/graphql-query
TEST_PAYLOAD = "{\"query\":\"{\\n  product(id: \\\"1\\\") {\\n    id\\n    name\\n    quantity\\n  }\\n}\\n\",\"variables\":{}}"
INTERVAL_SECONDS = 10  
TIMEOUT_SECONDS = 10   
MAX_RETRIES = 3  

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('endpoint_calls.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully."""
    logger.info("Received interrupt signal")
    sys.exit(0)

# App entrypoint
if __name__ == "__main__":
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    # Create and run the tester
    caller = EndpointTester(
        url=ENDPOINT_URL,
        payload=TEST_PAYLOAD,
        interval=INTERVAL_SECONDS,
        timeout=TIMEOUT_SECONDS,
        max_retries=MAX_RETRIES,
        logger=logger
    )
    
    try:
        caller.run()
    except KeyboardInterrupt:
        caller.stop()
    finally:
        logger.info("Script terminated")