"""
Endpoint Tester
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
import time
import requests

class EndpointTester:
    """ A class that contains method to periodically call a specified endpoint with error handling and logging """
    def __init__(self, url, payload, logger, interval=30, timeout=10, max_retries=3):
        self.url = url
        self.payload = payload
        self.logger = logger
        self.interval = interval
        self.timeout = timeout
        self.max_retries = max_retries
        self.running = True
        self.call_count = 0
        self.success_count = 0
        self.error_count = 0

    def do_post_request(self):
        """Make a single request to the endpoint with retry logic."""
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Calling {self.url} (attempt {attempt + 1}/{self.max_retries})")
                
                response = requests.post(
                    self.url, 
                    timeout=self.timeout,
                    data=self.payload,
                    headers={
                        'Content-Type': 'application/json'
                    }
                )
                
                # Log response details
                self.logger.info(f"Response: {response.status_code} - {response.reason}")
                if response.text:
                    self.logger.info(f"Response body: {response.text[:200]}...")  # First 200 chars
                
                # Consider 2xx status codes as success
                if 200 <= response.status_code < 300:
                    self.success_count += 1
                    return True
                else:
                    self.logger.warning(f"Unexpected status code: {response.status_code}")
                    
            except requests.exceptions.Timeout:
                self.logger.error(f"Timeout on attempt {attempt + 1}")
            except requests.exceptions.ConnectionError:
                self.logger.error(f"Connection error on attempt {attempt + 1}")
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed on attempt {attempt + 1}: {e}")
            
            if attempt < self.max_retries - 1:
                time.sleep(2)  # Wait 2 seconds before retry
        
        self.error_count += 1
        self.logger.error(f"All {self.max_retries} attempts failed for {self.url}")
        return False

    def run(self):
        """Main loop that calls the endpoint periodically."""
        self.logger.info(f"Starting periodic calls to {self.url} every {self.interval} seconds")
        self.logger.info("Press Ctrl+C to stop")
        
        while self.running:
            try:
                self.call_count += 1
                self.logger.info(f"--- Call #{self.call_count} ---")
                
                self.do_post_request()
                
                # Print statistics every 10 calls
                if self.call_count % 10 == 0:
                    success_rate = (self.success_count / self.call_count) * 100
                    self.logger.info(f"Statistics: {self.call_count} calls, "
                              f"{self.success_count} successful ({success_rate:.1f}%), "
                              f"{self.error_count} errors")
                
                # Wait for the specified interval
                self.logger.info(f"Waiting {self.interval} seconds until next call...")
                time.sleep(self.interval)
                
            except KeyboardInterrupt:
                self.stop()
                break
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                time.sleep(5)  # Wait a bit before continuing

    def stop(self):
        """Stop the periodic calls."""
        self.running = False
        self.logger.info("Stopping periodic calls...")
        self.logger.info(f"Final statistics: {self.call_count} total calls, "
                   f"{self.success_count} successful, {self.error_count} errors")
