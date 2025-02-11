import json
import logging
import time
import os
import http.client
from typing import List, Dict
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging - console only
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class LinkedInEmailScraper:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv('WIZA_API_KEY')
        if not self.api_key:
            raise ValueError("WIZA_API_KEY must be set in .env file")
        
        self.authorization = f"Bearer {self.api_key}"
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': self.authorization
        }
        self.max_retries = 10  # Maximum number of status checks
        self.retry_delay = 5   # Seconds between status checks

    def _make_request(self, method: str, endpoint: str, payload: dict = None) -> Dict:
        """
        Make HTTP request to Wiza API
        """
        conn = http.client.HTTPSConnection("wiza.co")
        try:
            if payload:
                conn.request(method, endpoint, json.dumps(payload), self.headers)
            else:
                conn.request(method, endpoint, None, self.headers)
            
            response = conn.getresponse()
            data = response.read()
            
            if response.status != 200:
                logging.error(f"Error in API call: {response.status} {response.reason}")
                logging.error(f"Response: {data.decode('utf-8')}")
                return None
                
            return json.loads(data.decode('utf-8'))
            
        except Exception as e:
            logging.error(f"Error making request: {str(e)}")
            return None
        finally:
            conn.close()

    def check_credits(self) -> bool:
        """
        Check available credits before processing
        """
        result = self._make_request("GET", "/api/meta/credits")
        if result:
            logging.info(f"Credits information: {result}")
            return True
        return False

    def check_reveal_status(self, reveal_id: int) -> Dict:
        """
        Check the status of a reveal request
        """
        return self._make_request("GET", f"/api/individual_reveals/{reveal_id}")

    def wait_for_completion(self, reveal_id: int) -> Dict:
        """
        Poll the reveal status until completion or max retries reached
        """
        for attempt in range(self.max_retries):
            status_response = self.check_reveal_status(reveal_id)
            
            if not status_response:
                logging.error(f"Failed to get status for reveal {reveal_id}")
                return None
                
            if status_response.get('data', {}).get('is_complete'):
                logging.info(f"Reveal {reveal_id} completed")
                return status_response
                
            if status_response.get('data', {}).get('status') == 'failed':
                logging.error(f"Reveal {reveal_id} failed")
                return status_response
                
            logging.info(f"Reveal {reveal_id} still processing (attempt {attempt + 1}/{self.max_retries})")
            time.sleep(self.retry_delay)
            
        logging.warning(f"Max retries reached for reveal {reveal_id}")
        return None

    def process_linkedin_profile(self, profile_url: str) -> Dict:
        """
        Process a single LinkedIn profile URL and return the API response
        """
        payload = {
            "individual_reveal": {
                "profile_url": profile_url
            },
            "enrichment_level": "full"
        }

        initial_response = self._make_request("POST", "/api/individual_reveals", payload)
        if not initial_response:
            return None
            
        reveal_id = initial_response.get('data', {}).get('id')
        if not reveal_id:
            logging.error("No reveal ID in response")
            return initial_response
            
        logging.info(f"Waiting for reveal {reveal_id} to complete...")
        return self.wait_for_completion(reveal_id)

    def process_agents_file(self, file_path: str) -> List[Dict]:
        """
        Process all LinkedIn profiles from the agents JSON file
        """
        if not self.check_credits():
            logging.error("Failed to verify credits. Stopping process.")
            return []

        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
        except Exception as e:
            logging.error(f"Error reading file {file_path}: {str(e)}")
            return []

        results = []
        for entry in data:
            for agent in entry.get('agents', []):
                linkedin_url = agent.get('linkedin')
                if not linkedin_url:
                    logging.warning(f"No LinkedIn URL found for agent: {agent.get('name', 'Unknown')}")
                    continue

                logging.info(f"Processing LinkedIn profile for: {agent.get('name', 'Unknown')}")
                result = self.process_linkedin_profile(linkedin_url)
                
                if result:
                    results.append({
                        'agent_name': agent.get('name'),
                        'linkedin_url': linkedin_url,
                        'wiza_response': result
                    })
                
                time.sleep(1)

        return results

def main():
    try:
        api_key = os.getenv('WIZA_API_KEY')
        if not api_key:
            raise ValueError("WIZA_API_KEY not found in .env file")
        
        scraper = LinkedInEmailScraper(api_key)
        
        # Process the agents file
        results = scraper.process_agents_file('1_agents_with_linkedin.json')
        
        # Save results to a JSON file
        with open('2_agents_with_email_and_phone.json', 'w') as f:
            json.dump(results, f, indent=2)
        logging.info("Results saved to 2_agents_with_email_and_phone.json")
        
    except ValueError as e:
        logging.error(f"Configuration error: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()