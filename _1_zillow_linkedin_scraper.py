import json
import asyncio
import os
from typing import List, Dict
from pydantic import BaseModel
from firecrawl import FirecrawlApp
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get environment variables
API_KEY = os.getenv('FIRECRAWL_API_KEY')
DELAY = int(os.getenv('FIRECRAWL_DELAY', 1))

# Initialize Firecrawl
app = FirecrawlApp(api_key=API_KEY)

# Define the extraction schema
class ExtractSchema(BaseModel):
    linkedin_profile: str

async def extract_linkedin_url(zillow_url: str) -> str:
    """
    Extract LinkedIn URL from a Zillow profile using Firecrawl API.
    Returns empty string if no LinkedIn URL is found.
    """
    try:
        print(f"\nProcessing URL: {zillow_url}")
        
        # Extract data using firecrawl-py
        response = app.extract(
            [zillow_url],
            {
                'prompt': '',
                'schema': ExtractSchema.model_json_schema(),
            }
        )
        
        print(f"API Response: {json.dumps(response, indent=2)}")
        
        # Extract LinkedIn URL from response
        linkedin_url = ""
        if response and isinstance(response, dict):
            if response.get('success') and 'data' in response:
                linkedin_url = response['data'].get('linkedin_profile', '')
        
        print(f"Extracted LinkedIn URL: {linkedin_url}")
        return linkedin_url
    
    except Exception as e:
        print(f"Error extracting LinkedIn URL: {str(e)}")
        return ''

async def process_agents(data: List[Dict]) -> List[Dict]:
    """
    Process list of agents and return only those with found LinkedIn URLs.
    """
    if not API_KEY:
        raise ValueError("FIRECRAWL_API_KEY not found in environment variables")
    
    # Create new structure for results
    results = []
    current_office = None
    
    total_agents = sum(len(item.get('agents', [])) for item in data)
    if total_agents == 0:
        raise ValueError("No agents found in the data")
    
    processed = 0
    success_count = 0
    
    for item in data:
        agents_with_linkedin = []
        current_office = item.copy()  # Copy office data
        
        for agent in item.get('agents', []):
            processed += 1
            if 'zillow_profile' in agent:
                print(f"\nProcessing agent {processed}/{total_agents}: {agent['name']}")
                linkedin_url = await extract_linkedin_url(agent['zillow_profile'])
                
                if linkedin_url:  # Only add agents with LinkedIn URLs
                    agent_data = agent.copy()
                    agent_data['linkedin'] = linkedin_url
                    agents_with_linkedin.append(agent_data)
                    success_count += 1
                    
                print(f"Result for {agent['name']}: LinkedIn URL = {linkedin_url}")
                print(f"Current success rate: {(success_count/processed)*100:.1f}%")
                
                if processed < total_agents:
                    print(f"Waiting {DELAY} seconds before next request...")
                    await asyncio.sleep(DELAY)
        
        # Only add office to results if it has agents with LinkedIn URLs
        if agents_with_linkedin:
            current_office['agents'] = agents_with_linkedin
            results.append(current_office)
    
    return results

async def main():
    print("\nStarting LinkedIn URL extraction process...")
    
    try:
        print("\nLoading 0_agents.json...")
        
        # Check if file exists
        if not os.path.exists('0_agents.json'):
            print("Error: 0_agents.json file not found")
            return
            
        # Load and validate the file
        with open('0_agents.json', 'r') as f:
            file_content = f.read()
            print(f"\nFile content preview: {file_content[:500]}...")
            
            try:
                data = json.loads(file_content)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {str(e)}")
                return
            
        print(f"\nLoaded {sum(len(item.get('agents', [])) for item in data)} agents")
        
        # Process the data
        updated_data = await process_agents(data)

        # Save the updated data
        print("\nSaving results to 1_agents_with_linkedin.json...")
        with open('1_agents_with_linkedin.json', 'w') as f:
            json.dump(updated_data, f, indent=2)
        print("Successfully saved results to 1_agents_with_linkedin.json")
        
        # Print summary
        total_agents = sum(len(item.get('agents', [])) for item in updated_data)
        
        print(f"\nFinal Summary:")
        print(f"Total agents with LinkedIn profiles: {total_agents}")
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())