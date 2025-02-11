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
if not API_KEY:
    raise ValueError("FIRECRAWL_API_KEY not found in environment variables")

# Initialize Firecrawl
app = FirecrawlApp(api_key=API_KEY)

# Define the extraction schema based on provided JSON schema
class AgentSchema(BaseModel):
    name: str
    zillow_profile: str

class ExtractSchema(BaseModel):
    agents: List[AgentSchema]

def generate_urls(zip_code: int, pages: int) -> List[str]:
    """
    Generate list of Zillow URLs based on zip code and number of pages.
    """
    base_url = f"https://www.zillow.com/professionals/real-estate-agent-reviews/{zip_code}/"
    return [f"{base_url}?page={page}" for page in range(1, pages + 1)]

async def extract_agents_data(url: str) -> Dict:
    """
    Extract agents data from a single URL using Firecrawl API.
    """
    try:
        print(f"\nProcessing URL: {url}")
        
        # Extract data using firecrawl-py
        response = app.extract(
            [url],
            {
                'prompt': 'Extract the name, and Zillow profile URL for each real estate agent. zillow_profile looks like "https://www.zillow.com/profile/userid"',
                'schema': ExtractSchema.model_json_schema(),
            }
        )
        
        print(f"API Response: {json.dumps(response, indent=2)}")
        
        # Process and validate response
        if response and isinstance(response, dict):
            if response.get('success') and 'data' in response:
                return response['data']
        
        return {'agents': []}
    
    except Exception as e:
        print(f"Error extracting data from {url}: {str(e)}")
        return {'agents': []}

async def process_zip_code(zip_code: int, pages: int) -> List[Dict]:
    """
    Process all pages for a given zip code and return combined results.
    """
    # Generate URLs for all pages
    urls = generate_urls(zip_code, pages)
    print(f"\nGenerated {len(urls)} URLs for zip code {zip_code}")
    
    # Process each URL and combine results
    all_agents = []
    for url in urls:
        result = await extract_agents_data(url)
        if result and 'agents' in result:
            all_agents.extend(result['agents'])
        
        # Add delay between requests
        if url != urls[-1]:  # Don't delay after the last request
            await asyncio.sleep(1)
    
    return [{'agents': all_agents}]

async def main(zip_code: int, pages: int):
    print(f"\nStarting agent data extraction for zip code {zip_code}...")
    
    try:
        # Process the zip code
        results = await process_zip_code(zip_code, pages)
        
        # Save the results
        output_file = '0_agents.json'
        print(f"\nSaving results to {output_file}...")
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Print summary
        total_agents = sum(len(item.get('agents', [])) for item in results)
        print(f"\nFinal Summary:")
        print(f"Total agents extracted: {total_agents}")
        print(f"Results saved to {output_file}")
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract real estate agent data from Zillow')
    parser.add_argument('zip_code', type=int, help='ZIP code to search')
    parser.add_argument('pages', type=int, help='Number of pages to process')
    
    args = parser.parse_args()
    asyncio.run(main(args.zip_code, args.pages))