import os
import json
import asyncio
import subprocess
import shutil
from typing import Dict, Optional
from _1_zillow_linkedin_scraper import main as run_linkedin_scraper
from _2_linkedin_email_and_phone_scraper import LinkedInEmailScraper
from _3_upload_google_sheets import GoogleSheetsUploader

AGENTS_EXAMPLE = {
    "office_name": "Example Real Estate",
    "office_address": "123 Main St, City, State 12345",
    "agents": [
        {
            "name": "John Doe",
            "title": "Real Estate Agent",
            "zillow_profile": "https://www.zillow.com/profile/johndoe"
        }
    ]
}

ENRICHMENT_LEVELS = {
    1: ("none", "No additional data"),
    2: ("partial", "Find email only"),
    3: ("phone", "Find phone numbers only"),
    4: ("full", "Find both email and phone numbers")
}

AGENT_COUNT_OPTIONS = {
    1: 10,
    2: 50,
    3: 100,
    4: 200,
    5: 350
}

def check_file_exists(filename: str) -> bool:
    """Check if a file exists and return boolean"""
    return os.path.exists(filename)

def create_example_file(filename: str, content: Dict) -> None:
    """Create an example JSON file"""
    try:
        with open(filename, 'w') as f:
            json.dump(content, f, indent=2)
        print(f"\nCreated example file: {filename}")
        print("Please edit this file with your actual data and run the script again.")
    except Exception as e:
        print(f"Error creating example file: {str(e)}")

def get_user_choice(prompt: str, valid_choices: list) -> str:
    """Get user input with validation"""
    while True:
        choice = input(prompt).lower()
        if choice in valid_choices:
            return choice
        print(f"Invalid choice. Please choose from: {', '.join(valid_choices)}")

def get_zip_code() -> int:
    """Get valid ZIP code from user"""
    while True:
        try:
            zip_code = input("\nEnter the ZIP code to scrape: ")
            if len(zip_code) == 5 and zip_code.isdigit():
                return int(zip_code)
            print("Please enter a valid 5-digit ZIP code.")
        except ValueError:
            print("Please enter a valid ZIP code.")

def get_agent_count_choice() -> int:
    """Get user's choice for number of agents to scrape"""
    print("\nHow many agents would you like to scrape?")
    for key, value in AGENT_COUNT_OPTIONS.items():
        print(f"{key}. {value} agents")
    
    while True:
        try:
            choice = int(input("\nChoose option (1-5): "))
            if 1 <= choice <= 5:
                return choice
            print("Please enter a number between 1 and 5.")
        except ValueError:
            print("Please enter a valid number.")

def get_enrichment_choice() -> Optional[int]:
    """Get user's choice for enrichment level"""
    print("\nEnrichment Levels Available:")
    for key, (level, description) in ENRICHMENT_LEVELS.items():
        print(f"{key}. {description}")
    
    while True:
        try:
            choice = int(input("\nChoose enrichment level (1-4): "))
            if 1 <= choice <= 4:
                return choice
            print("Please enter a number between 1 and 4.")
        except ValueError:
            print("Please enter a valid number.")

async def run_zillow_scraper(zip_code: int, pages: int):
    """Run the Zillow scraper script"""
    try:
        subprocess.run(['python', '_0_zillow_agents_scraper.py', str(zip_code), str(pages)], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running Zillow scraper: {str(e)}")
        raise

async def main():
    print("\nWelcome to the Agent Data Enrichment Tool!")

    # Step 0: Get ZIP code and agent count
    zip_code = get_zip_code()
    agent_count_choice = get_agent_count_choice()
    
    # Calculate number of pages (agents divided by 15 rounded up)
    target_agents = AGENT_COUNT_OPTIONS[agent_count_choice]
    pages = -(-target_agents // 15)  # Ceiling division
    
    print(f"\nWill scrape approximately {target_agents} agents across {pages} pages...")
    
    # Run Zillow scraper
    try:
        await run_zillow_scraper(zip_code, pages)
    except Exception as e:
        print(f"Error during Zillow scraping: {str(e)}")
        return

    # Step 1: LinkedIn Profile Scraping
    if not check_file_exists('0_agents.json'):
        print("\nNo 0_agents.json file found! Scraping may have failed.")
        return

    should_scrape_linkedin = get_user_choice(
        "\nDo you want to scrape LinkedIn profiles from Zillow profiles? (y/n): ",
        ['y', 'n']
    )

    if should_scrape_linkedin == 'y':
        print("\nStarting LinkedIn profile scraping...")
        await run_linkedin_scraper()
        print("\nLinkedIn profile scraping completed.")

    # Step 2: Contact Information Enrichment
    linkedin_file = '1_agents_with_linkedin.json'
    if not check_file_exists(linkedin_file):
        print(f"\nNo {linkedin_file} file found!")
        print("Please provide the file and run the script again.")
        return

    enrichment_choice = get_enrichment_choice()
    if not enrichment_choice:
        print("\nNo enrichment level selected. Exiting...")
        return

    output_file = '2_agents_with_email_and_phone.json'

    # If user selected "none" (choice 1), copy the LinkedIn data file
    if enrichment_choice == 1:
        print("\nCopying LinkedIn data without enrichment...")
        try:
            shutil.copy2(linkedin_file, output_file)
            print(f"Created {output_file} as a copy of {linkedin_file}")
        except Exception as e:
            print(f"Error copying file: {str(e)}")
            return
    else:
        # Get the enrichment level string for the API
        enrichment_level = ENRICHMENT_LEVELS[enrichment_choice][0]
        print(f"\nStarting contact information enrichment with level: {enrichment_level}")
        
        try:
            scraper = LinkedInEmailScraper()
            results = scraper.process_agents_file(linkedin_file)
            
            # Save results
            with open(output_file, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"\nResults saved to {output_file}")
            
        except Exception as e:
            print(f"Error during enrichment: {str(e)}")
            return

    # Step 3: Upload to Google Sheets
    should_upload = get_user_choice(
        "\nDo you want to upload the results to Google Sheets? (y/n): ",
        ['y', 'n']
    )

    if should_upload == 'y':
        try:
            print("\nUploading data to Google Sheets...")
            uploader = GoogleSheetsUploader()
            spreadsheet_id = uploader.upload_data(output_file)
            print(f"Successfully uploaded data to Google Sheets (ID: {spreadsheet_id})")
        except Exception as e:
            print(f"Error during Google Sheets upload: {str(e)}")
            return

    print("\nProcess completed successfully!")

if __name__ == "__main__":
    asyncio.run(main())