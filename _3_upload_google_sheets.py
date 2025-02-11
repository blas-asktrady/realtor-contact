import os
import json
import logging
import pickle
from typing import List, Dict
from datetime import datetime
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

class GoogleSheetsUploader:
    def __init__(self):
        self.creds = self._get_credentials()
        self.sheets_service = build('sheets', 'v4', credentials=self.creds)
        self.drive_service = build('drive', 'v3', credentials=self.creds)
        self.folder_path = ['0_idea_validation', 'homereels', 'agents_contact_info']

    def _get_credentials(self):
        """
        Get or create OAuth2 credentials
        """
        creds = None
        
        # Check if token file exists and delete it to force new authentication
        if os.path.exists('token.pickle'):
            os.remove('token.pickle')
            logging.info("Removed existing token.pickle to force new authentication")
        
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
                
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists('credentials.json'):
                    raise ValueError("credentials.json file not found")
                    
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
                
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
                
        return creds

    def _find_folder(self, folder_name: str, parent_id: str = None) -> str:
        """
        Find a folder by name and optional parent ID
        """
        try:
            query_parts = [
                f"name = '{folder_name}'",
                "mimeType = 'application/vnd.google-apps.folder'",
                "trashed = false"
            ]
            
            if parent_id and parent_id != 'root':
                query_parts.append(f"'{parent_id}' in parents")
            elif parent_id == 'root':
                query_parts.append("'root' in parents")
            
            query = " and ".join(query_parts)
            logging.info(f"Searching for folder with query: {query}")
            
            results = self.drive_service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)',
                pageSize=1
            ).execute()
            
            files = results.get('files', [])
            
            if files:
                folder_id = files[0]['id']
                logging.info(f"Found folder: {folder_name} (ID: {folder_id})")
                return folder_id
            
            logging.warning(f"Folder not found: {folder_name}")
            return None
            
        except Exception as e:
            logging.error(f"Error finding folder {folder_name}: {str(e)}")
            raise

    def _get_destination_folder_id(self) -> str:
        """
        Navigate through the folder path to get the final destination folder ID
        """
        try:
            current_parent = 'root'
            
            # Navigate through the folder path
            for folder_name in self.folder_path:
                folder_id = self._find_folder(folder_name, current_parent)
                
                if not folder_id:
                    raise ValueError(f"Could not find folder: {folder_name}")
                
                current_parent = folder_id
                logging.info(f"Successfully navigated to folder: {folder_name}")
            
            return current_parent
            
        except Exception as e:
            logging.error(f"Error finding destination folder: {str(e)}")
            raise

    def _create_new_spreadsheet(self) -> str:
        """
        Create a new spreadsheet with timestamp in the name and move it to the correct folder
        """
        timestamp = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
        spreadsheet_name = f"Agents_Data_{timestamp}"
        
        try:
            # Get the destination folder ID first
            folder_id = self._get_destination_folder_id()
            logging.info(f"Found destination folder ID: {folder_id}")
            
            # Create new spreadsheet metadata with the parent folder specified
            spreadsheet_metadata = {
                'name': spreadsheet_name,
                'mimeType': 'application/vnd.google-apps.spreadsheet',
                'parents': [folder_id]
            }
            
            # Create the file in Drive first
            file = self.drive_service.files().create(
                body=spreadsheet_metadata,
                fields='id'
            ).execute()
            
            spreadsheet_id = file.get('id')
            logging.info(f"Created new spreadsheet with ID: {spreadsheet_id}")
            
            return spreadsheet_id
            
        except Exception as e:
            logging.error(f"Error creating new spreadsheet: {str(e)}")
            raise

    def _flatten_agent_data(self, agent_data: Dict) -> List:
        """
        Extract agent name and LinkedIn URL from the agent data structure
        """
        return [
            agent_data.get('name', ''),
            agent_data.get('linkedin', ''),
            agent_data.get('zillow_profile', '')  # Added Zillow profile
        ]

    def _prepare_headers(self) -> List[str]:
        """
        Define the headers for the spreadsheet
        """
        return [
            'Agent Name',
            'LinkedIn URL',
            'Zillow Profile'  # Added Zillow profile header
        ]

    def upload_data(self, json_file_path: str):
        """
        Upload data from JSON file to a new Google Sheet in the specified folder
        """
        try:
            # Create new spreadsheet in the correct folder
            spreadsheet_id = self._create_new_spreadsheet()
            
            # Read the JSON file
            with open(json_file_path, 'r') as file:
                raw_data = json.load(file)
                
            # Extract the agents array from the nested structure
            data = raw_data[0].get('agents', []) if raw_data else []
            
            # Prepare the data
            headers = self._prepare_headers()
            rows = [self._flatten_agent_data(agent) for agent in data]
            
            # Insert headers and data
            values = [headers] + rows
            body = {
                'values': values
            }
            
            # Upload data
            range_name = 'Sheet1!A1:C'  # Updated to include column C for Zillow profile
            result = self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            
            logging.info(f"Successfully uploaded {len(rows)} rows of data")
            logging.info(f"Updated {result.get('updatedCells')} cells")
            
            # Auto-resize columns
            requests = [{
                'autoResizeDimensions': {
                    'dimensions': {
                        'sheetId': 0,  # Assuming Sheet1 has ID 0
                        'dimension': 'COLUMNS',
                        'startIndex': 0,
                        'endIndex': 3  # Updated to include column C
                    }
                }
            }]
            
            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': requests}
            ).execute()
            
            return spreadsheet_id
            
        except Exception as e:
            logging.error(f"Error uploading data to Google Sheets: {str(e)}")
            raise

def main():
    try:
        # Initialize uploader
        uploader = GoogleSheetsUploader()
        
        # Upload data to new spreadsheet
        spreadsheet_id = uploader.upload_data('1_agents_with_linkedin.json')
        
        logging.info(f"Data upload completed successfully to spreadsheet ID: {spreadsheet_id}")
        
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")

if __name__ == "__main__":
    main()