"""
Excel reader for establishments data.
"""

import logging
import pandas as pd
from typing import List, Dict
from urllib.parse import urlparse

class ExcelReader:
    """Reads establishment data from Excel files."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def read_establishments(self, file_path: str) -> List[Dict]:
        """
        Read establishments from Excel file.
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            List of establishment dictionaries
        """
        try:
            df = pd.read_excel(file_path)
            self.logger.info(f"Read {len(df)} rows from Excel file: {file_path}")
            
            establishments = []
            for _, row in df.iterrows():
                # Extract required fields
                display_name = str(row.get('displayName', '')).strip()
                google_url = str(row.get('googleUrl', '')).strip()
                website = str(row.get('website', '')).strip()
                
                # Validate required fields
                if not display_name or not google_url or not website:
                    self.logger.warning(f"Skipping row with missing data: {display_name}")
                    continue
                
                # Clean website URL
                website = self._clean_website_url(website)
                
                establishments.append({
                    'display_name': display_name,
                    'google_url': google_url,
                    'website': website
                })
            
            self.logger.info(f"Successfully processed {len(establishments)} establishments")
            return establishments
            
        except Exception as e:
            self.logger.error(f"Error reading Excel file {file_path}: {e}")
            return []
    
    def _clean_website_url(self, website: str) -> str:
        """Clean and normalize website URL."""
        if not website:
            return ""
        
        # Remove any existing query parameters
        if '?' in website:
            website = website.split('?')[0]
        
        # Ensure it starts with http/https
        if not website.startswith(('http://', 'https://')):
            website = 'https://' + website
        
        # Remove trailing slash
        website = website.rstrip('/')
        
        return website