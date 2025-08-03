"""
Apify client for scraping reviews from Google Maps and Trustpilot.
"""

import logging
from typing import Dict, List
from urllib.parse import urlparse
from apify_client import ApifyClient as ApifyClientSDK

class ApifyClient:
    """Handles scraping operations using Apify actors."""
    
    def __init__(self, api_token: str, config: Dict = None):
        self.client = ApifyClientSDK(api_token)
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        
        # Get actor IDs from config or use defaults
        google_config = self.config.get('google_maps', {})
        trustpilot_config = self.config.get('trustpilot', {})
        
        self.GOOGLE_ACTOR_ID = google_config.get('actor_id', 'Xb8osYTtOjlsgI6k9')
        self.TRUSTPILOT_ACTOR_ID = trustpilot_config.get('actor_id', 'fLXimoyuhE1UQgDbM')
    
    def scrape_google_reviews(self, google_url: str) -> List[Dict]:
        """
        Scrape Google Maps reviews.
        
        Args:
            google_url: Google Maps URL for the establishment
            
        Returns:
            List of processed review dictionaries
        """
        self.logger.info(f"Starting Google scrape for: {google_url}")
        
        # Get settings from config
        settings = self.config.get('google_maps', {}).get('settings', {})
        
        run_input = {
            "startUrls": [{"url": google_url}],
            "language": settings.get('language', 'en'),
            "maxReviews": settings.get('max_reviews', 99999),
            "personalData": settings.get('personal_data', False),
            "reviewsSort": settings.get('reviews_sort', 'newest'),
            "reviewsOrigin": settings.get('reviews_origin', 'all')
        }
        
        try:
            # Call the actor
            run = self.client.actor(self.GOOGLE_ACTOR_ID).call(run_input=run_input)
            
            # Get results
            results = []
            for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                results.append(item)
            
            self.logger.info(f"Retrieved {len(results)} Google reviews")
            return self._process_google_reviews(results, google_url)
            
        except Exception as e:
            self.logger.error(f"Error scraping Google reviews: {e}")
            return []
    
    def scrape_trustpilot_reviews(self, website: str) -> List[Dict]:
        """
        Scrape Trustpilot reviews.
        
        Args:
            website: Website URL to find on Trustpilot
            
        Returns:
            List of processed review dictionaries
        """
        # Extract domain from website URL
        domain = urlparse(website).netloc
        if domain.startswith('www.'):
            domain = domain[4:]
        
        trustpilot_domain = f"{domain}?languages=all"
        self.logger.info(f"Starting Trustpilot scrape for: {trustpilot_domain}")
        
        # Get settings from config
        settings = self.config.get('trustpilot', {}).get('settings', {})
        
        run_input = {
            "companyDomain": trustpilot_domain,
            "count": settings.get('count', 99999),
            "replies": settings.get('replies', False),
            "startPage": settings.get('start_page', 1),
            "verified": settings.get('verified', False)
        }
        
        try:
            # Call the actor
            run = self.client.actor(self.TRUSTPILOT_ACTOR_ID).call(run_input=run_input)
            
            # Get results
            results = []
            for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                results.append(item)
            
            self.logger.info(f"Retrieved {len(results)} Trustpilot reviews")
            return self._process_trustpilot_reviews(results, website)
            
        except Exception as e:
            self.logger.error(f"Error scraping Trustpilot reviews: {e}")
            return []
    
    def _process_google_reviews(self, raw_reviews: List[Dict], source_url: str) -> List[Dict]:
        """Process Google reviews - add metadata and clean data."""
        processed = []
        
        for review in raw_reviews:
            try:
                # Start with the raw review data
                processed_review = review.copy()
                
                # Add standardized metadata fields
                processed_review.update({
                    "platform": "google",
                    "review_id": review.get("reviewId"),
                    "rating": review.get("stars"),
                    "source_url": source_url,
                })
                
                processed.append(processed_review)
                
            except Exception as e:
                self.logger.warning(f"Error processing Google review: {e}")
                continue
        
        return processed
    
    def _process_trustpilot_reviews(self, raw_reviews: List[Dict], source_url: str) -> List[Dict]:
        """Process Trustpilot reviews - add metadata and clean data."""
        processed = []
        
        for review in raw_reviews:
            try:
                # Start with the raw review data
                processed_review = review.copy()
                
                # Remove author name for privacy
                processed_review.pop('authorName', None)
                
                # Add standardized metadata fields
                processed_review.update({
                    "platform": "trustpilot",
                    "review_id": review.get("reviewUrl", ""),
                    "rating": review.get("ratingValue", 0),
                    "verified": review.get("verificationLevel") == "verified",
                    "source_url": source_url,
                })
                
                processed.append(processed_review)
                
            except Exception as e:
                self.logger.warning(f"Error processing Trustpilot review: {e}")
                continue
        
        return processed