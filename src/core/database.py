"""
Core database management module for clinic reviews system.
Handles MongoDB connections and basic operations.
"""

import logging
from pathlib import Path
from typing import Optional, Dict, List
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson import ObjectId

class DatabaseManager:
    """Manages MongoDB connections and basic database operations."""
    
    def __init__(self, config_dict: Dict = None):
        self.client = None
        self.db = None
        self.config = config_dict or {}
        self.logger = logging.getLogger(__name__)
        
    def connect(self, connection_string: str, database_name: str = "review_scraper") -> bool:
        """
        Connect to MongoDB database.
        
        Args:
            connection_string: MongoDB connection string
            database_name: Name of the database to use
            
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.client = MongoClient(connection_string)
            self.db = self.client[database_name]
            
            # Test the connection
            self.client.admin.command('ping')
            self.logger.info(f"Successfully connected to MongoDB database: {database_name}")
            return True
            
        except ConnectionFailure as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to MongoDB: {e}")
            return False
    
    def close_connection(self):
        """Close database connection."""
        if self.client:
            self.client.close()
            self.logger.info("Database connection closed")
    
    def create_indexes(self):
        """Create all necessary indexes for optimal performance."""
        try:
            # Establishments collection
            self.db.establishments.create_index("google_url", unique=True)
            self.db.establishments.create_index("website")
            
            # Google reviews collection
            self.db.google_reviews.create_index("establishment_id")
            self.db.google_reviews.create_index("scraped_at")
            self.db.google_reviews.create_index([("establishment_id", 1), ("review_id", 1)])
            
            # Trustpilot reviews collection
            self.db.trustpilot_reviews.create_index("establishment_id")
            self.db.trustpilot_reviews.create_index("scraped_at")
            self.db.trustpilot_reviews.create_index([("establishment_id", 1), ("review_id", 1)])
            
            # Unified reviews collection
            self.db.unified_reviews.create_index("establishment_id")
            self.db.unified_reviews.create_index("platform")
            self.db.unified_reviews.create_index("review_date")
            self.db.unified_reviews.create_index([("establishment_id", 1), ("platform", 1)])
            
            # Enriched reviews collection
            self.db.enriched_reviews.create_index("establishment_id")
            self.db.enriched_reviews.create_index("platform")
            self.db.enriched_reviews.create_index("processed_at")
            self.db.enriched_reviews.create_index([("establishment_id", 1), ("platform", 1)])
            
            self.logger.info("Database indexes created successfully")
            
        except Exception as e:
            self.logger.error(f"Error creating indexes: {e}")
    
    def get_establishment_by_url(self, google_url: str) -> Optional[Dict]:
        """Get establishment by Google URL."""
        try:
            return self.db.establishments.find_one({"google_url": google_url})
        except Exception as e:
            self.logger.error(f"Error fetching establishment by URL: {e}")
            return None
    
    def create_establishment(self, display_name: str, google_url: str, website: str) -> Optional[str]:
        """Create new establishment record."""
        try:
            establishment = {
                "display_name": display_name,
                "google_url": google_url,
                "website": website,
                "trustpilot_url": f"{website}?languages=all",
                "google_last_scraped": None,
                "trustpilot_last_scraped": None,
                "google_total_reviews": 0,
                "trustpilot_total_reviews": 0,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
            
            result = self.db.establishments.insert_one(establishment)
            self.logger.info(f"Created establishment: {display_name}")
            return str(result.inserted_id)
            
        except Exception as e:
            self.logger.error(f"Error creating establishment: {e}")
            return None
    
    def update_establishment_scrape_info(self, establishment_id: str, platform: str, total_reviews: int):
        """Update establishment scrape statistics."""
        try:
            update_data = {
                f"{platform}_last_scraped": datetime.utcnow(),
                f"{platform}_total_reviews": total_reviews,
                "updated_at": datetime.utcnow()
            }
            
            self.db.establishments.update_one(
                {"_id": ObjectId(establishment_id)},
                {"$set": update_data}
            )
            
        except Exception as e:
            self.logger.error(f"Error updating establishment scrape info: {e}")
    
    def save_reviews(self, collection_name: str, establishment_id: str, reviews: List[Dict]) -> int:
        """Save reviews to specified collection."""
        if not reviews:
            return 0
            
        try:
            # Add metadata to each review
            for review in reviews:
                review["establishment_id"] = establishment_id
                review["scraped_at"] = datetime.utcnow()
            
            result = self.db[collection_name].insert_many(reviews)
            count = len(result.inserted_ids)
            
            self.logger.info(f"Saved {count} reviews to {collection_name} for establishment {establishment_id}")
            return count
            
        except Exception as e:
            self.logger.error(f"Error saving reviews to {collection_name}: {e}")
            return 0
    
    def get_collections_stats(self) -> Dict:
        """Get basic statistics about all collections."""
        try:
            stats = {}
            collections = [
                'establishments', 'google_reviews', 'trustpilot_reviews', 
                'unified_reviews', 'enriched_reviews'
            ]
            
            for collection in collections:
                try:
                    count = self.db[collection].count_documents({})
                    stats[collection] = count
                except Exception:
                    stats[collection] = 0
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting collection stats: {e}")
            return {}
    
    def get_establishments_list(self, limit: Optional[int] = None) -> List[Dict]:
        """Get list of all establishments."""
        try:
            query = self.db.establishments.find({}, {
                "_id": 1, "display_name": 1, "google_url": 1, "website": 1
            })
            
            if limit:
                query = query.limit(limit)
                
            return list(query)
        except Exception as e:
            self.logger.error(f"Error fetching establishments list: {e}")
            return []