"""
Unification processor - combines Google and Trustpilot reviews into standardized format.
"""

from typing import Dict, List, Set
from datetime import datetime
from bson import ObjectId

from ..core.base_processor import BaseProcessor

class UnificationProcessor(BaseProcessor):
    """Processes raw reviews into unified format."""
    
    def __init__(self, config_path: str = "config/scraping_config.yaml"):
        super().__init__(config_path)
    
    def unify_reviews_incremental(self, establishment_ids: List[str] = None) -> Dict[str, int]:
        """
        Incrementally unify reviews from Google and Trustpilot collections.
        Only processes reviews that haven't been unified yet.
        
        Args:
            establishment_ids: Optional list of establishment IDs to process
            
        Returns:
            Dictionary with counts of unified reviews by platform
        """
        self.logger.info("Starting incremental review unification...")
        
        # Get existing unified review IDs to avoid duplicates
        existing_unified_ids = self._get_existing_unified_review_ids()
        self.logger.info(f"Found {len(existing_unified_ids)} existing unified reviews")
        
        # Build query filter
        query_filter = {}
        if establishment_ids:
            query_filter["establishment_id"] = {"$in": establishment_ids}
        
        unified_count = {"google": 0, "trustpilot": 0}
        batch_size = self.config.get('processing', {}).get('batch_size', 1000)
        
        # Process Google reviews
        self.logger.info("Processing Google reviews...")
        google_count = self._process_platform_reviews(
            "google_reviews", query_filter, existing_unified_ids, batch_size
        )
        unified_count["google"] = google_count
        
        # Process Trustpilot reviews
        self.logger.info("Processing Trustpilot reviews...")
        trustpilot_count = self._process_platform_reviews(
            "trustpilot_reviews", query_filter, existing_unified_ids, batch_size
        )
        unified_count["trustpilot"] = trustpilot_count
        
        total_unified = unified_count["google"] + unified_count["trustpilot"]
        self.logger.info(f"Unification complete! Unified {total_unified} new reviews: "
                        f"Google={unified_count['google']}, Trustpilot={unified_count['trustpilot']}")
        
        return unified_count
    
    def _get_existing_unified_review_ids(self) -> Set:
        """Get all existing unified review IDs to avoid duplicates."""
        try:
            existing_ids = self.db_manager.db.unified_reviews.distinct("_id")
            return set(existing_ids)
        except Exception:
            return set()
    
    def _process_platform_reviews(self, collection_name: str, query_filter: Dict, 
                                existing_ids: Set, batch_size: int) -> int:
        """Process reviews from a specific platform collection."""
        unified_count = 0
        reviews_to_insert = []
        
        try:
            reviews = self.db_manager.db[collection_name].find(query_filter)
            
            for review in reviews:
                # Skip if already unified (using MongoDB _id)
                if review["_id"] in existing_ids:
                    continue
                
                if "google" in collection_name:
                    unified_review = self._standardize_google_review(review)
                else:
                    unified_review = self._standardize_trustpilot_review(review)
                
                reviews_to_insert.append(unified_review)
                unified_count += 1
                
                # Batch insert to manage memory
                if len(reviews_to_insert) >= batch_size:
                    self._insert_unified_batch(reviews_to_insert)
                    reviews_to_insert.clear()
            
            # Insert remaining reviews
            if reviews_to_insert:
                self._insert_unified_batch(reviews_to_insert)
            
        except Exception as e:
            self.logger.error(f"Error processing {collection_name}: {e}")
        
        return unified_count
    
    def _insert_unified_batch(self, reviews: List[Dict]):
        """Insert batch of unified reviews."""
        try:
            self.db_manager.db.unified_reviews.insert_many(reviews, ordered=False)
            self.logger.info(f"Inserted batch of {len(reviews)} unified reviews")
        except Exception as e:
            self.logger.error(f"Error inserting batch: {str(e)[:200]}...")
    
    def _standardize_google_review(self, review: Dict) -> Dict:
        """Standardize Google review to unified format."""
        return {
            "_id": review["_id"],
            "original_review_id": review.get("review_id"),
            "establishment_id": review.get("establishment_id"),
            "platform": "google",
            
            # Author information
            "author_name": review.get("name"),
            "author_id": review.get("reviewerId"),
            "author_url": review.get("reviewerUrl"),
            "author_photo_url": review.get("reviewerPhotoUrl"),
            "author_review_count": review.get("reviewerNumberOfReviews"),
            "is_local_guide": review.get("isLocalGuide", False),
            
            # Review content
            "rating": review.get("rating") or review.get("stars"),
            "title": None,  # Google reviews don't have titles
            "review_text": review.get("text", ""),
            "review_text_translated": review.get("textTranslated"),
            "review_language": review.get("language"),
            "original_language": review.get("originalLanguage"),
            "translated_language": review.get("translatedLanguage"),
            
            # Dates
            "review_date": review.get("publishedAtDate", ""),
            "published_at": review.get("publishAt"),
            "published_at_date": review.get("publishedAtDate"),
            "visited_in": review.get("visitedIn"),
            
            # Engagement
            "helpful_votes": review.get("likesCount", 0),
            "verified_purchase": None,
            
            # Owner response
            "response_from_owner_date": review.get("responseFromOwnerDate"),
            "response_from_owner_text": review.get("responseFromOwnerText"),
            
            # Media and context
            "review_image_urls": review.get("reviewImageUrls", []),
            "review_context": review.get("reviewContext", {}),
            "review_detailed_rating": review.get("reviewDetailedRating", {}),
            
            # Location data
            "country_code": review.get("countryCode"),
            "location": review.get("location", {}),
            "address": review.get("address"),
            "city": review.get("city"),
            "state": review.get("state"),
            "postal_code": review.get("postalCode"),
            
            # Business information
            "place_id": review.get("placeId"),
            "business_title": review.get("title"),
            "categories": review.get("categories", []),
            "category_name": review.get("categoryName"),
            
            # Metadata
            "source_url": review.get("source_url", ""),
            "scraped_at": review.get("scraped_at"),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
    
    def _standardize_trustpilot_review(self, review: Dict) -> Dict:
        """Standardize Trustpilot review to unified format."""
        return {
            "_id": review["_id"],
            "original_review_id": review.get("review_id"),
            "establishment_id": review.get("establishment_id"),
            "platform": "trustpilot",
            
            # Author information (limited for privacy)
            "author_name": None,  # Removed for privacy
            "author_id": None,
            "author_url": None,
            "author_photo_url": None,
            "author_review_count": review.get("numberOfReviews"),
            "is_local_guide": None,
            
            # Review content
            "rating": review.get("ratingValue", 0),
            "title": review.get("reviewHeadline", ""),
            "review_text": review.get("reviewBody", ""),
            "review_text_translated": None,
            "review_language": review.get("reviewLanguage"),
            "original_language": None,
            "translated_language": None,
            
            # Dates
            "review_date": review.get("datePublished", ""),
            "published_at": None,
            "published_at_date": review.get("datePublished"),
            "visited_in": None,
            "experience_date": review.get("experienceDate"),
            
            # Engagement
            "helpful_votes": review.get("likes", 0),
            "verified_purchase": review.get("verified", False),
            "verification_level": review.get("verificationLevel"),
            
            # Owner response
            "response_from_owner_date": None,
            "response_from_owner_text": None,
            
            # Media and context
            "review_image_urls": [],
            "review_context": {},
            "review_detailed_rating": {},
            
            # Location data
            "country_code": review.get("consumerCountryCode"),
            "location": {},
            "address": None,
            "city": None,
            "state": None,
            "postal_code": None,
            
            # Business information
            "place_id": None,
            "business_title": None,
            "categories": [],
            "category_name": None,
            
            # Metadata
            "source_url": review.get("source_url", ""),
            "review_url": review.get("reviewUrl"),
            "scraped_at": review.get("scraped_at"),
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
    
    def get_unified_reviews_stats(self) -> Dict:
        """Get statistics about unified reviews."""
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": "$platform",
                        "count": {"$sum": 1},
                        "avg_rating": {"$avg": "$rating"}
                    }
                }
            ]
            
            platform_stats = list(self.db_manager.db.unified_reviews.aggregate(pipeline))
            total_reviews = self.db_manager.db.unified_reviews.count_documents({})
            
            return {
                "total_reviews": total_reviews,
                "platform_breakdown": platform_stats,
                "last_updated": datetime.utcnow()
            }
        except Exception as e:
            self.logger.error(f"Error getting unified reviews stats: {e}")
            return {}