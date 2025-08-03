"""
Enrichment processor - analyzes reviews using AI to extract sentiment attributes.
"""

import json
import hashlib
from typing import Dict, List, Set, Optional
from datetime import datetime
from dateutil.parser import parse as parse_date
from bson import ObjectId
from pymongo import UpdateOne
import google.generativeai as genai

from ..core.base_processor import BaseProcessor

class EnrichmentProcessor(BaseProcessor):
    """Processes reviews to extract sentiment and other attributes using AI."""
    
    def __init__(self, config_path: str = "config/enrichment_config.yaml"):
        super().__init__(config_path)
        self.genai_model = None
        self.translation_cache = {}
        self.token_count = 0
        
    def initialize(self) -> bool:
        """Initialize processor with database and AI model."""
        if not super().initialize():
            return False
        
        return self._setup_genai()
    
    def _setup_genai(self) -> bool:
        """Setup Google Generative AI."""
        try:
            tokens = self._load_tokens()
            if not tokens.get('google_api_key'):
                self.logger.error("Google API key not found")
                return False
            
            genai.configure(api_key=tokens['google_api_key'])
            
            model_name = self.config.get('ai_model', {}).get('name', 'gemini-2.5-flash')
            self.genai_model = genai.GenerativeModel(model_name)
            
            self.logger.info(f"Google Generative AI configured with model: {model_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error setting up Generative AI: {e}")
            return False
    
    def process_reviews(self, establishment_ids: List[str] = None, 
                       published_after: str = None, 
                       incremental: bool = True,
                       attribute_groups: List[str] = None) -> bool:
        """
        Main processing method for enriching reviews.
        
        Args:
            establishment_ids: List of establishment IDs to process
            published_after: ISO date string to filter reviews
            incremental: Only process unprocessed reviews
            attribute_groups: List of attribute groups ('sentiment', 'complaint', 'response', 'all')
        """
        try:
            # Get reviews to process
            reviews = self._get_reviews_to_process(establishment_ids, published_after, incremental)
            
            if not reviews:
                self.logger.info("No reviews to process")
                return True
            
            # Determine which attribute groups to process
            if not attribute_groups:
                attribute_groups = ['all']
            
            if 'all' in attribute_groups:
                attribute_groups = ['sentiment', 'complaint', 'response']
            
            enrichment_data = {}
            
            # Process sentiment attributes
            if 'sentiment' in attribute_groups:
                sentiment_data = self._process_sentiment_attributes(reviews)
                self._merge_enrichment_data(enrichment_data, sentiment_data)
            
            # Process complaint attribute
            if 'complaint' in attribute_groups:
                complaint_data = self._process_complaint_attribute(reviews)
                self._merge_enrichment_data(enrichment_data, complaint_data)
            
            # Upsert current data before processing response attributes
            self._upsert_enriched_reviews(enrichment_data, reviews)
            
            # Process response attributes (requires complaint data to be available)
            if 'response' in attribute_groups:
                response_data = self._process_response_attributes(reviews)
                self._merge_enrichment_data(enrichment_data, response_data)
            
            # Final upsert
            self._upsert_enriched_reviews(enrichment_data, reviews)
            
            self.logger.info("Review enrichment processing completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during review processing: {e}")
            return False
    
    def _get_reviews_to_process(self, establishment_ids: List[str] = None, 
                               published_after: str = None, 
                               incremental: bool = True) -> List[Dict]:
        """Get reviews that need processing based on parameters."""
        query = {}
        
        # Filter by establishment IDs
        config_establishments = self.config.get('target_establishments', [])
        target_establishments = establishment_ids or config_establishments
        
        if target_establishments:
            query["establishment_id"] = {"$in": target_establishments}
            self.logger.info(f"Filtering by {len(target_establishments)} target establishments")
        
        # Filter by date
        if published_after:
            try:
                parsed_date = parse_date(published_after)
                query["published_at_date"] = {"$gte": parsed_date.isoformat()}
            except Exception as e:
                self.logger.error(f"Error parsing date '{published_after}': {e}")
        
        # Get reviews from unified_reviews (not ls_unified_reviews anymore)
        reviews = list(self.db_manager.db.unified_reviews.find(query))
        
        if incremental:
            # Filter out already processed reviews
            processed_ids = set(self.db_manager.db.enriched_reviews.distinct("_id"))
            reviews = [r for r in reviews if r["_id"] not in processed_ids]
        
        # Filter by minimum length
        min_length = self.config.get('processing', {}).get('min_review_length', 10)
        reviews = [r for r in reviews if self._get_review_content_length(r) >= min_length]
        
        self.logger.info(f"Found {len(reviews)} reviews to process")
        return reviews
    
    def _get_review_content_length(self, review: Dict) -> int:
        """Calculate character count of review content."""
        content = ""
        if review.get('title'):
            content += review['title']
        if review.get('review_text'):
            content += " " + review['review_text']
        return len(content.strip())
    
    def _process_sentiment_attributes(self, reviews: List[Dict]) -> Dict:
        """Process sentiment attributes (0-3 scale)."""
        enabled_attrs = {
            name: config for name, config in self.config.get('sentiment_attributes', {}).items()
            if config.get('enabled', True)
        }
        
        if not enabled_attrs:
            return {}
        
        enrichment_data = {}
        batch_size = self.config.get('processing', {}).get('batch_size', 25)
        sentiment_batch_size = self.config.get('processing', {}).get('sentiment_batch_size', 3)
        
        # Process attributes in smaller batches to avoid token limits
        attr_names = list(enabled_attrs.keys())
        
        for attr_start in range(0, len(attr_names), sentiment_batch_size):
            attr_batch = {name: enabled_attrs[name] 
                         for name in attr_names[attr_start:attr_start + sentiment_batch_size]}
            
            self.logger.info(f"Processing sentiment attributes: {list(attr_batch.keys())}")
            
            # Process reviews in batches
            for review_start in range(0, len(reviews), batch_size):
                review_batch = reviews[review_start:review_start + batch_size]
                
                prompt = self._build_sentiment_prompt(review_batch, attr_batch)
                response_data = self._call_gemini_batch(prompt)
                
                if response_data:
                    validated_data = self._validate_sentiment_response(response_data, set(attr_batch.keys()))
                    self._merge_enrichment_data(enrichment_data, validated_data)
        
        return enrichment_data
    
    def _process_complaint_attribute(self, reviews: List[Dict]) -> Dict:
        """Process complaint classification."""
        if not self.config.get('complaint_attribute', {}).get('is_complaint', {}).get('enabled', True):
            return {}
        
        enrichment_data = {}
        batch_size = self.config.get('processing', {}).get('batch_size', 25)
        
        # Process reviews in batches
        for i in range(0, len(reviews), batch_size):
            batch = reviews[i:i + batch_size]
            
            prompt = self._build_complaint_prompt(batch)
            response_data = self._call_gemini_batch(prompt)
            
            if response_data:
                validated_data = self._validate_binary_response(response_data, {'is_complaint'})
                self._merge_enrichment_data(enrichment_data, validated_data)
        
        return enrichment_data
    
    def _process_response_attributes(self, reviews: List[Dict]) -> Dict:
        """Process response-specific attributes."""
        enabled_attrs = {
            name: config for name, config in self.config.get('response_attributes', {}).items()
            if config.get('enabled', True)
        }
        
        if not enabled_attrs:
            return {}
        
        # Filter reviews that have responses and complaints
        eligible_reviews = []
        for review in reviews:
            has_response = review.get('response_from_owner_text')
            
            # Check if is_complaint is already processed
            existing_enrichment = self.db_manager.db.enriched_reviews.find_one({"_id": review["_id"]})
            is_complaint = existing_enrichment.get('is_complaint', 0) if existing_enrichment else 0
            
            if has_response and is_complaint == 1:
                eligible_reviews.append(review)
        
        if not eligible_reviews:
            self.logger.info("No reviews eligible for response attribute analysis")
            return {}
        
        enrichment_data = {}
        batch_size = self.config.get('processing', {}).get('batch_size', 25)
        
        # Process reviews in batches
        for i in range(0, len(eligible_reviews), batch_size):
            batch = eligible_reviews[i:i + batch_size]
            
            prompt = self._build_response_prompt(batch, enabled_attrs)
            response_data = self._call_gemini_batch(prompt)
            
            if response_data:
                validated_data = self._validate_binary_response(response_data, set(enabled_attrs.keys()))
                self._merge_enrichment_data(enrichment_data, validated_data)
        
        return enrichment_data
    
    def _build_sentiment_prompt(self, reviews: List[Dict], attributes: Dict) -> str:
        """Build prompt for sentiment analysis."""
        attrs_list = []
        for attr_name, attr_config in attributes.items():
            attrs_list.append(f"{attr_name}: {attr_config['description']}")
        
        prompt = f"""Analyze sentiment for: {', '.join(attrs_list)}

Scale: 0=not mentioned, 1=negative, 2=neutral/mixed, 3=positive

Return JSON: {{"review_id": {{{', '.join(f'"{attr}": 0' for attr in attributes.keys())}}}}}

Reviews:
"""
        
        for review in reviews:
            review_id = str(review['_id'])
            title = review.get('title', '') or ''
            text = review.get('review_text', '') or ''
            content = f"{title} {text}".strip()
            prompt += f"{review_id}: {content}\n"
        
        return prompt
    
    def _build_complaint_prompt(self, reviews: List[Dict]) -> str:
        """Build prompt for complaint classification."""
        prompt = """Classify reviews as complaint (1) or not (0).

Return JSON: {"review_id": 0}

Reviews:
"""
        
        for review in reviews:
            review_id = str(review['_id'])
            title = review.get('title', '') or ''
            text = review.get('review_text', '') or ''
            content = f"{title} {text}".strip()
            prompt += f"{review_id}: {content}\n"
        
        return prompt
    
    def _build_response_prompt(self, reviews: List[Dict], attributes: Dict) -> str:
        """Build prompt for response analysis."""
        attrs_list = []
        for attr_name, attr_config in attributes.items():
            attrs_list.append(f"{attr_name}: {attr_config['description']}")
        
        prompt = f"""Analyze owner responses for: {', '.join(attrs_list)}

Return JSON: {{"review_id": {{{', '.join(f'"{attr}": 0' for attr in attributes.keys())}}}}}

Review + Response pairs:
"""
        
        for review in reviews:
            review_id = str(review['_id'])
            title = review.get('title', '') or ''
            text = review.get('review_text', '') or ''
            response = review.get('response_from_owner_text', '') or ''
            
            review_content = f"{title} {text}".strip()
            prompt += f"{review_id}:\nReview: {review_content}\nResponse: {response}\n\n"
        
        return prompt
    
    def _call_gemini_batch(self, prompt: str) -> Dict:
        """Call Gemini API with error handling."""
        try:
            # Check token limit
            max_tokens = self.config.get('processing', {}).get('max_tokens', 700000)
            estimated_tokens = len(prompt) // 4  # Rough estimation
            
            if estimated_tokens > max_tokens:
                self.logger.warning(f"Prompt too long ({estimated_tokens} tokens), skipping batch")
                return {}
            
            self.token_count += estimated_tokens
            
            response = self.genai_model.generate_content(prompt)
            
            if not response or not response.text:
                self.logger.error("Empty response from Gemini")
                return {}
            
            # Parse JSON response
            response_text = response.text.strip()
            
            # Remove markdown code blocks if present
            if response_text.startswith('```json'):
                response_text = response_text[7:]
            elif response_text.startswith('```'):
                response_text = response_text[3:]
            
            if response_text.endswith('```'):
                response_text = response_text[:-3]
            
            response_text = response_text.strip()
            
            # Parse JSON
            result = json.loads(response_text)
            return result
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse Gemini response as JSON: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Gemini API call failed: {e}")
            return {}
    
    def _validate_sentiment_response(self, response_data: Dict, expected_attributes: Set[str]) -> Dict:
        """Validate sentiment analysis response format."""
        validated_data = {}
        
        for review_id, attributes in response_data.items():
            if not isinstance(attributes, dict):
                continue
            
            validated_attributes = {}
            for attr_name, value in attributes.items():
                if attr_name in expected_attributes and isinstance(value, int) and 0 <= value <= 3:
                    validated_attributes[attr_name] = value
            
            if validated_attributes:
                validated_data[review_id] = validated_attributes
        
        return validated_data
    
    def _validate_binary_response(self, response_data: Dict, expected_attributes: Set[str]) -> Dict:
        """Validate binary classification response format."""
        validated_data = {}
        
        for review_id, attributes in response_data.items():
            if isinstance(attributes, int):
                # Handle complaint attribute (single integer)
                if 'is_complaint' in expected_attributes and 0 <= attributes <= 1:
                    validated_data[review_id] = {'is_complaint': attributes}
            elif isinstance(attributes, dict):
                # Handle response attributes (dictionary)
                validated_attributes = {}
                for attr_name, value in attributes.items():
                    if attr_name in expected_attributes and isinstance(value, int) and 0 <= value <= 1:
                        validated_attributes[attr_name] = value
                
                if validated_attributes:
                    validated_data[review_id] = validated_attributes
        
        return validated_data
    
    def _merge_enrichment_data(self, target: Dict, source: Dict):
        """Merge enrichment data dictionaries."""
        for review_id, attrs in source.items():
            if review_id not in target:
                target[review_id] = {}
            target[review_id].update(attrs)
    
    def _calculate_basic_fields(self, review: Dict) -> Dict:
        """Calculate has_response and review_length fields."""
        has_response = 1 if review.get('response_from_owner_text') else 0
        review_length = self._get_review_content_length(review)
        
        return {
            'has_response': has_response,
            'review_length': review_length
        }
    
    def _upsert_enriched_reviews(self, enrichment_data: Dict, reviews: List[Dict]):
        """Upsert enrichment data to enriched_reviews collection."""
        if not enrichment_data and not reviews:
            return
        
        operations = []
        processed_at = datetime.utcnow()
        
        for review in reviews:
            review_id = str(review['_id'])
            
            # Start with basic fields
            update_data = self._calculate_basic_fields(review)
            
            # Add LLM-generated fields if available
            if review_id in enrichment_data:
                update_data.update(enrichment_data[review_id])
            
            # Add metadata
            update_data.update({
                'establishment_id': review['establishment_id'],
                'platform': review['platform'],
                'published_at_date': review.get('published_at_date'),
                'processed_at': processed_at,
                'updated_at': processed_at
            })
            
            operation = UpdateOne(
                {'_id': ObjectId(review_id)},
                {'$set': update_data},
                upsert=True
            )
            operations.append(operation)
        
        if operations:
            try:
                result = self.db_manager.db.enriched_reviews.bulk_write(operations)
                self.logger.info(f"Upserted {result.upserted_count} new and modified {result.modified_count} existing enriched reviews")
            except Exception as e:
                self.logger.error(f"Error upserting enriched reviews: {e}")
    
    def get_processing_stats(self) -> Dict:
        """Get statistics about processed reviews."""
        try:
            base_stats = super().get_processing_stats()
            
            total_unified = base_stats.get('unified_reviews', 0)
            total_enriched = base_stats.get('enriched_reviews', 0)
            
            # Platform breakdown
            platform_pipeline = [
                {
                    "$group": {
                        "_id": "$platform",
                        "count": {"$sum": 1}
                    }
                }
            ]
            
            platform_stats = list(self.db_manager.db.enriched_reviews.aggregate(platform_pipeline))
            
            return {
                **base_stats,
                "total_unified_reviews": total_unified,
                "total_enriched_reviews": total_enriched,
                "unprocessed_count": total_unified - total_enriched,
                "platform_breakdown": platform_stats,
                "processing_coverage": f"{total_enriched}/{total_unified} ({(total_enriched/total_unified*100):.1f}%)" if total_unified > 0 else "0/0 (0%)",
                "estimated_tokens_used": self.token_count
            }
            
        except Exception as e:
            self.logger.error(f"Error getting processing stats: {e}")
            return {}