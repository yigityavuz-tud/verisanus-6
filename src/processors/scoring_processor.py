"""
Scoring processor - calculates clinic scores based on enriched review data with weighted scoring.
"""

import statistics
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from collections import defaultdict, Counter
from bson import ObjectId

from core.base_processor import BaseProcessor

class ScoringProcessor(BaseProcessor):
    """Calculates various scores for clinics based on enriched review data with weighted scoring."""
    
    def __init__(self, config_path: str = "config/scoring_config.yaml"):
        super().__init__(config_path)
        
        # Get weights from config
        self.service_quality_weights = self.config.get('service_quality_weights', {
            'treatment_satisfaction': 0.30,
            'post_op': 0.20,
            'staff_satisfaction': 0.30,
            'facility': 0.20
        })
        
        self.communication_weights = self.config.get('communication_weights', {
            'onsite_communication': 0.40,
            'scheduling': 0.20,
            'online_communication': 0.40
        })
    
    def process_all_establishments(self, establishment_ids: List[str] = None) -> Dict[str, int]:
        """
        Process scores for all establishments.
        
        Args:
            establishment_ids: Optional list of establishment IDs to process
            
        Returns:
            Dictionary with processing results
        """
        self.logger.info("Starting clinic scoring process...")
        
        # Get establishments to process
        if establishment_ids:
            establishments = [{"_id": ObjectId(eid)} for eid in establishment_ids]
            self.logger.info(f"Processing {len(establishment_ids)} specified establishments")
        else:
            establishments = list(self.db_manager.db.establishments.find({}, {"_id": 1}))
            self.logger.info(f"Processing all {len(establishments)} establishments")
        
        processed_count = 0
        updated_count = 0
        
        for establishment in establishments:
            establishment_id = str(establishment["_id"])
            
            try:
                # Calculate scores
                scores = self._calculate_establishment_scores(establishment_id)
                
                # Update establishment document
                if scores:
                    result = self.db_manager.db.establishments.update_one(
                        {"_id": establishment["_id"]},
                        {"$set": scores}
                    )
                    
                    if result.modified_count > 0:
                        updated_count += 1
                
                processed_count += 1
                
                if processed_count % 10 == 0:
                    self.logger.info(f"Processed {processed_count}/{len(establishments)} establishments")
                
            except Exception as e:
                self.logger.error(f"Error processing establishment {establishment_id}: {e}")
                continue
        
        self.logger.info(f"Scoring complete! Processed: {processed_count}, Updated: {updated_count}")
        return {"processed": processed_count, "updated": updated_count}
    
    def _get_reviewer_weight(self, review: Dict) -> float:
        """Calculate reviewer weight based on platform and authenticity indicators."""
        platform = review.get('platform', '')
        
        if platform == 'google':
            # Google Local Guide gets 1.25x weight
            if review.get('is_local_guide', False):
                return 1.25
        elif platform == 'trustpilot':
            # Trustpilot verified reviews get 1.25x weight
            if review.get('verification_level') == 'verified':
                return 1.25
        
        return 1.0  # Default weight
    
    def _calculate_weighted_star_rating(self, rating_reviews: List[Dict]) -> Tuple[Optional[float], Optional[float]]:
        """Calculate both raw and weighted average star ratings."""
        if not rating_reviews:
            return None, None
        
        # Raw average
        ratings = [review['rating'] for review in rating_reviews]
        raw_average = statistics.mean(ratings)
        
        # Weighted average
        weighted_sum = 0
        total_weight = 0
        
        for review in rating_reviews:
            weight = self._get_reviewer_weight(review)
            weighted_sum += review['rating'] * weight
            total_weight += weight
        
        weighted_average = weighted_sum / total_weight if total_weight > 0 else None
        
        return round(raw_average, 3), round(weighted_average, 3)
    
    def _calculate_establishment_scores(self, establishment_id: str) -> Dict:
        """Calculate all scores for a single establishment."""
        try:
            # Get rating data from unified_reviews
            rating_reviews = list(self.db_manager.db.unified_reviews.find(
                {
                    "establishment_id": establishment_id,
                    "rating": {"$exists": True, "$ne": None, "$gt": 0}
                },
                {"_id": 1, "rating": 1, "platform": 1, "is_local_guide": 1, "verification_level": 1}
            ))
            
            # Get enriched data with platform info for weighting
            enriched_reviews = list(self.db_manager.db.unified_reviews.aggregate([
                {"$match": {"establishment_id": establishment_id}},
                {"$lookup": {
                    "from": "enriched_reviews",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "enriched"
                }},
                {"$match": {"enriched": {"$ne": []}}},
                {"$project": {
                    "_id": 1,
                    "platform": 1,
                    "is_local_guide": 1,
                    "verification_level": 1,
                    "enriched": {"$arrayElemAt": ["$enriched", 0]}
                }}
            ]))
            
            if not rating_reviews and not enriched_reviews:
                return {}
            
            # Calculate star ratings (raw and weighted)
            raw_rating, weighted_rating = self._calculate_weighted_star_rating(rating_reviews)
            
            # Initialize weighted attribute score counters
            attribute_scores = defaultdict(lambda: {"scores": [], "weights": []})
            
            # Process enriched data with weights
            for review_data in enriched_reviews:
                enriched = review_data.get('enriched', {})
                weight = self._get_reviewer_weight(review_data)
                
                # Calculate online communication score
                online_comm_score = self._calculate_online_communication_score(enriched)
                if online_comm_score is not None:
                    attribute_scores['online_communication']["scores"].append(online_comm_score)
                    attribute_scores['online_communication']["weights"].append(weight)
                
                # Collect other attribute scores
                for attribute in ['staff_satisfaction', 'scheduling', 'treatment_satisfaction', 
                                'onsite_communication', 'facility', 'post_op', 'affordability', 
                                'recommendation']:
                    score = enriched.get(attribute)
                    if score is not None:
                        attribute_scores[attribute]["scores"].append(score)
                        attribute_scores[attribute]["weights"].append(weight)
            
            # Calculate weighted NPS scores for each attribute
            nps_scores = {}
            for attribute, data in attribute_scores.items():
                nps_score = self._calculate_weighted_nps_score(data["scores"], data["weights"])
                if nps_score is not None:
                    nps_scores[attribute] = nps_score
            
            # Calculate composite scores using weighted NPS scores
            service_quality_score = self._calculate_composite_score(
                nps_scores, self.service_quality_weights
            )
            
            communication_score = self._calculate_composite_score(
                nps_scores, self.communication_weights
            )
            
            # Prepare results
            results = {
                "total_reviews_analyzed": len(set([r['_id'] for r in rating_reviews] + 
                                               [r['_id'] for r in enriched_reviews])),
                "scores_updated_at": datetime.utcnow()
            }
            
            # Add star ratings
            if raw_rating is not None:
                results["raw_average_rating"] = raw_rating
            if weighted_rating is not None:
                results["weighted_average_rating"] = weighted_rating
            
            # Add individual NPS scores
            for attribute in ['affordability', 'recommendation']:
                if attribute in nps_scores:
                    results[f"{attribute}_score"] = nps_scores[attribute]
            
            # Add composite scores
            if service_quality_score is not None:
                results["service_quality_score"] = service_quality_score
            
            if communication_score is not None:
                results["communication_score"] = communication_score
            
            # Add online communication score separately for transparency
            if 'online_communication' in nps_scores:
                results["online_communication_score"] = nps_scores['online_communication']
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error calculating scores for establishment {establishment_id}: {e}")
            return {}
    
    def _calculate_online_communication_score(self, enriched_data: Dict) -> Optional[int]:
        """Calculate online communication score using rule-based logic."""
        is_complaint = enriched_data.get('is_complaint', 0)
        has_response = enriched_data.get('has_response', 0)
        has_constructive_response = enriched_data.get('has_constructive_response', 0)
        
        # Get rules from config
        rules = self.config.get('online_communication_rules', {
            'no_complaint': 0,
            'complaint_no_response': 1,
            'complaint_response_poor': 2,
            'complaint_response_good': 3
        })
        
        if is_complaint != 1:
            return rules['no_complaint']  # Not a complaint
        elif is_complaint == 1 and has_response == 0:
            return rules['complaint_no_response']  # Complaint without response
        elif is_complaint == 1 and has_response == 1 and has_constructive_response == 1:
            return rules['complaint_response_good']  # Complaint with constructive response
        elif is_complaint == 1 and has_response == 1:
            return rules['complaint_response_poor']  # Complaint with response but not constructive
        else:
            return rules['no_complaint']  # Default case
    
    def _calculate_weighted_nps_score(self, scores: List[int], weights: List[float]) -> Optional[float]:
        """Calculate weighted NPS-style score from scores and weights."""
        if not scores or len(scores) != len(weights):
            return None
        
        # Get which scores to include from config
        include_scores = self.config.get('scoring', {}).get('nps_include_scores', [1, 2, 3])
        
        weighted_positive = 0  # Score 3 = positive
        weighted_neutral = 0   # Score 2 = neutral/mixed
        weighted_negative = 0  # Score 1 = negative
        total_weight = 0
        
        for score, weight in zip(scores, weights):
            if score in include_scores:
                total_weight += weight
                if score == 3:
                    weighted_positive += weight
                elif score == 2:
                    weighted_neutral += weight
                elif score == 1:
                    weighted_negative += weight
        
        if total_weight == 0:
            return None  # No data to calculate score
        
        nps_score = ((weighted_positive - weighted_negative) / total_weight) * 100
        return round(nps_score, 2)
    
    def _calculate_composite_score(self, individual_scores: Dict, weights: Dict) -> Optional[float]:
        """Calculate weighted composite score."""
        weighted_sum = 0
        total_weight = 0
        
        for attribute, weight in weights.items():
            score = individual_scores.get(attribute)
            if score is not None:  # Only include attributes with valid scores
                weighted_sum += score * weight
                total_weight += weight
        
        if total_weight == 0:
            return None  # No valid scores to calculate composite
        
        # Normalize by actual total weight used
        composite_score = weighted_sum / total_weight
        return round(composite_score, 2)
    
    def get_processing_stats(self) -> Dict:
        """Get statistics about the scoring process."""
        try:
            base_stats = super().get_processing_stats()
            
            # Count establishments with scores
            establishments_with_scores = self.db_manager.db.establishments.count_documents({
                "weighted_average_rating": {"$exists": True}
            })
            
            total_establishments = base_stats.get('establishments', 0)
            
            # Get score distribution
            pipeline = [
                {
                    "$match": {"weighted_average_rating": {"$exists": True}}
                },
                {
                    "$group": {
                        "_id": None,
                        "avg_raw_rating": {"$avg": "$raw_average_rating"},
                        "avg_weighted_rating": {"$avg": "$weighted_average_rating"},
                        "avg_service_quality": {"$avg": "$service_quality_score"},
                        "avg_communication": {"$avg": "$communication_score"},
                        "avg_affordability": {"$avg": "$affordability_score"},
                        "avg_recommendation": {"$avg": "$recommendation_score"}
                    }
                }
            ]
            
            stats_result = list(self.db_manager.db.establishments.aggregate(pipeline))
            averages = stats_result[0] if stats_result else {}
            
            return {
                **base_stats,
                "total_establishments": total_establishments,
                "establishments_with_scores": establishments_with_scores,
                "coverage_percentage": round((establishments_with_scores / total_establishments * 100), 2) if total_establishments > 0 else 0,
                "average_scores": {k: round(v, 2) if v else None for k, v in averages.items() if k != "_id"}
            }
            
        except Exception as e:
            self.logger.error(f"Error getting scoring stats: {e}")
            return {}