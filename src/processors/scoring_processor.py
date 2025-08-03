"""
Scoring processor - calculates clinic scores based on enriched review data.
"""

import statistics
from typing import Dict, List, Optional
from datetime import datetime
from collections import defaultdict, Counter
from bson import ObjectId

from ..core.base_processor import BaseProcessor

class ScoringProcessor(BaseProcessor):
    """Calculates various scores for clinics based on enriched review data."""
    
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
        
        self.prior_weight = self.config.get('bayesian', {}).get('prior_weight', 100)
    
    def process_all_establishments(self, establishment_ids: List[str] = None) -> Dict[str, int]:
        """
        Process scores for all establishments.
        
        Args:
            establishment_ids: Optional list of establishment IDs to process
            
        Returns:
            Dictionary with processing results
        """
        self.logger.info("Starting clinic scoring process...")
        
        # Calculate prior average rating
        prior_avg = self._calculate_prior_average()
        
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
                scores = self._calculate_establishment_scores(establishment_id, prior_avg)
                
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
    
    def _calculate_prior_average(self) -> float:
        """Calculate sample average rating across all reviews."""
        try:
            pipeline = [
                {
                    "$match": {
                        "rating": {"$exists": True, "$ne": None, "$gt": 0}
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "avg_rating": {"$avg": "$rating"}
                    }
                }
            ]
            
            result = list(self.db_manager.db.unified_reviews.aggregate(pipeline))
            if result and result[0].get('avg_rating'):
                prior_avg = result[0]['avg_rating']
                self.logger.info(f"Calculated prior average rating: {prior_avg:.3f}")
                return prior_avg
            else:
                # Fallback to reasonable default
                default_avg = 4.0
                self.logger.warning(f"Could not calculate prior average, using default: {default_avg}")
                return default_avg
                
        except Exception as e:
            self.logger.error(f"Error calculating prior average: {e}")
            return 4.0
    
    def _calculate_establishment_scores(self, establishment_id: str, prior_avg: float) -> Dict:
        """Calculate all scores for a single establishment."""
        try:
            # Get rating data from unified_reviews
            rating_reviews = list(self.db_manager.db.unified_reviews.find(
                {
                    "establishment_id": establishment_id,
                    "rating": {"$exists": True, "$ne": None, "$gt": 0}
                },
                {"_id": 1, "rating": 1}
            ))
            
            # Get enriched data
            enriched_reviews = list(self.db_manager.db.enriched_reviews.find(
                {"establishment_id": establishment_id}
            ))
            
            if not rating_reviews and not enriched_reviews:
                return {}
            
            # Extract ratings
            ratings = [review['rating'] for review in rating_reviews]
            
            # Calculate adjusted rating
            adjusted_rating = self._calculate_adjusted_rating(ratings, prior_avg)
            
            # Initialize score counters
            attribute_scores = defaultdict(Counter)
            
            # Process enriched data
            for enriched in enriched_reviews:
                # Calculate online communication score
                online_comm_score = self._calculate_online_communication_score(enriched)
                attribute_scores['online_communication'][online_comm_score] += 1
                
                # Collect other attribute scores
                for attribute in ['staff_satisfaction', 'scheduling', 'treatment_satisfaction', 
                                'onsite_communication', 'facility', 'post_op', 'affordability', 
                                'recommendation']:
                    score = enriched.get(attribute)
                    if score is not None:
                        attribute_scores[attribute][score] += 1
            
            # Calculate NPS scores for each attribute
            nps_scores = {}
            for attribute, score_counts in attribute_scores.items():
                nps_score = self._calculate_nps_score(score_counts)
                if nps_score is not None:
                    nps_scores[attribute] = nps_score
            
            # Calculate composite scores
            service_quality_score = self._calculate_composite_score(
                nps_scores, self.service_quality_weights
            )
            
            communication_score = self._calculate_composite_score(
                nps_scores, self.communication_weights
            )
            
            # Prepare results
            results = {
                "adjusted_rating": adjusted_rating,
                "total_reviews_analyzed": len(set([r['_id'] for r in rating_reviews] + 
                                               [r['_id'] for r in enriched_reviews])),
                "scores_updated_at": datetime.utcnow()
            }
            
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
    
    def _calculate_adjusted_rating(self, ratings: List[float], prior_avg: float) -> Optional[float]:
        """Calculate Bayesian adjusted rating."""
        if not ratings:
            return None
        
        review_count = len(ratings)
        clinic_avg = statistics.mean(ratings)
        
        adjusted_rating = (
            (self.prior_weight * prior_avg + review_count * clinic_avg) / 
            (self.prior_weight + review_count)
        )
        
        return round(adjusted_rating, 3)
    
    def _calculate_online_communication_score(self, enriched_data: Dict) -> int:
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
    
    def _calculate_nps_score(self, score_counts: Counter) -> Optional[float]:
        """Calculate NPS-style score from score distribution."""
        # Get which scores to include from config
        include_scores = self.config.get('scoring', {}).get('nps_include_scores', [1, 2, 3])
        
        positive_count = score_counts.get(3, 0)  # Score 3 = positive
        neutral_count = score_counts.get(2, 0)   # Score 2 = neutral/mixed
        negative_count = score_counts.get(1, 0)  # Score 1 = negative
        
        # Only count scores that should be included
        total_count = sum(score_counts.get(score, 0) for score in include_scores)
        
        if total_count == 0:
            return None  # No data to calculate score
        
        nps_score = ((positive_count - negative_count) / total_count) * 100
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
                "adjusted_rating": {"$exists": True}
            })
            
            total_establishments = base_stats.get('establishments', 0)
            
            # Get score distribution
            pipeline = [
                {
                    "$match": {"adjusted_rating": {"$exists": True}}
                },
                {
                    "$group": {
                        "_id": None,
                        "avg_adjusted_rating": {"$avg": "$adjusted_rating"},
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