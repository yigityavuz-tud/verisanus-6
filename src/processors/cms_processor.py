"""
CMS processor - updates CMS data from establishments collection.
"""

import os
import re
import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from pathlib import Path

from core.base_processor import BaseProcessor

class CMSProcessor(BaseProcessor):
    """Processes establishments data and updates CMS file."""
    
    def __init__(self, cms_directory: str = r"C:\Users\yigit\Desktop\Enterprises\verisanus-6\cms"):
        super().__init__()
        self.cms_directory = Path(cms_directory)
        
        # Field mappings from establishments to CMS
        self.field_mappings = {
            'weighted_average_rating': 'Weighted Rating Average',
            'communication_score_normalized': 'Communication Score',
            'affordability_score_normalized': 'Affordability Score', 
            'service_quality_score_normalized': 'Service Score',
            'recommendation_score_normalized': 'Recommendation Score',
            'affordability_pct_weighted': 'Affordability Stat',
            'recommendation_pct_weighted': 'Recommendation Stat',
            'facility_pct_weighted': 'Facility Stat',
            'onsite_communication_pct_weighted': 'Onsite Communication Stat',
            'post_op_pct_weighted': 'Post-op Stat',
            'scheduling_pct_weighted': 'Scheduling Stat',
            'staff_satisfaction_pct_weighted': 'Staff Stat',
            'treatment_satisfaction_pct_weighted': 'Treatment Stat',
            'clinic_score': 'Score'
        }
        
        # Rank fields to calculate
        self.rank_fields = {
            'Communication Score': 'Communication Rank',
            'Affordability Score': 'Affordability Rank',
            'Service Score': 'Service Rank', 
            'Recommendation Score': 'Recommendation Rank',
            'Score': 'Rank'
        }
    
    def find_latest_cms_file(self) -> Optional[Path]:
        """Find the CMS file with the highest suffix number."""
        try:
            pattern = re.compile(r'ClinicScores - Clinics\((\d+)\)\.csv$')
            max_number = -1
            latest_file = None
            
            for file_path in self.cms_directory.glob('ClinicScores - Clinics*.csv'):
                match = pattern.search(file_path.name)
                if match:
                    number = int(match.group(1))
                    if number > max_number:
                        max_number = number
                        latest_file = file_path
            
            if latest_file:
                self.logger.info(f"Found latest CMS file: {latest_file.name}")
                return latest_file
            else:
                self.logger.error("No CMS files found matching pattern")
                return None
                
        except Exception as e:
            self.logger.error(f"Error finding latest CMS file: {e}")
            return None
    
    def load_cms_data(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Load CMS data with proper UTF-8 BOM encoding."""
        try:
            # Try UTF-8 BOM first, then UTF-8, then detect encoding
            encodings = ['utf-8-sig', 'utf-8', 'cp1252']
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    self.logger.info(f"Successfully loaded CMS data with {encoding} encoding")
                    self.logger.info(f"CMS file contains {len(df)} records")
                    return df
                except UnicodeDecodeError:
                    continue
            
            self.logger.error("Failed to load CMS file with any encoding")
            return None
            
        except Exception as e:
            self.logger.error(f"Error loading CMS data: {e}")
            return None
    
    def get_establishments_data(self) -> Dict[str, Dict]:
        """Get establishments data indexed by display_name."""
        try:
            establishments = list(self.db_manager.db.establishments.find({}))
            
            # Index by display_name for fast lookup
            data_by_name = {}
            for est in establishments:
                display_name = est.get('display_name')
                if display_name:
                    # Use total_reviews_analyzed field for Reviews count
                    total_reviews = est.get('total_reviews_analyzed', 0) or 0
                    
                    # Add calculated field
                    est['total_reviews'] = total_reviews
                    data_by_name[display_name] = est
            
            self.logger.info(f"Loaded data for {len(data_by_name)} establishments")
            return data_by_name
            
        except Exception as e:
            self.logger.error(f"Error loading establishments data: {e}")
            return {}
    
    def update_cms_data(self, cms_df: pd.DataFrame, establishments_data: Dict[str, Dict]) -> pd.DataFrame:
        """Update CMS data with establishments data."""
        updated_count = 0
        
        # Filter for Display=TRUE records
        display_true_mask = cms_df['Display'].astype(str).str.upper() == 'TRUE'
        display_true_records = cms_df[display_true_mask].copy()
        
        self.logger.info(f"Found {len(display_true_records)} records with Display=TRUE")
        
        # Update field values
        for idx, row in display_true_records.iterrows():
            display_name = row['Display Name']
            
            if display_name in establishments_data:
                est_data = establishments_data[display_name]
                
                # Update Reviews field
                cms_df.at[idx, 'Reviews'] = est_data.get('total_reviews', 0)
                
                # Update mapped fields
                for est_field, cms_field in self.field_mappings.items():
                    value = est_data.get(est_field)
                    if value is not None:
                        cms_df.at[idx, cms_field] = value
                
                updated_count += 1
            else:
                self.logger.warning(f"Establishment not found in database: {display_name}")
        
        self.logger.info(f"Updated {updated_count} CMS records with establishment data")
        
        # Calculate rankings
        cms_df = self._calculate_rankings(cms_df)
        
        return cms_df
    
    def _calculate_rankings(self, cms_df: pd.DataFrame) -> pd.DataFrame:
        """Calculate rankings for Display=TRUE records with tie-breaking by weighted_average_rating."""
        # Filter for Display=TRUE records
        display_true_mask = cms_df['Display'].astype(str).str.upper() == 'TRUE'
        
        for score_field, rank_field in self.rank_fields.items():
            try:
                # Get valid scores for Display=TRUE records
                valid_scores_mask = (
                    display_true_mask & 
                    pd.notna(cms_df[score_field]) & 
                    (cms_df[score_field] != '') &
                    pd.notna(cms_df['Weighted Rating Average']) &
                    (cms_df['Weighted Rating Average'] != '')
                )
                
                if valid_scores_mask.sum() == 0:
                    self.logger.warning(f"No valid scores found for {score_field}")
                    continue
                
                # Get the subset of data for ranking
                ranking_data = cms_df.loc[valid_scores_mask].copy()
                
                # Convert to numeric if needed
                ranking_data[score_field] = pd.to_numeric(ranking_data[score_field], errors='coerce')
                ranking_data['Weighted Rating Average'] = pd.to_numeric(ranking_data['Weighted Rating Average'], errors='coerce')
                
                # Remove any rows where conversion failed
                valid_numeric_mask = (
                    pd.notna(ranking_data[score_field]) & 
                    pd.notna(ranking_data['Weighted Rating Average'])
                )
                ranking_data = ranking_data.loc[valid_numeric_mask]
                
                if len(ranking_data) == 0:
                    self.logger.warning(f"No valid numeric scores found for {score_field}")
                    continue
                
                # Sort by primary score (descending) then by weighted_average_rating (descending) for tie-breaking
                ranking_data = ranking_data.sort_values(
                    [score_field, 'Weighted Rating Average'], 
                    ascending=[False, False]
                )
                
                # Assign ranks using standard ranking (1, 2, 2, 4, 5...)
                ranking_data['rank'] = range(1, len(ranking_data) + 1)
                
                # Initialize rank column with NaN
                if rank_field not in cms_df.columns:
                    cms_df[rank_field] = pd.NA
                
                # Update ranks in the main dataframe
                for idx, rank in zip(ranking_data.index, ranking_data['rank']):
                    cms_df.loc[idx, rank_field] = rank
                
                self.logger.info(f"Calculated ranks for {score_field}: {len(ranking_data)} records")
                
            except Exception as e:
                self.logger.error(f"Error calculating ranks for {score_field}: {e}")
                continue
        
        return cms_df
    
    def save_cms_data(self, cms_df: pd.DataFrame, original_file_path: Path) -> bool:
        """Save updated CMS data with UTF-8 BOM encoding."""
        try:
            # Create backup of original file
            backup_path = original_file_path.with_suffix('.csv.backup')
            if original_file_path.exists():
                original_file_path.replace(backup_path)
                self.logger.info(f"Created backup: {backup_path.name}")
            
            # Save with UTF-8 BOM encoding
            cms_df.to_csv(original_file_path, index=False, encoding='utf-8-sig')
            
            self.logger.info(f"Successfully saved updated CMS data to {original_file_path.name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving CMS data: {e}")
            return False
    
    def process_cms_update(self) -> bool:
        """Main method to process CMS update."""
        try:
            self.logger.info("Starting CMS update process...")
            
            # Find latest CMS file
            cms_file_path = self.find_latest_cms_file()
            if not cms_file_path:
                return False
            
            # Load CMS data
            cms_df = self.load_cms_data(cms_file_path)
            if cms_df is None:
                return False
            
            # Load establishments data
            establishments_data = self.get_establishments_data()
            if not establishments_data:
                self.logger.error("No establishments data found")
                return False
            
            # Update CMS data
            updated_cms_df = self.update_cms_data(cms_df, establishments_data)
            
            # Save updated data
            if self.save_cms_data(updated_cms_df, cms_file_path):
                self.logger.info("CMS update process completed successfully")
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"Error in CMS update process: {e}")
            return False
    
    def get_processing_stats(self) -> Dict:
        """Get statistics about CMS processing."""
        try:
            # Find latest CMS file
            cms_file_path = self.find_latest_cms_file()
            if not cms_file_path:
                return {"error": "No CMS file found"}
            
            # Load CMS data for stats
            cms_df = self.load_cms_data(cms_file_path)
            if cms_df is None:
                return {"error": "Could not load CMS file"}
            
            # Count Display=TRUE records
            display_true_count = (cms_df['Display'].astype(str).str.upper() == 'TRUE').sum()
            
            # Count records with scores
            score_fields = ['Communication Score', 'Affordability Score', 'Service Score', 'Recommendation Score', 'Score']
            records_with_scores = 0
            
            for field in score_fields:
                if field in cms_df.columns:
                    valid_scores = pd.notna(cms_df[field]) & (cms_df[field] != '')
                    if valid_scores.sum() > records_with_scores:
                        records_with_scores = valid_scores.sum()
            
            return {
                "cms_file": cms_file_path.name,
                "total_cms_records": len(cms_df),
                "display_true_records": display_true_count,
                "records_with_scores": records_with_scores,
                "last_modified": datetime.fromtimestamp(cms_file_path.stat().st_mtime).isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting CMS stats: {e}")
            return {"error": str(e)}