"""
Base processor class with common functionality for all processors.
"""

import logging
import yaml
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime

from .database import DatabaseManager

class BaseProcessor:
    """Base class for all processors with common functionality."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path
        self.config = {}
        self.db_manager = DatabaseManager()
        self.logger = self._setup_logging()
        
        if config_path:
            self._load_config()
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        return logging.getLogger(self.__class__.__name__)
    
    def _load_config(self):
        """Load configuration from YAML file."""
        try:
            config_file = Path(self.config_path)
            if not config_file.exists():
                self.logger.warning(f"Config file not found: {self.config_path}")
                return
                
            with open(config_file, 'r') as f:
                self.config = yaml.safe_load(f) or {}
            
            self.logger.info(f"Configuration loaded from {self.config_path}")
            
        except Exception as e:
            self.logger.error(f"Error loading config from {self.config_path}: {e}")
            self.config = {}
    
    def _load_tokens(self) -> Dict[str, str]:
        """Load API tokens from files."""
        tokens = {}
        token_files = {
            'mongodb_connection': 'tokens/mongodb_connection.txt',
            'apify_token': 'tokens/apify_token.txt',
            'google_api_key': 'tokens/google_api_key.txt'
        }
        
        for token_name, file_path in token_files.items():
            try:
                with open(file_path, 'r') as f:
                    tokens[token_name] = f.read().strip()
            except FileNotFoundError:
                self.logger.warning(f"Token file not found: {file_path}")
                tokens[token_name] = None
            except Exception as e:
                self.logger.error(f"Error reading token file {file_path}: {e}")
                tokens[token_name] = None
        
        return tokens
    
    def initialize(self) -> bool:
        """Initialize the processor with database connection."""
        tokens = self._load_tokens()
        
        if not tokens.get('mongodb_connection'):
            self.logger.error("MongoDB connection string not found")
            return False
        
        database_name = self.config.get('database', {}).get('name', 'review_scraper')
        
        if not self.db_manager.connect(tokens['mongodb_connection'], database_name):
            return False
        
        # Create indexes for optimal performance
        self.db_manager.create_indexes()
        
        self.logger.info(f"{self.__class__.__name__} initialized successfully")
        return True
    
    def cleanup(self):
        """Clean up resources."""
        if self.db_manager:
            self.db_manager.close_connection()
    
    def get_processing_stats(self) -> Dict:
        """Get basic processing statistics. Override in subclasses."""
        return self.db_manager.get_collections_stats()
    
    def _log_progress(self, current: int, total: int, operation: str = "Processing"):
        """Log progress for long-running operations."""
        if current % 100 == 0 or current == total:
            percentage = (current / total * 100) if total > 0 else 0
            self.logger.info(f"{operation}: {current}/{total} ({percentage:.1f}%)")
    
    def _batch_process(self, items: list, batch_size: int, process_func, operation_name: str = "Processing"):
        """
        Process items in batches with progress logging.
        
        Args:
            items: List of items to process
            batch_size: Size of each batch
            process_func: Function to process each batch
            operation_name: Name for logging
        """
        total_items = len(items)
        processed_count = 0
        
        for i in range(0, total_items, batch_size):
            batch = items[i:i + batch_size]
            
            try:
                result = process_func(batch)
                processed_count += len(batch)
                
                self._log_progress(processed_count, total_items, operation_name)
                
                yield result
                
            except Exception as e:
                self.logger.error(f"Error processing batch {i//batch_size + 1}: {e}")
                continue