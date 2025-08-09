#!/usr/bin/env python3
"""
Main entry point for the Clinic Reviews System.
Provides CLI interface for all operations.
"""

import sys
import argparse
import logging
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Import with absolute paths from src
from scrapers.excel_reader import ExcelReader
from scrapers.apify_client import ApifyClient
from processors.unification_processor import UnificationProcessor
from processors.enrichment_processor import EnrichmentProcessor
from processors.scoring_processor import ScoringProcessor
from processors.cms_processor import CMSProcessor
from core.database import DatabaseManager

def setup_logging(verbose: bool = False):
    """Setup global logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    
    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(logs_dir / 'clinic_reviews.log'),
            logging.StreamHandler()
        ]
    )

def load_tokens():
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
            print(f"Warning: Token file not found: {file_path}")
            tokens[token_name] = None
    
    return tokens

def scrape_reviews(args):
    """Scrape reviews from establishments in Excel file."""
    print(f"🔍 Starting review scraping from: {args.excel}")
    
    # Load tokens and config
    tokens = load_tokens()
    if not tokens.get('mongodb_connection') or not tokens.get('apify_token'):
        print("❌ Required tokens not found")
        return False
    
    # Initialize components
    db_manager = DatabaseManager()
    excel_reader = ExcelReader()
    
    try:
        # Connect to database
        if not db_manager.connect(tokens['mongodb_connection']):
            print("❌ Failed to connect to database")
            return False
        
        # Load configuration for Apify client
        unification_processor = UnificationProcessor()
        unification_processor._load_config()
        
        apify_client = ApifyClient(tokens['apify_token'], unification_processor.config)
        
        # Read establishments from Excel
        establishments = excel_reader.read_establishments(args.excel)
        if not establishments:
            print("❌ No establishments found in Excel file")
            return False
        
        # Process each establishment
        processed_establishments = []
        for est in establishments:
            # Check if establishment already exists
            existing = db_manager.get_establishment_by_url(est['google_url'])
            
            if existing:
                print(f"ℹ️  Establishment already exists: {est['display_name']}")
                establishment_id = str(existing['_id'])
            else:
                # Create new establishment
                establishment_id = db_manager.create_establishment(
                    est['display_name'],
                    est['google_url'],
                    est['website']
                )
                if not establishment_id:
                    print(f"❌ Failed to create establishment: {est['display_name']}")
                    continue
            
            processed_establishments.append({
                'id': establishment_id,
                'display_name': est['display_name'],
                'google_url': est['google_url'],
                'website': est['website']
            })
        
        print(f"📊 Starting scrape for {len(processed_establishments)} establishments")
        
        # Scrape reviews for each establishment
        total_google = 0
        total_trustpilot = 0
        
        for i, establishment in enumerate(processed_establishments, 1):
            print(f"🔄 Processing {i}/{len(processed_establishments)}: {establishment['display_name']}")
            
            try:
                # Scrape Google reviews
                print("  📍 Scraping Google Maps reviews...")
                google_reviews = apify_client.scrape_google_reviews(establishment['google_url'])
                google_count = db_manager.save_reviews("google_reviews", establishment['id'], google_reviews)
                
                # Update establishment stats
                db_manager.update_establishment_scrape_info(establishment['id'], 'google', google_count)
                
                # Scrape Trustpilot reviews
                print("  ⭐ Scraping Trustpilot reviews...")
                trustpilot_reviews = apify_client.scrape_trustpilot_reviews(establishment['website'])
                trustpilot_count = db_manager.save_reviews("trustpilot_reviews", establishment['id'], trustpilot_reviews)
                
                # Update establishment stats
                db_manager.update_establishment_scrape_info(establishment['id'], 'trustpilot', trustpilot_count)
                
                total_google += google_count
                total_trustpilot += trustpilot_count
                
                print(f"  ✅ Completed: Google={google_count}, Trustpilot={trustpilot_count}")
                
            except Exception as e:
                print(f"  ❌ Error scraping {establishment['display_name']}: {e}")
                continue
        
        print(f"🎉 Scraping complete! Total reviews: Google={total_google}, Trustpilot={total_trustpilot}")
        return True
        
    except Exception as e:
        print(f"❌ Error during scraping: {e}")
        return False
    finally:
        db_manager.close_connection()

def unify_reviews(args):
    """Unify reviews from raw collections."""
    print("🔄 Starting review unification...")
    
    processor = UnificationProcessor()
    
    try:
        if not processor.initialize():
            print("❌ Failed to initialize unification processor")
            return False
        
        establishment_ids = None
        if args.establishments:
            establishment_ids = [id.strip() for id in args.establishments.split(',')]
        
        result = processor.unify_reviews_incremental(establishment_ids)
        
        if not args.quick:
            stats = processor.get_unified_reviews_stats()
            print("\n📊 Unification Statistics:")
            print(f"Total unified reviews: {stats.get('total_reviews', 0)}")
            for platform_stat in stats.get('platform_breakdown', []):
                platform = platform_stat['_id']
                count = platform_stat['count']
                avg_rating = platform_stat.get('avg_rating', 0)
                print(f"  {platform.capitalize()}: {count} reviews (avg rating: {avg_rating:.2f})")
        
        return True
        
    except Exception as e:
        print(f"❌ Unification failed: {e}")
        return False
    finally:
        processor.cleanup()

def enrich_reviews(args):
    """Enrich reviews with AI analysis."""
    print("🤖 Starting review enrichment...")
    
    processor = EnrichmentProcessor()
    
    try:
        if not processor.initialize():
            print("❌ Failed to initialize enrichment processor")
            return False
        
        establishment_ids = None
        if args.establishments:
            establishment_ids = [id.strip() for id in args.establishments.split(',')]
        
        attribute_groups = None
        if args.attributes:
            attribute_groups = [attr.strip() for attr in args.attributes.split(',')]
        
        success = processor.process_reviews(
            establishment_ids=establishment_ids,
            published_after=args.published_after,
            incremental=not args.no_incremental,
            attribute_groups=attribute_groups
        )
        
        if not args.quick:
            stats = processor.get_processing_stats()
            print("\n📊 Enrichment Statistics:")
            for key, value in stats.items():
                print(f"{key}: {value}")
        
        return success
        
    except Exception as e:
        print(f"❌ Enrichment failed: {e}")
        return False
    finally:
        processor.cleanup()

def score_clinics(args):
    """Calculate clinic scores."""
    print("📊 Starting clinic scoring...")
    
    processor = ScoringProcessor()
    
    try:
        if not processor.initialize():
            print("❌ Failed to initialize scoring processor")
            return False
        
        establishment_ids = None
        if args.establishments:
            establishment_ids = [id.strip() for id in args.establishments.split(',')]
        
        result = processor.process_all_establishments(establishment_ids)
        
        if not args.quick:
            stats = processor.get_processing_stats()
            print("\n📊 Scoring Statistics:")
            for key, value in stats.items():
                if key == "average_scores":
                    print(f"{key}:")
                    for score_name, score_value in value.items():
                        print(f"  {score_name}: {score_value}")
                else:
                    print(f"{key}: {value}")
        
        return True
        
    except Exception as e:
        print(f"❌ Scoring failed: {e}")
        return False
    finally:
        processor.cleanup()

def update_cms(args):
    """Update CMS data from establishments collection."""
    print("📊 Starting CMS update...")
    
    processor = CMSProcessor(args.cms_directory)
    
    try:
        if not processor.initialize():
            print("❌ Failed to initialize CMS processor")
            return False
        
        success = processor.process_cms_update()
        
        if not args.quick:
            stats = processor.get_processing_stats()
            print("\n📊 CMS Update Statistics:")
            for key, value in stats.items():
                print(f"{key}: {value}")
        
        return success
        
    except Exception as e:
        print(f"❌ CMS update failed: {e}")
        return False
    finally:
        processor.cleanup()

def show_stats(args):
    """Show database statistics."""
    print("📊 Fetching database statistics...")
    
    db_manager = DatabaseManager()
    tokens = load_tokens()
    
    try:
        if not db_manager.connect(tokens['mongodb_connection']):
            print("❌ Failed to connect to database")
            return False
        
        stats = db_manager.get_collections_stats()
        
        print("\n" + "="*60)
        print("DATABASE STATISTICS")
        print("="*60)
        
        for collection, count in stats.items():
            print(f"{collection.replace('_', ' ').title()}: {count:,}")
        
        print("="*60)
        return True
        
    except Exception as e:
        print(f"❌ Error fetching statistics: {e}")
        return False
    finally:
        db_manager.close_connection()

def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Clinic Reviews System - Scrape, analyze, and score clinic reviews',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s scrape --excel data/establishments.xlsx
  %(prog)s unify --quick
  %(prog)s enrich --attributes sentiment,complaint
  %(prog)s score --establishments "id1,id2"
  %(prog)s cms --cms-directory "C:/path/to/cms"
  %(prog)s stats
        """
    )
    
    parser.add_argument('-v', '--verbose', action='store_true', 
                       help='Enable verbose logging')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Scrape command
    scrape_parser = subparsers.add_parser('scrape', help='Scrape reviews from establishments')
    scrape_parser.add_argument('--excel', required=True, 
                              help='Path to Excel file with establishments')
    
    # Unify command
    unify_parser = subparsers.add_parser('unify', help='Unify reviews from raw collections')
    unify_parser.add_argument('--establishments', 
                             help='Comma-separated establishment IDs to process')
    unify_parser.add_argument('--quick', action='store_true', 
                             help='Quick mode with minimal output')
    
    # Enrich command
    enrich_parser = subparsers.add_parser('enrich', help='Enrich reviews with AI analysis')
    enrich_parser.add_argument('--establishments', 
                              help='Comma-separated establishment IDs to process')
    enrich_parser.add_argument('--published-after', 
                              help='ISO date string (e.g., 2024-01-01T00:00:00.000Z)')
    enrich_parser.add_argument('--no-incremental', action='store_true', 
                              help='Process all reviews (not just new ones)')
    enrich_parser.add_argument('--attributes', 
                              help='Comma-separated attribute groups (sentiment,complaint,response,all)')
    enrich_parser.add_argument('--quick', action='store_true', 
                              help='Quick mode with minimal output')
    
    # Score command
    score_parser = subparsers.add_parser('score', help='Calculate clinic scores')
    score_parser.add_argument('--establishments', 
                             help='Comma-separated establishment IDs to process')
    score_parser.add_argument('--quick', action='store_true', 
                             help='Quick mode with minimal output')
    
    # CMS command
    cms_parser = subparsers.add_parser('cms', help='Update CMS data from establishments')
    cms_parser.add_argument('--cms-directory', default=r'C:\Users\yigit\Desktop\Enterprises\verisanus-6\cms',
                           help='Path to CMS directory')
    cms_parser.add_argument('--quick', action='store_true', 
                           help='Quick mode with minimal output')
    
    # Stats command
    subparsers.add_parser('stats', help='Show database statistics')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Setup logging
    setup_logging(args.verbose)
    
    # Execute command
    try:
        if args.command == 'scrape':
            success = scrape_reviews(args)
        elif args.command == 'unify':
            success = unify_reviews(args)
        elif args.command == 'enrich':
            success = enrich_reviews(args)
        elif args.command == 'score':
            success = score_clinics(args)
        elif args.command == 'cms':
            success = update_cms(args)
        elif args.command == 'stats':
            success = show_stats(args)
        else:
            print(f"❌ Unknown command: {args.command}")
            return
        
        if success:
            print(f"\n✅ {args.command.upper()} operation completed successfully!")
        else:
            print(f"\n❌ {args.command.upper()} operation failed!")
            
    except KeyboardInterrupt:
        print("\n\n⚠️ Operation interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()