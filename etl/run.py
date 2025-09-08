"""
MoMo Data Processor - ETL Runner
Main ETL pipeline orchestration
"""

import logging
import sys
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
import argparse

from .config import Config
from .parse_xml import XMLParser
from .clean_normalize import DataCleaner
from .categorize import MessageCategorizer
from .load_db import DatabaseLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/etl.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class ETLRunner:
    """Main ETL pipeline runner"""
    
    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.xml_parser = XMLParser(self.config)
        self.data_cleaner = DataCleaner(self.config)
        self.categorizer = MessageCategorizer(self.config)
        self.db_loader = DatabaseLoader(self.config)
        
        logger.info("ETL Runner initialized")
    
    def run_full_pipeline(self, input_path: Path = None) -> Dict[str, Any]:
        """
        Run the complete ETL pipeline
        
        Args:
            input_path: Path to input data (file or directory)
            
        Returns:
            Dictionary with pipeline execution results
        """
        start_time = datetime.now()
        logger.info("Starting full ETL pipeline")
        
        try:
            # Step 1: Extract data
            logger.info("Step 1: Extracting data")
            raw_records = self._extract_data(input_path)
            
            if not raw_records:
                logger.warning("No data extracted, pipeline completed")
                return {
                    'status': 'completed',
                    'total_records': 0,
                    'successful_records': 0,
                    'failed_records': 0,
                    'processing_time': 0,
                    'errors': ['No data found to process']
                }
            
            # Step 2: Clean and normalize data
            logger.info("Step 2: Cleaning and normalizing data")
            cleaned_records = self.data_cleaner.clean_records(raw_records)
            
            # Step 3: Categorize messages
            logger.info("Step 3: Categorizing messages")
            categorized_records = self.categorizer.categorize_records(cleaned_records)
            
            # Step 4: Load data into database
            logger.info("Step 4: Loading data into database")
            load_results = self.db_loader.load_records(categorized_records)
            
            # Step 5: Update statistics
            logger.info("Step 5: Updating statistics")
            self.db_loader.update_category_statistics(categorized_records)
            
            # Calculate final statistics
            processing_time = (datetime.now() - start_time).total_seconds()
            
            results = {
                'status': 'completed',
                'total_records': len(raw_records),
                'successful_records': load_results['successful_records'],
                'failed_records': load_results['failed_records'],
                'processing_time': processing_time,
                'batch_id': load_results.get('batch_id'),
                'errors': load_results.get('errors', [])
            }
            
            logger.info(f"ETL pipeline completed successfully in {processing_time:.2f}s")
            logger.info(f"Processed {results['successful_records']}/{results['total_records']} records")
            
            return results
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'processing_time': (datetime.now() - start_time).total_seconds()
            }
    
    def _extract_data(self, input_path: Path = None) -> List[Dict[str, Any]]:
        """Extract data from input source"""
        if input_path is None:
            input_path = self.config.raw_dir
        
        if not input_path.exists():
            logger.error(f"Input path does not exist: {input_path}")
            return []
        
        if input_path.is_file():
            return self.xml_parser.parse_file(input_path)
        elif input_path.is_dir():
            return self.xml_parser.parse_directory(input_path)
        else:
            logger.error(f"Invalid input path: {input_path}")
            return []
    
    def run_extraction_only(self, input_path: Path = None) -> List[Dict[str, Any]]:
        """Run only the extraction step"""
        logger.info("Running extraction only")
        return self._extract_data(input_path)
    
    def run_cleaning_only(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Run only the cleaning step"""
        logger.info("Running cleaning only")
        return self.data_cleaner.clean_records(records)
    
    def run_categorization_only(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Run only the categorization step"""
        logger.info("Running categorization only")
        return self.categorizer.categorize_records(records)
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status and statistics"""
        try:
            db_stats = self.db_loader.get_database_stats()
            return {
                'status': 'ready',
                'database_stats': db_stats,
                'config': {
                    'raw_data_path': str(self.config.raw_dir),
                    'processed_data_path': str(self.config.processed_dir),
                    'database_url': self.config.database_url
                }
            }
        except Exception as e:
            logger.error(f"Failed to get pipeline status: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }

def main():
    """Main entry point for ETL pipeline"""
    parser = argparse.ArgumentParser(description='MoMo Data Processor ETL Pipeline')
    parser.add_argument('--input', '-i', type=str, help='Input file or directory path')
    parser.add_argument('--step', '-s', choices=['extract', 'clean', 'categorize', 'load', 'full'], 
                       default='full', help='ETL step to run')
    parser.add_argument('--config', '-c', type=str, help='Configuration file path')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize ETL runner
    config = Config()
    runner = ETLRunner(config)
    
    # Run based on specified step
    if args.step == 'full':
        input_path = Path(args.input) if args.input else None
        results = runner.run_full_pipeline(input_path)
        print(f"ETL Pipeline Results: {results}")
    
    elif args.step == 'extract':
        input_path = Path(args.input) if args.input else None
        records = runner.run_extraction_only(input_path)
        print(f"Extracted {len(records)} records")
    
    else:
        print(f"Step '{args.step}' not implemented for standalone execution")
        sys.exit(1)

if __name__ == "__main__":
    main()
