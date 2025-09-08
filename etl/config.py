"""
AMAzing SMS - ETL Configuration
Configuration settings for the ETL pipeline
"""

import os
from pathlib import Path
from typing import Dict, Any

class Config:
    """Configuration class for ETL pipeline settings"""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent.parent
        self.data_dir = self.base_dir / "data"
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        self.logs_dir = self.data_dir / "logs"
        self.dead_letter_dir = self.logs_dir / "dead_letter"
        
        # Database settings
        self.database_url = os.getenv("DATABASE_URL", f"sqlite:///{self.data_dir}/db.sqlite3")
        
        # ETL settings
        self.batch_size = int(os.getenv("BATCH_SIZE", "100"))
        self.max_retries = int(os.getenv("MAX_RETRIES", "3"))
        self.retry_delay = int(os.getenv("RETRY_DELAY", "5"))
        
        # XML processing settings
        self.xml_encoding = "utf-8"
        self.xml_schema_validation = True
        
        # Logging settings
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.log_file = self.logs_dir / "etl.log"
        
        # Create directories if they don't exist
        self._create_directories()
    
    def _create_directories(self):
        """Create necessary directories if they don't exist"""
        directories = [
            self.data_dir,
            self.raw_dir,
            self.processed_dir,
            self.logs_dir,
            self.dead_letter_dir
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration dictionary"""
        return {
            "url": self.database_url,
            "echo": os.getenv("DB_ECHO", "false").lower() == "true"
        }
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration dictionary"""
        return {
            "level": self.log_level,
            "file": str(self.log_file),
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
