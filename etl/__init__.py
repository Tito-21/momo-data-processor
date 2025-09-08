"""
MoMo Data Processor - ETL Package
Extract, Transform, Load pipeline for MoMo data processing
"""

__version__ = "1.0.0"
__author__ = "MoMo Data Processor Team"

# ETL modules
from .config import Config
from .parse_xml import XMLParser
from .clean_normalize import DataCleaner
from .categorize import MessageCategorizer
from .load_db import DatabaseLoader
from .run import ETLRunner

__all__ = [
    'Config',
    'XMLParser', 
    'DataCleaner',
    'MessageCategorizer',
    'DatabaseLoader',
    'ETLRunner'
]
