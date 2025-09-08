"""
MoMo Data Processor - API Package
FastAPI-based REST API for MoMo data processing and retrieval
"""

__version__ = "1.0.0"
__author__ = "MoMo Data Processor Team"

# API modules
from .app import app
from .db import get_database, DatabaseManager
from .schemas import SMSMessage, CategoryStats, ProcessingStats

__all__ = [
    'app',
    'get_database',
    'DatabaseManager',
    'SMSMessage',
    'CategoryStats', 
    'ProcessingStats'
]
