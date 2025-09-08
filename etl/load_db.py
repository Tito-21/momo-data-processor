"""
MoMo Data Processor - Database Loader
Handles loading processed data into the database
"""

import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime
import json
from .config import Config

logger = logging.getLogger(__name__)

class DatabaseLoader:
    """Handles loading processed SMS data into the database"""
    
    def __init__(self, config: Config):
        self.config = config
        self.db_path = Path(config.database_url.replace("sqlite:///", ""))
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize database tables if they don't exist"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create SMS messages table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS sms_messages (
                        id TEXT PRIMARY KEY,
                        sender TEXT NOT NULL,
                        recipient TEXT NOT NULL,
                        content TEXT NOT NULL,
                        timestamp TEXT,
                        type TEXT,
                        category TEXT,
                        category_confidence REAL,
                        category_breakdown TEXT,
                        processed_at TEXT NOT NULL,
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create processing logs table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS processing_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        batch_id TEXT,
                        total_records INTEGER,
                        successful_records INTEGER,
                        failed_records INTEGER,
                        processing_time REAL,
                        started_at TEXT,
                        completed_at TEXT,
                        status TEXT
                    )
                """)
                
                # Create category statistics table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS category_stats (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        category TEXT NOT NULL,
                        count INTEGER NOT NULL,
                        avg_confidence REAL,
                        date TEXT NOT NULL,
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create indexes for better performance
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_sms_category ON sms_messages(category)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_sms_timestamp ON sms_messages(timestamp)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_sms_sender ON sms_messages(sender)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_sms_processed_at ON sms_messages(processed_at)")
                
                conn.commit()
                logger.info("Database initialized successfully")
                
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def load_records(self, records: List[Dict[str, Any]], batch_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Load a batch of records into the database
        
        Args:
            records: List of processed SMS records
            batch_id: Optional batch identifier for tracking
            
        Returns:
            Dictionary with loading statistics
        """
        if not records:
            return {
                'total_records': 0,
                'successful_records': 0,
                'failed_records': 0,
                'errors': []
            }
        
        batch_id = batch_id or f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        start_time = datetime.now()
        successful_records = 0
        failed_records = 0
        errors = []
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                for record in records:
                    try:
                        self._insert_record(cursor, record)
                        successful_records += 1
                    except Exception as e:
                        failed_records += 1
                        error_msg = f"Failed to insert record {record.get('id', 'unknown')}: {e}"
                        errors.append(error_msg)
                        logger.error(error_msg)
                
                # Log processing statistics
                self._log_processing_stats(
                    cursor, batch_id, len(records), successful_records, 
                    failed_records, start_time, datetime.now()
                )
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
        
        processing_time = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"Loaded {successful_records}/{len(records)} records in {processing_time:.2f}s")
        
        return {
            'batch_id': batch_id,
            'total_records': len(records),
            'successful_records': successful_records,
            'failed_records': failed_records,
            'processing_time': processing_time,
            'errors': errors
        }
    
    def _insert_record(self, cursor: sqlite3.Cursor, record: Dict[str, Any]):
        """Insert a single record into the database"""
        cursor.execute("""
            INSERT OR REPLACE INTO sms_messages (
                id, sender, recipient, content, timestamp, type,
                category, category_confidence, category_breakdown,
                processed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            record.get('id', ''),
            record.get('sender', ''),
            record.get('recipient', ''),
            record.get('content', ''),
            record.get('timestamp', ''),
            record.get('type', 'unknown'),
            record.get('category', 'unknown'),
            record.get('category_confidence', 0.0),
            json.dumps(record.get('category_breakdown', {})),
            record.get('processed_at', datetime.utcnow().isoformat())
        ))
    
    def _log_processing_stats(self, cursor: sqlite3.Cursor, batch_id: str, 
                            total_records: int, successful_records: int, 
                            failed_records: int, start_time: datetime, 
                            end_time: datetime):
        """Log processing statistics to the database"""
        processing_time = (end_time - start_time).total_seconds()
        status = "completed" if failed_records == 0 else "completed_with_errors"
        
        cursor.execute("""
            INSERT INTO processing_logs (
                batch_id, total_records, successful_records, failed_records,
                processing_time, started_at, completed_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            batch_id, total_records, successful_records, failed_records,
            processing_time, start_time.isoformat(), end_time.isoformat(), status
        ))
    
    def update_category_statistics(self, records: List[Dict[str, Any]]):
        """Update category statistics based on processed records"""
        if not records:
            return
        
        category_counts = {}
        category_confidences = {}
        
        for record in records:
            category = record.get('category', 'unknown')
            confidence = record.get('category_confidence', 0.0)
            
            if category not in category_counts:
                category_counts[category] = 0
                category_confidences[category] = []
            
            category_counts[category] += 1
            category_confidences[category].append(confidence)
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                current_date = datetime.now().strftime('%Y-%m-%d')
                
                for category, count in category_counts.items():
                    avg_confidence = sum(category_confidences[category]) / len(category_confidences[category])
                    
                    cursor.execute("""
                        INSERT INTO category_stats (category, count, avg_confidence, date)
                        VALUES (?, ?, ?, ?)
                    """, (category, count, avg_confidence, current_date))
                
                conn.commit()
                logger.info("Category statistics updated successfully")
                
        except Exception as e:
            logger.error(f"Failed to update category statistics: {e}")
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get total message count
                cursor.execute("SELECT COUNT(*) FROM sms_messages")
                total_messages = cursor.fetchone()[0]
                
                # Get category distribution
                cursor.execute("""
                    SELECT category, COUNT(*) as count 
                    FROM sms_messages 
                    GROUP BY category 
                    ORDER BY count DESC
                """)
                category_distribution = dict(cursor.fetchall())
                
                # Get recent processing stats
                cursor.execute("""
                    SELECT COUNT(*) as total_batches,
                           SUM(successful_records) as total_successful,
                           SUM(failed_records) as total_failed
                    FROM processing_logs
                    WHERE date(started_at) = date('now')
                """)
                today_stats = cursor.fetchone()
                
                return {
                    'total_messages': total_messages,
                    'category_distribution': category_distribution,
                    'today_batches': today_stats[0] or 0,
                    'today_successful': today_stats[1] or 0,
                    'today_failed': today_stats[2] or 0
                }
                
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return {}
    
    def cleanup_old_data(self, days_to_keep: int = 30):
        """Clean up old data to manage database size"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cutoff_date = datetime.now().replace(day=datetime.now().day - days_to_keep)
                
                # Delete old messages
                cursor.execute("""
                    DELETE FROM sms_messages 
                    WHERE date(processed_at) < date(?)
                """, (cutoff_date.isoformat(),))
                
                deleted_messages = cursor.rowcount
                
                # Delete old processing logs
                cursor.execute("""
                    DELETE FROM processing_logs 
                    WHERE date(started_at) < date(?)
                """, (cutoff_date.isoformat(),))
                
                deleted_logs = cursor.rowcount
                
                conn.commit()
                
                logger.info(f"Cleaned up {deleted_messages} old messages and {deleted_logs} old logs")
                
        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")
