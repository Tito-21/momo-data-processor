"""
AMAzing SMS - Database Manager
Database operations and connection management
"""

import sqlite3
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime, date
import logging
from contextlib import contextmanager

from .schemas import SMSMessage, CategoryStats, ProcessingStats

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Database manager for SMS data operations"""
    
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self._ensure_database_exists()
    
    def _ensure_database_exists(self):
        """Ensure database file exists and is properly initialized"""
        if not self.db_path.exists():
            logger.warning(f"Database file not found: {self.db_path}")
            # Database will be created when first accessed
    
    @contextmanager
    def get_connection(self):
        """Get database connection with proper error handling"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable column access by name
            yield conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get overall database statistics"""
        try:
            with self.get_connection() as conn:
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
                
                # Get latest processing time
                cursor.execute("""
                    SELECT completed_at, status
                    FROM processing_logs
                    ORDER BY completed_at DESC
                    LIMIT 1
                """)
                latest_processing = cursor.fetchone()
                
                return {
                    'total_messages': total_messages,
                    'category_distribution': category_distribution,
                    'today_batches': today_stats[0] or 0,
                    'today_successful': today_stats[1] or 0,
                    'today_failed': today_stats[2] or 0,
                    'latest_processing': {
                        'completed_at': latest_processing[0] if latest_processing else None,
                        'status': latest_processing[1] if latest_processing else None
                    }
                }
                
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {}
    
    def get_messages(
        self,
        skip: int = 0,
        limit: int = 100,
        category: Optional[str] = None,
        sender: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> List[SMSMessage]:
        """Get SMS messages with optional filtering"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Build query with filters
                query = "SELECT * FROM sms_messages WHERE 1=1"
                params = []
                
                if category:
                    query += " AND category = ?"
                    params.append(category)
                
                if sender:
                    query += " AND sender LIKE ?"
                    params.append(f"%{sender}%")
                
                if start_date:
                    query += " AND date(timestamp) >= ?"
                    params.append(start_date.isoformat())
                
                if end_date:
                    query += " AND date(timestamp) <= ?"
                    params.append(end_date.isoformat())
                
                query += " ORDER BY processed_at DESC LIMIT ? OFFSET ?"
                params.extend([limit, skip])
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                messages = []
                for row in rows:
                    message = SMSMessage(
                        id=row['id'],
                        sender=row['sender'],
                        recipient=row['recipient'],
                        content=row['content'],
                        timestamp=row['timestamp'],
                        type=row['type'],
                        category=row['category'],
                        category_confidence=row['category_confidence'],
                        category_breakdown=row['category_breakdown'],
                        processed_at=row['processed_at'],
                        created_at=row['created_at']
                    )
                    messages.append(message)
                
                return messages
                
        except Exception as e:
            logger.error(f"Error getting messages: {e}")
            return []
    
    def get_message_by_id(self, message_id: str) -> Optional[SMSMessage]:
        """Get a specific message by ID"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("SELECT * FROM sms_messages WHERE id = ?", (message_id,))
                row = cursor.fetchone()
                
                if not row:
                    return None
                
                return SMSMessage(
                    id=row['id'],
                    sender=row['sender'],
                    recipient=row['recipient'],
                    content=row['content'],
                    timestamp=row['timestamp'],
                    type=row['type'],
                    category=row['category'],
                    category_confidence=row['category_confidence'],
                    category_breakdown=row['category_breakdown'],
                    processed_at=row['processed_at'],
                    created_at=row['created_at']
                )
                
        except Exception as e:
            logger.error(f"Error getting message {message_id}: {e}")
            return None
    
    def get_category_statistics(
        self,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None
    ) -> List[CategoryStats]:
        """Get category statistics"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                query = """
                    SELECT category, COUNT(*) as count, AVG(category_confidence) as avg_confidence
                    FROM sms_messages
                    WHERE 1=1
                """
                params = []
                
                if date_from:
                    query += " AND date(timestamp) >= ?"
                    params.append(date_from.isoformat())
                
                if date_to:
                    query += " AND date(timestamp) <= ?"
                    params.append(date_to.isoformat())
                
                query += " GROUP BY category ORDER BY count DESC"
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                stats = []
                for row in rows:
                    stat = CategoryStats(
                        category=row['category'],
                        count=row['count'],
                        avg_confidence=round(row['avg_confidence'] or 0, 3)
                    )
                    stats.append(stat)
                
                return stats
                
        except Exception as e:
            logger.error(f"Error getting category statistics: {e}")
            return []
    
    def get_processing_logs(self, limit: int = 50) -> List[ProcessingStats]:
        """Get ETL processing logs"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT batch_id, total_records, successful_records, failed_records,
                           processing_time, started_at, completed_at, status
                    FROM processing_logs
                    ORDER BY started_at DESC
                    LIMIT ?
                """, (limit,))
                
                rows = cursor.fetchall()
                
                logs = []
                for row in rows:
                    log = ProcessingStats(
                        batch_id=row['batch_id'],
                        total_records=row['total_records'],
                        successful_records=row['successful_records'],
                        failed_records=row['failed_records'],
                        processing_time=row['processing_time'],
                        started_at=row['started_at'],
                        completed_at=row['completed_at'],
                        status=row['status']
                    )
                    logs.append(log)
                
                return logs
                
        except Exception as e:
            logger.error(f"Error getting processing logs: {e}")
            return []

def get_database() -> DatabaseManager:
    """Dependency to get database manager instance"""
    # This would typically use environment variables or config
    db_path = "data/db.sqlite3"
    return DatabaseManager(db_path)
