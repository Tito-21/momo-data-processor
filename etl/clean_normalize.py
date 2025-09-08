"""
AMAzing SMS - Data Cleaner and Normalizer
Handles data cleaning and normalization for SMS records
"""

import re
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
from .config import Config

logger = logging.getLogger(__name__)

class DataCleaner:
    """Data cleaning and normalization for SMS records"""
    
    def __init__(self, config: Config):
        self.config = config
        self.phone_pattern = re.compile(r'^\+?[1-9]\d{1,14}$')
        self.email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    
    def clean_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Clean and normalize a list of SMS records
        
        Args:
            records: List of raw SMS records
            
        Returns:
            List of cleaned and normalized records
        """
        cleaned_records = []
        
        for record in records:
            try:
                cleaned_record = self.clean_record(record)
                if cleaned_record:
                    cleaned_records.append(cleaned_record)
            except Exception as e:
                logger.error(f"Error cleaning record {record.get('id', 'unknown')}: {e}")
                # Add to dead letter queue
                self._add_to_dead_letter(record, str(e))
        
        logger.info(f"Cleaned {len(cleaned_records)} out of {len(records)} records")
        return cleaned_records
    
    def clean_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Clean and normalize a single SMS record
        
        Args:
            record: Raw SMS record
            
        Returns:
            Cleaned record or None if invalid
        """
        if not record:
            return None
        
        cleaned = {}
        
        # Clean and validate ID
        cleaned['id'] = self._clean_id(record.get('id', ''))
        if not cleaned['id']:
            logger.warning("Record missing valid ID, skipping")
            return None
        
        # Clean phone numbers
        cleaned['sender'] = self._clean_phone_number(record.get('sender', ''))
        cleaned['recipient'] = self._clean_phone_number(record.get('recipient', ''))
        
        # Clean and normalize content
        cleaned['content'] = self._clean_content(record.get('content', ''))
        
        # Clean and validate timestamp
        cleaned['timestamp'] = self._clean_timestamp(record.get('timestamp', ''))
        
        # Clean message type
        cleaned['type'] = self._clean_message_type(record.get('type', 'unknown'))
        
        # Add metadata
        cleaned['processed_at'] = datetime.utcnow().isoformat()
        cleaned['original_record'] = record
        
        return cleaned
    
    def _clean_id(self, record_id: str) -> str:
        """Clean and validate record ID"""
        if not record_id:
            return ""
        
        # Remove whitespace and convert to string
        cleaned = str(record_id).strip()
        
        # Remove any non-alphanumeric characters except hyphens and underscores
        cleaned = re.sub(r'[^a-zA-Z0-9\-_]', '', cleaned)
        
        return cleaned
    
    def _clean_phone_number(self, phone: str) -> str:
        """Clean and validate phone number"""
        if not phone:
            return ""
        
        # Remove all non-digit characters except +
        cleaned = re.sub(r'[^\d+]', '', str(phone))
        
        # Add country code if missing (assuming +1 for US)
        if cleaned and not cleaned.startswith('+'):
            if len(cleaned) == 10:
                cleaned = '+1' + cleaned
            elif len(cleaned) == 11 and cleaned.startswith('1'):
                cleaned = '+' + cleaned
        
        # Validate phone number format
        if cleaned and not self.phone_pattern.match(cleaned):
            logger.warning(f"Invalid phone number format: {phone}")
            return ""
        
        return cleaned
    
    def _clean_content(self, content: str) -> str:
        """Clean and normalize message content"""
        if not content:
            return ""
        
        # Convert to string and strip whitespace
        cleaned = str(content).strip()
        
        # Remove excessive whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Remove control characters except newlines and tabs
        cleaned = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', cleaned)
        
        # Normalize line endings
        cleaned = cleaned.replace('\r\n', '\n').replace('\r', '\n')
        
        return cleaned
    
    def _clean_timestamp(self, timestamp: str) -> str:
        """Clean and validate timestamp"""
        if not timestamp:
            return ""
        
        try:
            # Try to parse various timestamp formats
            timestamp_formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%dT%H:%M:%S.%fZ',
                '%Y-%m-%d %H:%M:%S.%f'
            ]
            
            for fmt in timestamp_formats:
                try:
                    dt = datetime.strptime(timestamp, fmt)
                    return dt.isoformat()
                except ValueError:
                    continue
            
            # If no format matches, try to parse as ISO format
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            return dt.isoformat()
            
        except Exception as e:
            logger.warning(f"Could not parse timestamp '{timestamp}': {e}")
            return ""
    
    def _clean_message_type(self, msg_type: str) -> str:
        """Clean and normalize message type"""
        if not msg_type:
            return "unknown"
        
        # Convert to lowercase and strip whitespace
        cleaned = str(msg_type).strip().lower()
        
        # Map common variations to standard types
        type_mapping = {
            'sms': 'sms',
            'text': 'sms',
            'message': 'sms',
            'mms': 'mms',
            'multimedia': 'mms',
            'notification': 'notification',
            'alert': 'notification',
            'unknown': 'unknown'
        }
        
        return type_mapping.get(cleaned, 'unknown')
    
    def _add_to_dead_letter(self, record: Dict[str, Any], error: str):
        """Add failed record to dead letter queue"""
        try:
            dead_letter_file = self.config.dead_letter_dir / f"failed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            import json
            dead_letter_record = {
                'original_record': record,
                'error': error,
                'failed_at': datetime.utcnow().isoformat()
            }
            
            with open(dead_letter_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(dead_letter_record) + '\n')
                
        except Exception as e:
            logger.error(f"Failed to add record to dead letter queue: {e}")
