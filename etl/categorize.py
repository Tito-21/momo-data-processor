"""
MoMo Data Processor - Message Categorizer
Categorizes MoMo messages based on content analysis
"""

import re
from typing import List, Dict, Any, Optional
from enum import Enum
import logging
from .config import Config

logger = logging.getLogger(__name__)

class MessageCategory(Enum):
    """Enumeration of message categories"""
    PAYMENT = "payment"
    TRANSFER = "transfer"
    BALANCE = "balance"
    NOTIFICATION = "notification"
    MARKETING = "marketing"
    SUPPORT = "support"
    UNKNOWN = "unknown"

class MessageCategorizer:
    """Categorizes SMS messages based on content patterns"""
    
    def __init__(self, config: Config):
        self.config = config
        self.category_patterns = self._initialize_patterns()
        self.confidence_threshold = 0.7
    
    def _initialize_patterns(self) -> Dict[MessageCategory, List[Dict[str, Any]]]:
        """Initialize regex patterns for each category"""
        return {
            MessageCategory.PAYMENT: [
                {
                    'pattern': r'(?i)(payment|paid|charge|billing|invoice|receipt)',
                    'weight': 1.0
                },
                {
                    'pattern': r'(?i)(amount|dollar|\$|USD|money)',
                    'weight': 0.8
                },
                {
                    'pattern': r'(?i)(transaction|purchase|buy|order)',
                    'weight': 0.9
                }
            ],
            MessageCategory.TRANSFER: [
                {
                    'pattern': r'(?i)(transfer|send|sent|received|deposit|withdrawal)',
                    'weight': 1.0
                },
                {
                    'pattern': r'(?i)(account|bank|wallet)',
                    'weight': 0.7
                },
                {
                    'pattern': r'(?i)(to|from|recipient|sender)',
                    'weight': 0.6
                }
            ],
            MessageCategory.BALANCE: [
                {
                    'pattern': r'(?i)(balance|available|remaining|current)',
                    'weight': 1.0
                },
                {
                    'pattern': r'(?i)(account|statement|summary)',
                    'weight': 0.8
                },
                {
                    'pattern': r'(?i)(check|view|see)',
                    'weight': 0.5
                }
            ],
            MessageCategory.NOTIFICATION: [
                {
                    'pattern': r'(?i)(alert|notification|reminder|update)',
                    'weight': 1.0
                },
                {
                    'pattern': r'(?i)(important|urgent|attention)',
                    'weight': 0.8
                },
                {
                    'pattern': r'(?i)(system|service|maintenance)',
                    'weight': 0.7
                }
            ],
            MessageCategory.MARKETING: [
                {
                    'pattern': r'(?i)(offer|promotion|deal|discount|sale)',
                    'weight': 1.0
                },
                {
                    'pattern': r'(?i)(subscribe|unsubscribe|opt)',
                    'weight': 0.9
                },
                {
                    'pattern': r'(?i)(advertisement|ad|marketing)',
                    'weight': 1.0
                }
            ],
            MessageCategory.SUPPORT: [
                {
                    'pattern': r'(?i)(help|support|assistance|contact)',
                    'weight': 1.0
                },
                {
                    'pattern': r'(?i)(problem|issue|error|trouble)',
                    'weight': 0.9
                },
                {
                    'pattern': r'(?i)(customer|service|representative)',
                    'weight': 0.8
                }
            ]
        }
    
    def categorize_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Categorize a list of SMS records
        
        Args:
            records: List of cleaned SMS records
            
        Returns:
            List of records with added category information
        """
        categorized_records = []
        
        for record in records:
            try:
                categorized_record = self.categorize_record(record)
                categorized_records.append(categorized_record)
            except Exception as e:
                logger.error(f"Error categorizing record {record.get('id', 'unknown')}: {e}")
                # Add category as unknown if categorization fails
                record['category'] = MessageCategory.UNKNOWN.value
                record['category_confidence'] = 0.0
                categorized_records.append(record)
        
        logger.info(f"Categorized {len(categorized_records)} records")
        return categorized_records
    
    def categorize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Categorize a single SMS record
        
        Args:
            record: Cleaned SMS record
            
        Returns:
            Record with added category information
        """
        content = record.get('content', '')
        sender = record.get('sender', '')
        
        # Combine content and sender for analysis
        text_to_analyze = f"{content} {sender}".strip()
        
        if not text_to_analyze:
            record['category'] = MessageCategory.UNKNOWN.value
            record['category_confidence'] = 0.0
            return record
        
        # Calculate confidence scores for each category
        category_scores = {}
        
        for category, patterns in self.category_patterns.items():
            score = self._calculate_category_score(text_to_analyze, patterns)
            category_scores[category] = score
        
        # Find the best category
        best_category = max(category_scores, key=category_scores.get)
        best_score = category_scores[best_category]
        
        # Set category and confidence
        record['category'] = best_category.value
        record['category_confidence'] = round(best_score, 3)
        
        # Add detailed category breakdown for debugging
        record['category_breakdown'] = {
            cat.value: round(score, 3) 
            for cat, score in category_scores.items()
        }
        
        return record
    
    def _calculate_category_score(self, text: str, patterns: List[Dict[str, Any]]) -> float:
        """
        Calculate confidence score for a category based on patterns
        
        Args:
            text: Text to analyze
            pattern: List of pattern dictionaries with 'pattern' and 'weight' keys
            
        Returns:
            Confidence score between 0 and 1
        """
        if not text or not patterns:
            return 0.0
        
        total_weight = 0.0
        matched_weight = 0.0
        
        for pattern_info in patterns:
            pattern = pattern_info['pattern']
            weight = pattern_info['weight']
            
            total_weight += weight
            
            if re.search(pattern, text):
                matched_weight += weight
        
        if total_weight == 0:
            return 0.0
        
        # Normalize score to 0-1 range
        score = matched_weight / total_weight
        
        # Apply additional heuristics
        score = self._apply_heuristics(text, score)
        
        return min(score, 1.0)
    
    def _apply_heuristics(self, text: str, base_score: float) -> float:
        """
        Apply additional heuristics to improve categorization
        
        Args:
            text: Text to analyze
            base_score: Base confidence score
            
        Returns:
            Adjusted confidence score
        """
        # Length heuristic: longer messages might be more informative
        if len(text) > 100:
            base_score *= 1.1
        
        # Multiple keyword matches boost confidence
        keyword_count = len(re.findall(r'\b(?:payment|transfer|balance|notification|marketing|support)\b', text, re.IGNORECASE))
        if keyword_count > 1:
            base_score *= 1.2
        
        # Presence of numbers (amounts, account numbers) can indicate financial messages
        if re.search(r'\d+', text):
            base_score *= 1.05
        
        return min(base_score, 1.0)
    
    def get_category_statistics(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Get statistics about message categories
        
        Args:
            records: List of categorized records
            
        Returns:
            Dictionary with category statistics
        """
        if not records:
            return {}
        
        category_counts = {}
        total_confidence = 0.0
        high_confidence_count = 0
        
        for record in records:
            category = record.get('category', 'unknown')
            confidence = record.get('category_confidence', 0.0)
            
            category_counts[category] = category_counts.get(category, 0) + 1
            total_confidence += confidence
            
            if confidence >= self.confidence_threshold:
                high_confidence_count += 1
        
        total_records = len(records)
        avg_confidence = total_confidence / total_records if total_records > 0 else 0.0
        
        return {
            'total_records': total_records,
            'category_distribution': category_counts,
            'average_confidence': round(avg_confidence, 3),
            'high_confidence_count': high_confidence_count,
            'high_confidence_percentage': round((high_confidence_count / total_records) * 100, 2) if total_records > 0 else 0
        }
