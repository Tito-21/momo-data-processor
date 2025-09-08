"""
MoMo Data Processor - Message Categorizer Tests
Unit tests for message categorization functionality
"""

import pytest
from unittest.mock import patch

from etl.categorize import MessageCategorizer, MessageCategory
from etl.config import Config

class TestMessageCategorizer:
    """Test cases for MessageCategorizer class"""
    
    @pytest.fixture
    def config(self):
        """Create a test configuration"""
        return Config()
    
    @pytest.fixture
    def categorizer(self, config):
        """Create a categorizer instance"""
        return MessageCategorizer(config)
    
    @pytest.fixture
    def sample_records(self):
        """Sample records for testing"""
        return [
            {
                'id': 'msg001',
                'sender': '+1234567890',
                'recipient': '+0987654321',
                'content': 'Payment received: $100.00 from John Doe',
                'timestamp': '2024-01-01T10:00:00Z',
                'type': 'sms'
            },
            {
                'id': 'msg002',
                'sender': '+1234567890',
                'recipient': '+0987654321',
                'content': 'Your account balance is $250.00',
                'timestamp': '2024-01-01T11:00:00Z',
                'type': 'sms'
            },
            {
                'id': 'msg003',
                'sender': '+1234567890',
                'recipient': '+0987654321',
                'content': 'Transfer $50.00 to Jane Smith completed',
                'timestamp': '2024-01-01T12:00:00Z',
                'type': 'sms'
            },
            {
                'id': 'msg004',
                'sender': '+1234567890',
                'recipient': '+0987654321',
                'content': 'Important: System maintenance tonight at 2 AM',
                'timestamp': '2024-01-01T13:00:00Z',
                'type': 'sms'
            },
            {
                'id': 'msg005',
                'sender': '+1234567890',
                'recipient': '+0987654321',
                'content': 'Special offer: 50% off all services!',
                'timestamp': '2024-01-01T14:00:00Z',
                'type': 'sms'
            },
            {
                'id': 'msg006',
                'sender': '+1234567890',
                'recipient': '+0987654321',
                'content': 'Need help? Contact our support team',
                'timestamp': '2024-01-01T15:00:00Z',
                'type': 'sms'
            }
        ]
    
    def test_categorizer_initialization(self, categorizer):
        """Test categorizer initialization"""
        assert categorizer.config is not None
        assert categorizer.category_patterns is not None
        assert categorizer.confidence_threshold == 0.7
        assert len(categorizer.category_patterns) == 6  # All categories
    
    def test_categorize_records_success(self, categorizer, sample_records):
        """Test successful record categorization"""
        categorized_records = categorizer.categorize_records(sample_records)
        
        assert len(categorized_records) == len(sample_records)
        
        # Check that all records have category and confidence
        for record in categorized_records:
            assert 'category' in record
            assert 'category_confidence' in record
            assert 'category_breakdown' in record
            assert record['category'] in [cat.value for cat in MessageCategory]
            assert 0.0 <= record['category_confidence'] <= 1.0
    
    def test_categorize_records_with_errors(self, categorizer):
        """Test categorization with errors"""
        invalid_records = [
            {'id': 'test1', 'content': 'Test message'},  # Missing required fields
            None,  # None record
            {'id': 'test2', 'content': 'Another test'}  # Missing required fields
        ]
        
        categorized_records = categorizer.categorize_records(invalid_records)
        
        # Should handle errors gracefully
        assert len(categorized_records) == 3
        for record in categorized_records:
            assert record['category'] == MessageCategory.UNKNOWN.value
            assert record['category_confidence'] == 0.0
    
    def test_categorize_record_payment(self, categorizer):
        """Test payment message categorization"""
        record = {
            'id': 'msg001',
            'sender': '+1234567890',
            'recipient': '+0987654321',
            'content': 'Payment received: $100.00 from John Doe',
            'timestamp': '2024-01-01T10:00:00Z',
            'type': 'sms'
        }
        
        categorized = categorizer.categorize_record(record)
        
        assert categorized['category'] == MessageCategory.PAYMENT.value
        assert categorized['category_confidence'] > 0.5
    
    def test_categorize_record_balance(self, categorizer):
        """Test balance message categorization"""
        record = {
            'id': 'msg002',
            'sender': '+1234567890',
            'recipient': '+0987654321',
            'content': 'Your account balance is $250.00',
            'timestamp': '2024-01-01T11:00:00Z',
            'type': 'sms'
        }
        
        categorized = categorizer.categorize_record(record)
        
        assert categorized['category'] == MessageCategory.BALANCE.value
        assert categorized['category_confidence'] > 0.5
    
    def test_categorize_record_transfer(self, categorizer):
        """Test transfer message categorization"""
        record = {
            'id': 'msg003',
            'sender': '+1234567890',
            'recipient': '+0987654321',
            'content': 'Transfer $50.00 to Jane Smith completed',
            'timestamp': '2024-01-01T12:00:00Z',
            'type': 'sms'
        }
        
        categorized = categorizer.categorize_record(record)
        
        assert categorized['category'] == MessageCategory.TRANSFER.value
        assert categorized['category_confidence'] > 0.5
    
    def test_categorize_record_notification(self, categorizer):
        """Test notification message categorization"""
        record = {
            'id': 'msg004',
            'sender': '+1234567890',
            'recipient': '+0987654321',
            'content': 'Important: System maintenance tonight at 2 AM',
            'timestamp': '2024-01-01T13:00:00Z',
            'type': 'sms'
        }
        
        categorized = categorizer.categorize_record(record)
        
        assert categorized['category'] == MessageCategory.NOTIFICATION.value
        assert categorized['category_confidence'] > 0.5
    
    def test_categorize_record_marketing(self, categorizer):
        """Test marketing message categorization"""
        record = {
            'id': 'msg005',
            'sender': '+1234567890',
            'recipient': '+0987654321',
            'content': 'Special offer: 50% off all services!',
            'timestamp': '2024-01-01T14:00:00Z',
            'type': 'sms'
        }
        
        categorized = categorizer.categorize_record(record)
        
        assert categorized['category'] == MessageCategory.MARKETING.value
        assert categorized['category_confidence'] > 0.5
    
    def test_categorize_record_support(self, categorizer):
        """Test support message categorization"""
        record = {
            'id': 'msg006',
            'sender': '+1234567890',
            'recipient': '+0987654321',
            'content': 'Need help? Contact our support team',
            'timestamp': '2024-01-01T15:00:00Z',
            'type': 'sms'
        }
        
        categorized = categorizer.categorize_record(record)
        
        assert categorized['category'] == MessageCategory.SUPPORT.value
        assert categorized['category_confidence'] > 0.5
    
    def test_categorize_record_unknown(self, categorizer):
        """Test unknown message categorization"""
        record = {
            'id': 'msg007',
            'sender': '+1234567890',
            'recipient': '+0987654321',
            'content': 'Random text with no clear category',
            'timestamp': '2024-01-01T16:00:00Z',
            'type': 'sms'
        }
        
        categorized = categorizer.categorize_record(record)
        
        assert categorized['category'] == MessageCategory.UNKNOWN.value
        assert categorized['category_confidence'] >= 0.0
    
    def test_categorize_record_empty_content(self, categorizer):
        """Test categorization with empty content"""
        record = {
            'id': 'msg008',
            'sender': '+1234567890',
            'recipient': '+0987654321',
            'content': '',
            'timestamp': '2024-01-01T17:00:00Z',
            'type': 'sms'
        }
        
        categorized = categorizer.categorize_record(record)
        
        assert categorized['category'] == MessageCategory.UNKNOWN.value
        assert categorized['category_confidence'] == 0.0
    
    def test_calculate_category_score(self, categorizer):
        """Test category score calculation"""
        patterns = [
            {'pattern': r'(?i)(payment|paid)', 'weight': 1.0},
            {'pattern': r'(?i)(amount|\$)', 'weight': 0.8}
        ]
        
        # Test with matching text
        score = categorizer._calculate_category_score('Payment received: $100', patterns)
        assert score > 0.8
        
        # Test with partial match
        score = categorizer._calculate_category_score('Amount: $50', patterns)
        assert score > 0.0
        
        # Test with no match
        score = categorizer._calculate_category_score('Hello world', patterns)
        assert score == 0.0
    
    def test_apply_heuristics(self, categorizer):
        """Test heuristic application"""
        # Test length heuristic
        long_text = 'This is a very long message with many words that should boost the confidence score'
        base_score = 0.5
        adjusted_score = categorizer._apply_heuristics(long_text, base_score)
        assert adjusted_score > base_score
        
        # Test keyword count heuristic
        multi_keyword_text = 'Payment received and amount is $100'
        base_score = 0.5
        adjusted_score = categorizer._apply_heuristics(multi_keyword_text, base_score)
        assert adjusted_score > base_score
        
        # Test number presence heuristic
        number_text = 'Payment of $100 received'
        base_score = 0.5
        adjusted_score = categorizer._apply_heuristics(number_text, base_score)
        assert adjusted_score > base_score
    
    def test_get_category_statistics(self, categorizer, sample_records):
        """Test category statistics generation"""
        categorized_records = categorizer.categorize_records(sample_records)
        stats = categorizer.get_category_statistics(categorized_records)
        
        assert 'total_records' in stats
        assert 'category_distribution' in stats
        assert 'average_confidence' in stats
        assert 'high_confidence_count' in stats
        assert 'high_confidence_percentage' in stats
        
        assert stats['total_records'] == len(categorized_records)
        assert stats['average_confidence'] >= 0.0
        assert stats['high_confidence_count'] >= 0
        assert stats['high_confidence_percentage'] >= 0.0
    
    def test_get_category_statistics_empty(self, categorizer):
        """Test category statistics with empty records"""
        stats = categorizer.get_category_statistics([])
        assert stats == {}
    
    def test_initialize_patterns(self, categorizer):
        """Test pattern initialization"""
        patterns = categorizer._initialize_patterns()
        
        # Check that all categories have patterns
        assert MessageCategory.PAYMENT in patterns
        assert MessageCategory.TRANSFER in patterns
        assert MessageCategory.BALANCE in patterns
        assert MessageCategory.NOTIFICATION in patterns
        assert MessageCategory.MARKETING in patterns
        assert MessageCategory.SUPPORT in patterns
        
        # Check pattern structure
        for category, pattern_list in patterns.items():
            assert isinstance(pattern_list, list)
            for pattern_info in pattern_list:
                assert 'pattern' in pattern_info
                assert 'weight' in pattern_info
                assert isinstance(pattern_info['weight'], (int, float))
                assert pattern_info['weight'] > 0
