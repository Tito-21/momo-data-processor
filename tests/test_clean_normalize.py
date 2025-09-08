"""
AMAzing SMS - Data Cleaner Tests
Unit tests for data cleaning and normalization functionality
"""

import pytest
from datetime import datetime
from unittest.mock import patch

from etl.clean_normalize import DataCleaner
from etl.config import Config

class TestDataCleaner:
    """Test cases for DataCleaner class"""
    
    @pytest.fixture
    def config(self):
        """Create a test configuration"""
        return Config()
    
    @pytest.fixture
    def cleaner(self, config):
        """Create a cleaner instance"""
        return DataCleaner(config)
    
    @pytest.fixture
    def sample_records(self):
        """Sample records for testing"""
        return [
            {
                'id': 'msg001',
                'sender': '+1-234-567-8900',
                'recipient': '0987654321',
                'content': '  Payment received: $100.00  ',
                'timestamp': '2024-01-01T10:00:00Z',
                'type': 'SMS'
            },
            {
                'id': 'msg002',
                'sender': '+1234567890',
                'recipient': '+0987654321',
                'content': 'Your balance is $250.00',
                'timestamp': '2024-01-01 11:00:00',
                'type': 'sms'
            },
            {
                'id': '',  # Invalid ID
                'sender': 'invalid-phone',
                'recipient': '+0987654321',
                'content': 'Test message',
                'timestamp': 'invalid-timestamp',
                'type': 'unknown'
            }
        ]
    
    def test_cleaner_initialization(self, cleaner):
        """Test cleaner initialization"""
        assert cleaner.config is not None
        assert cleaner.phone_pattern is not None
        assert cleaner.email_pattern is not None
    
    def test_clean_records_success(self, cleaner, sample_records):
        """Test successful record cleaning"""
        cleaned_records = cleaner.clean_records(sample_records)
        
        # Should have 2 valid records (one with invalid ID should be filtered out)
        assert len(cleaned_records) == 2
        
        # Check first record
        record1 = cleaned_records[0]
        assert record1['id'] == 'msg001'
        assert record1['sender'] == '+1234567890'  # Normalized
        assert record1['recipient'] == '+10987654321'  # Added country code
        assert record1['content'] == 'Payment received: $100.00'  # Trimmed
        assert record1['type'] == 'sms'  # Lowercased
        assert 'processed_at' in record1
    
    def test_clean_records_with_errors(self, cleaner):
        """Test record cleaning with errors"""
        invalid_records = [
            {'id': '', 'sender': 'test', 'recipient': 'test', 'content': 'test'},
            {'id': 'valid', 'sender': '+1234567890', 'recipient': '+0987654321', 'content': 'test'}
        ]
        
        cleaned_records = cleaner.clean_records(invalid_records)
        assert len(cleaned_records) == 1  # Only valid record should remain
    
    def test_clean_id_valid(self, cleaner):
        """Test ID cleaning with valid input"""
        assert cleaner._clean_id('msg001') == 'msg001'
        assert cleaner._clean_id('  msg002  ') == 'msg002'
        assert cleaner._clean_id('msg-003') == 'msg-003'
        assert cleaner._clean_id('msg_004') == 'msg_004'
    
    def test_clean_id_invalid(self, cleaner):
        """Test ID cleaning with invalid input"""
        assert cleaner._clean_id('') == ''
        assert cleaner._clean_id('  ') == ''
        assert cleaner._clean_id('msg@001') == 'msg001'  # Special chars removed
    
    def test_clean_phone_number_valid(self, cleaner):
        """Test phone number cleaning with valid input"""
        assert cleaner._clean_phone_number('+1234567890') == '+1234567890'
        assert cleaner._clean_phone_number('1234567890') == '+11234567890'
        assert cleaner._clean_phone_number('+1-234-567-8900') == '+12345678900'
        assert cleaner._clean_phone_number('(123) 456-7890') == '+1234567890'
    
    def test_clean_phone_number_invalid(self, cleaner):
        """Test phone number cleaning with invalid input"""
        assert cleaner._clean_phone_number('') == ''
        assert cleaner._clean_phone_number('invalid') == ''
        assert cleaner._clean_phone_number('123') == ''  # Too short
    
    def test_clean_content(self, cleaner):
        """Test content cleaning"""
        # Test whitespace trimming
        assert cleaner._clean_content('  Hello World  ') == 'Hello World'
        
        # Test excessive whitespace removal
        assert cleaner._clean_content('Hello    World') == 'Hello World'
        
        # Test control character removal
        content_with_control = 'Hello\x00World\x01Test'
        cleaned = cleaner._clean_content(content_with_control)
        assert '\x00' not in cleaned
        assert '\x01' not in cleaned
        
        # Test line ending normalization
        content_with_crlf = 'Hello\r\nWorld\rTest'
        cleaned = cleaner._clean_content(content_with_crlf)
        assert '\r\n' not in cleaned
        assert '\r' not in cleaned
        assert '\n' in cleaned
    
    def test_clean_timestamp_valid(self, cleaner):
        """Test timestamp cleaning with valid formats"""
        # ISO format
        assert cleaner._clean_timestamp('2024-01-01T10:00:00Z') == '2024-01-01T10:00:00+00:00'
        
        # Space format
        assert cleaner._clean_timestamp('2024-01-01 10:00:00') == '2024-01-01T10:00:00'
        
        # With microseconds
        assert cleaner._clean_timestamp('2024-01-01T10:00:00.123Z') == '2024-01-01T10:00:00.123000+00:00'
    
    def test_clean_timestamp_invalid(self, cleaner):
        """Test timestamp cleaning with invalid input"""
        assert cleaner._clean_timestamp('') == ''
        assert cleaner._clean_timestamp('invalid-timestamp') == ''
        assert cleaner._clean_timestamp('not-a-date') == ''
    
    def test_clean_message_type(self, cleaner):
        """Test message type cleaning"""
        assert cleaner._clean_message_type('SMS') == 'sms'
        assert cleaner._clean_message_type('  MMS  ') == 'mms'
        assert cleaner._clean_message_type('Notification') == 'notification'
        assert cleaner._clean_message_type('unknown') == 'unknown'
        assert cleaner._clean_message_type('invalid') == 'unknown'
        assert cleaner._clean_message_type('') == 'unknown'
    
    def test_clean_record_complete(self, cleaner):
        """Test complete record cleaning"""
        record = {
            'id': '  msg001  ',
            'sender': '+1-234-567-8900',
            'recipient': '0987654321',
            'content': '  Payment received: $100.00  ',
            'timestamp': '2024-01-01T10:00:00Z',
            'type': 'SMS'
        }
        
        cleaned = cleaner.clean_record(record)
        
        assert cleaned is not None
        assert cleaned['id'] == 'msg001'
        assert cleaned['sender'] == '+12345678900'
        assert cleaned['recipient'] == '+10987654321'
        assert cleaned['content'] == 'Payment received: $100.00'
        assert cleaned['timestamp'] == '2024-01-01T10:00:00+00:00'
        assert cleaned['type'] == 'sms'
        assert 'processed_at' in cleaned
        assert 'original_record' in cleaned
    
    def test_clean_record_invalid_id(self, cleaner):
        """Test record cleaning with invalid ID"""
        record = {
            'id': '',  # Invalid ID
            'sender': '+1234567890',
            'recipient': '+0987654321',
            'content': 'Test message',
            'timestamp': '2024-01-01T10:00:00Z',
            'type': 'sms'
        }
        
        cleaned = cleaner.clean_record(record)
        assert cleaned is None
    
    def test_clean_record_none(self, cleaner):
        """Test record cleaning with None input"""
        cleaned = cleaner.clean_record(None)
        assert cleaned is None
    
    @patch('etl.clean_normalize.datetime')
    def test_add_to_dead_letter(self, mock_datetime, cleaner):
        """Test adding failed record to dead letter queue"""
        mock_datetime.now.return_value.strftime.return_value = '20240101_120000'
        mock_datetime.utcnow.return_value.isoformat.return_value = '2024-01-01T12:00:00'
        
        record = {'id': 'test', 'content': 'test'}
        error = 'Test error'
        
        # Mock file operations
        with patch('builtins.open', mock_open()) as mock_file:
            cleaner._add_to_dead_letter(record, error)
            
            # Verify file was opened for writing
            mock_file.assert_called_once()
            
            # Verify JSON was written
            written_content = ''.join(call.args[0] for call in mock_file().write.call_args_list)
            assert 'test' in written_content
            assert 'Test error' in written_content
