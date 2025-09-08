"""
AMAzing SMS - XML Parser Tests
Unit tests for XML parsing functionality
"""

import pytest
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from unittest.mock import patch, mock_open

from etl.parse_xml import XMLParser
from etl.config import Config

class TestXMLParser:
    """Test cases for XMLParser class"""
    
    @pytest.fixture
    def config(self):
        """Create a test configuration"""
        return Config()
    
    @pytest.fixture
    def parser(self, config):
        """Create a parser instance"""
        return XMLParser(config)
    
    @pytest.fixture
    def sample_xml_content(self):
        """Sample XML content for testing"""
        return """<?xml version="1.0" encoding="UTF-8"?>
        <messages>
            <sms id="msg001" sender="+1234567890" recipient="+0987654321" timestamp="2024-01-01T10:00:00Z" type="sms">
                Payment received: $100.00 from John Doe
            </sms>
            <sms id="msg002" sender="+1234567890" recipient="+0987654321" timestamp="2024-01-01T11:00:00Z" type="sms">
                Your balance is $250.00
            </sms>
        </messages>"""
    
    def test_parser_initialization(self, parser):
        """Test parser initialization"""
        assert parser.config is not None
        assert parser.supported_formats == ['.xml']
    
    def test_parse_file_success(self, parser, sample_xml_content):
        """Test successful XML file parsing"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            f.write(sample_xml_content)
            f.flush()
            
            try:
                records = parser.parse_file(Path(f.name))
                
                assert len(records) == 2
                assert records[0]['id'] == 'msg001'
                assert records[0]['sender'] == '+1234567890'
                assert records[0]['recipient'] == '+0987654321'
                assert 'Payment received' in records[0]['content']
                
                assert records[1]['id'] == 'msg002'
                assert 'balance' in records[1]['content'].lower()
                
            finally:
                Path(f.name).unlink()
    
    def test_parse_file_not_found(self, parser):
        """Test parsing non-existent file"""
        records = parser.parse_file(Path("nonexistent.xml"))
        assert records == []
    
    def test_parse_file_unsupported_format(self, parser):
        """Test parsing unsupported file format"""
        with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as f:
            f.write("This is not XML")
            f.flush()
            
            try:
                records = parser.parse_file(Path(f.name))
                assert records == []
            finally:
                Path(f.name).unlink()
    
    def test_parse_file_invalid_xml(self, parser):
        """Test parsing invalid XML"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            f.write("This is not valid XML content")
            f.flush()
            
            try:
                records = parser.parse_file(Path(f.name))
                assert records == []
            finally:
                Path(f.name).unlink()
    
    def test_parse_directory_success(self, parser, sample_xml_content):
        """Test parsing directory with XML files"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create multiple XML files
            for i in range(3):
                xml_file = temp_path / f"test_{i}.xml"
                with open(xml_file, 'w') as f:
                    f.write(sample_xml_content)
            
            records = parser.parse_directory(temp_path)
            assert len(records) == 6  # 2 records per file * 3 files
    
    def test_parse_directory_not_found(self, parser):
        """Test parsing non-existent directory"""
        records = parser.parse_directory(Path("nonexistent_dir"))
        assert records == []
    
    def test_parse_directory_empty(self, parser):
        """Test parsing empty directory"""
        with tempfile.TemporaryDirectory() as temp_dir:
            records = parser.parse_directory(Path(temp_dir))
            assert records == []
    
    def test_extract_records(self, parser):
        """Test record extraction from XML root"""
        # Create a mock XML structure
        root = ET.Element('messages')
        
        sms1 = ET.SubElement(root, 'sms')
        sms1.set('id', 'test001')
        sms1.set('sender', '+1111111111')
        sms1.set('recipient', '+2222222222')
        sms1.set('timestamp', '2024-01-01T12:00:00Z')
        sms1.set('type', 'sms')
        sms1.text = 'Test message 1'
        
        sms2 = ET.SubElement(root, 'sms')
        sms2.set('id', 'test002')
        sms2.set('sender', '+3333333333')
        sms2.set('recipient', '+4444444444')
        sms2.set('timestamp', '2024-01-01T13:00:00Z')
        sms2.set('type', 'sms')
        sms2.text = 'Test message 2'
        
        records = parser._extract_records(root)
        
        assert len(records) == 2
        assert records[0]['id'] == 'test001'
        assert records[0]['content'] == 'Test message 1'
        assert records[1]['id'] == 'test002'
        assert records[1]['content'] == 'Test message 2'
    
    def test_validate_xml_structure_valid(self, parser, sample_xml_content):
        """Test XML structure validation with valid XML"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            f.write(sample_xml_content)
            f.flush()
            
            try:
                is_valid = parser.validate_xml_structure(Path(f.name))
                assert is_valid is True
            finally:
                Path(f.name).unlink()
    
    def test_validate_xml_structure_invalid(self, parser):
        """Test XML structure validation with invalid XML"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            f.write("This is not valid XML")
            f.flush()
            
            try:
                is_valid = parser.validate_xml_structure(Path(f.name))
                assert is_valid is False
            finally:
                Path(f.name).unlink()
    
    def test_validate_xml_structure_not_found(self, parser):
        """Test XML structure validation with non-existent file"""
        is_valid = parser.validate_xml_structure(Path("nonexistent.xml"))
        assert is_valid is False
