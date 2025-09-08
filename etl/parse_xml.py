"""
MoMo Data Processor - XML Parser
Handles parsing of MoMo XML data files
"""

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging
from .config import Config

logger = logging.getLogger(__name__)

class XMLParser:
    """Parser for MoMo SMS XML files"""
    
    def __init__(self, config: Config):
        self.config = config
        self.supported_formats = ['.xml']
    
    def parse_file(self, file_path: Path) -> List[Dict[str, Any]]:
        """
        Parse a single XML file and extract SMS data
        
        Args:
            file_path: Path to the XML file to parse
            
        Returns:
            List of parsed SMS records
        """
        try:
            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                return []
            
            if file_path.suffix.lower() not in self.supported_formats:
                logger.warning(f"Unsupported file format: {file_path}")
                return []
            
            logger.info(f"Parsing XML file: {file_path}")
            
            # Parse XML file
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Extract SMS records based on MoMo XML structure
            records = self._extract_records(root)
            
            logger.info(f"Successfully parsed {len(records)} records from {file_path}")
            return records
            
        except ET.ParseError as e:
            logger.error(f"XML parsing error in {file_path}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error parsing {file_path}: {e}")
            return []
    
    def parse_directory(self, directory_path: Path) -> List[Dict[str, Any]]:
        """
        Parse all XML files in a directory
        
        Args:
            directory_path: Path to directory containing XML files
            
        Returns:
            List of all parsed SMS records
        """
        all_records = []
        
        if not directory_path.exists():
            logger.error(f"Directory not found: {directory_path}")
            return all_records
        
        xml_files = list(directory_path.glob("*.xml"))
        
        if not xml_files:
            logger.warning(f"No XML files found in {directory_path}")
            return all_records
        
        logger.info(f"Found {len(xml_files)} XML files to process")
        
        for xml_file in xml_files:
            records = self.parse_file(xml_file)
            all_records.extend(records)
        
        logger.info(f"Total records parsed: {len(all_records)}")
        return all_records
    
    def _extract_records(self, root: ET.Element) -> List[Dict[str, Any]]:
        """
        Extract SMS records from XML root element
        
        Args:
            root: XML root element
            
        Returns:
            List of extracted records
        """
        records = []
        
        # This is a placeholder implementation
        # The actual XML structure will depend on MoMo's format
        # Common patterns might include:
        # - <sms> elements containing individual messages
        # - <message> elements with attributes
        # - Nested structures with sender, recipient, content, timestamp
        
        # Example implementation (to be updated based on actual XML structure):
        for sms_element in root.findall('.//sms'):
            record = {
                'id': sms_element.get('id', ''),
                'sender': sms_element.get('sender', ''),
                'recipient': sms_element.get('recipient', ''),
                'content': sms_element.text or '',
                'timestamp': sms_element.get('timestamp', ''),
                'type': sms_element.get('type', 'unknown'),
                'raw_xml': ET.tostring(sms_element, encoding='unicode')
            }
            records.append(record)
        
        return records
    
    def validate_xml_structure(self, file_path: Path) -> bool:
        """
        Validate XML file structure
        
        Args:
            file_path: Path to XML file to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Add validation logic based on expected MoMo XML structure
            # This is a placeholder - implement based on actual requirements
            
            return True
            
        except ET.ParseError:
            return False
        except Exception:
            return False
