"""
AMAzing SMS - Pydantic Schemas
Data models and validation schemas for the API
"""

from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List, Generic, TypeVar
from datetime import datetime, date

# Generic type for API responses
T = TypeVar('T')

class APIResponse(BaseModel, Generic[T]):
    """Generic API response wrapper"""
    success: bool
    data: T
    message: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class SMSMessage(BaseModel):
    """SMS message data model"""
    id: str = Field(..., description="Unique message identifier")
    sender: str = Field(..., description="Sender phone number")
    recipient: str = Field(..., description="Recipient phone number")
    content: str = Field(..., description="Message content")
    timestamp: Optional[str] = Field(None, description="Message timestamp")
    type: str = Field(default="sms", description="Message type")
    category: str = Field(default="unknown", description="Message category")
    category_confidence: float = Field(default=0.0, ge=0.0, le=1.0, description="Category confidence score")
    category_breakdown: Optional[Dict[str, float]] = Field(None, description="Detailed category scores")
    processed_at: str = Field(..., description="Processing timestamp")
    created_at: Optional[str] = Field(None, description="Database creation timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class CategoryStats(BaseModel):
    """Category statistics data model"""
    category: str = Field(..., description="Message category")
    count: int = Field(..., ge=0, description="Number of messages in category")
    avg_confidence: float = Field(..., ge=0.0, le=1.0, description="Average confidence score")

class ProcessingStats(BaseModel):
    """ETL processing statistics data model"""
    batch_id: str = Field(..., description="Batch processing identifier")
    total_records: int = Field(..., ge=0, description="Total records processed")
    successful_records: int = Field(..., ge=0, description="Successfully processed records")
    failed_records: int = Field(..., ge=0, description="Failed records")
    processing_time: float = Field(..., ge=0.0, description="Processing time in seconds")
    started_at: str = Field(..., description="Processing start timestamp")
    completed_at: str = Field(..., description="Processing completion timestamp")
    status: str = Field(..., description="Processing status")

class MessageFilter(BaseModel):
    """Message filtering parameters"""
    category: Optional[str] = Field(None, description="Filter by category")
    sender: Optional[str] = Field(None, description="Filter by sender")
    start_date: Optional[date] = Field(None, description="Filter by start date")
    end_date: Optional[date] = Field(None, description="Filter by end date")
    limit: int = Field(default=100, ge=1, le=1000, description="Maximum number of records")
    skip: int = Field(default=0, ge=0, description="Number of records to skip")

class ETLRunRequest(BaseModel):
    """ETL pipeline run request"""
    input_path: Optional[str] = Field(None, description="Input file or directory path")
    batch_size: Optional[int] = Field(None, ge=1, le=10000, description="Batch size for processing")
    force_reprocess: bool = Field(default=False, description="Force reprocessing of existing data")

class ETLRunResponse(BaseModel):
    """ETL pipeline run response"""
    batch_id: str = Field(..., description="Batch processing identifier")
    status: str = Field(..., description="Processing status")
    total_records: int = Field(..., ge=0, description="Total records to process")
    estimated_time: Optional[float] = Field(None, description="Estimated processing time in seconds")

class ExportRequest(BaseModel):
    """Data export request"""
    format: str = Field(default="json", description="Export format")
    category: Optional[str] = Field(None, description="Filter by category")
    start_date: Optional[date] = Field(None, description="Filter by start date")
    end_date: Optional[date] = Field(None, description="Filter by end date")
    include_metadata: bool = Field(default=True, description="Include processing metadata")

class HealthCheck(BaseModel):
    """Health check response"""
    status: str = Field(..., description="Service status")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Check timestamp")
    version: str = Field(default="1.0.0", description="API version")
    database_connected: bool = Field(..., description="Database connection status")

class ErrorResponse(BaseModel):
    """Error response model"""
    success: bool = Field(default=False)
    error: str = Field(..., description="Error message")
    error_code: Optional[str] = Field(None, description="Error code")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")
