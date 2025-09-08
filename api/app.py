"""
AMAzing SMS - FastAPI Application
Main API application for SMS data processing and retrieval
"""

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from typing import List, Optional, Dict, Any
from datetime import datetime, date
import logging

from .db import get_database, DatabaseManager
from .schemas import SMSMessage, CategoryStats, ProcessingStats, APIResponse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="AMAzing SMS API",
    description="API for MoMo SMS XML data processing and retrieval",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
app.mount("/static", StaticFiles(directory="web"), name="static")

@app.get("/")
async def serve_frontend():
    """Serve the main frontend page"""
    return FileResponse("index.html")

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@app.get("/api/stats", response_model=APIResponse[Dict[str, Any]])
async def get_statistics(db: DatabaseManager = Depends(get_database)):
    """Get overall system statistics"""
    try:
        stats = db.get_database_stats()
        return APIResponse(
            success=True,
            data=stats,
            message="Statistics retrieved successfully"
        )
    except Exception as e:
        logger.error(f"Error retrieving statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/messages", response_model=APIResponse[List[SMSMessage]])
async def get_messages(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    category: Optional[str] = Query(None, description="Filter by category"),
    sender: Optional[str] = Query(None, description="Filter by sender"),
    start_date: Optional[date] = Query(None, description="Filter by start date"),
    end_date: Optional[date] = Query(None, description="Filter by end date"),
    db: DatabaseManager = Depends(get_database)
):
    """Get SMS messages with optional filtering"""
    try:
        messages = db.get_messages(
            skip=skip,
            limit=limit,
            category=category,
            sender=sender,
            start_date=start_date,
            end_date=end_date
        )
        return APIResponse(
            success=True,
            data=messages,
            message=f"Retrieved {len(messages)} messages"
        )
    except Exception as e:
        logger.error(f"Error retrieving messages: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/messages/{message_id}", response_model=APIResponse[SMSMessage])
async def get_message(
    message_id: str,
    db: DatabaseManager = Depends(get_database)
):
    """Get a specific SMS message by ID"""
    try:
        message = db.get_message_by_id(message_id)
        if not message:
            raise HTTPException(status_code=404, detail="Message not found")
        
        return APIResponse(
            success=True,
            data=message,
            message="Message retrieved successfully"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving message {message_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/categories", response_model=APIResponse[List[CategoryStats]])
async def get_category_statistics(
    date_from: Optional[date] = Query(None, description="Start date for statistics"),
    date_to: Optional[date] = Query(None, description="End date for statistics"),
    db: DatabaseManager = Depends(get_database)
):
    """Get category statistics"""
    try:
        stats = db.get_category_statistics(date_from, date_to)
        return APIResponse(
            success=True,
            data=stats,
            message="Category statistics retrieved successfully"
        )
    except Exception as e:
        logger.error(f"Error retrieving category statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/processing-logs", response_model=APIResponse[List[ProcessingStats]])
async def get_processing_logs(
    limit: int = Query(50, ge=1, le=200, description="Number of logs to return"),
    db: DatabaseManager = Depends(get_database)
):
    """Get ETL processing logs"""
    try:
        logs = db.get_processing_logs(limit)
        return APIResponse(
            success=True,
            data=logs,
            message="Processing logs retrieved successfully"
        )
    except Exception as e:
        logger.error(f"Error retrieving processing logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/etl/run")
async def run_etl_pipeline(
    input_path: Optional[str] = None,
    db: DatabaseManager = Depends(get_database)
):
    """Trigger ETL pipeline execution"""
    try:
        # This would integrate with the ETL runner
        # For now, return a placeholder response
        return APIResponse(
            success=True,
            data={"message": "ETL pipeline execution triggered"},
            message="ETL pipeline started successfully"
        )
    except Exception as e:
        logger.error(f"Error running ETL pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/export/json")
async def export_json(
    category: Optional[str] = Query(None, description="Filter by category"),
    start_date: Optional[date] = Query(None, description="Filter by start date"),
    end_date: Optional[date] = Query(None, description="Filter by end date"),
    db: DatabaseManager = Depends(get_database)
):
    """Export data as JSON"""
    try:
        messages = db.get_messages(
            skip=0,
            limit=10000,  # Large limit for export
            category=category,
            start_date=start_date,
            end_date=end_date
        )
        
        export_data = {
            "export_timestamp": datetime.utcnow().isoformat(),
            "total_records": len(messages),
            "filters": {
                "category": category,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None
            },
            "data": [message.dict() for message in messages]
        }
        
        return export_data
    except Exception as e:
        logger.error(f"Error exporting data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
