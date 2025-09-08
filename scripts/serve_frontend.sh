#!/bin/bash
# MoMo Data Processor - Frontend Server Script
# This script serves the frontend and API

set -e  # Exit on any error

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Change to project root directory
cd "$PROJECT_ROOT"

# Set environment variables
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"
export DATABASE_URL="sqlite:///$(pwd)/data/db.sqlite3"

# Default values
HOST="0.0.0.0"
PORT="8000"
WORKERS="1"
RELOAD=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--host)
            HOST="$2"
            shift 2
            ;;
        -p|--port)
            PORT="$2"
            shift 2
            ;;
        -w|--workers)
            WORKERS="$2"
            shift 2
            ;;
        -r|--reload)
            RELOAD="--reload"
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  -h, --host HOST       Host to bind to (default: 0.0.0.0)"
            echo "  -p, --port PORT       Port to bind to (default: 8000)"
            echo "  -w, --workers NUM     Number of worker processes (default: 1)"
            echo "  -r, --reload          Enable auto-reload for development"
            echo "  --help                Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Create necessary directories
mkdir -p data/raw data/processed data/logs/dead_letter

# Check if database exists, if not create it
if [ ! -f "data/db.sqlite3" ]; then
    echo "Initializing database..."
    python -c "
import sys
sys.path.append('$PROJECT_ROOT')
from etl.load_db import DatabaseLoader
from etl.config import Config

config = Config()
db_loader = DatabaseLoader(config)
print('Database initialized successfully')
"
fi

# Start the server
echo "Starting MoMo Data Processor server..."
echo "Host: $HOST"
echo "Port: $PORT"
echo "Workers: $WORKERS"
echo "Reload: $([ -n "$RELOAD" ] && echo "enabled" || echo "disabled")"
echo ""
echo "Access the application at: http://$HOST:$PORT"
echo "API documentation at: http://$HOST:$PORT/api/docs"
echo ""
echo "Press Ctrl+C to stop the server"

# Run the FastAPI application
uvicorn api.app:app --host "$HOST" --port "$PORT" --workers "$WORKERS" $RELOAD
