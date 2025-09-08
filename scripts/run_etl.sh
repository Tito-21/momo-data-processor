#!/bin/bash
# AMAzing SMS - ETL Pipeline Runner Script
# This script runs the ETL pipeline with proper environment setup

set -e  # Exit on any error

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Change to project root directory
cd "$PROJECT_ROOT"

# Set environment variables
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"
export DATABASE_URL="sqlite:///$(pwd)/data/db.sqlite3"
export RAW_DATA_PATH="$(pwd)/data/raw"
export PROCESSED_DATA_PATH="$(pwd)/data/processed"
export LOG_PATH="$(pwd)/data/logs"

# Create necessary directories
mkdir -p data/raw data/processed data/logs/dead_letter

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

# Run ETL pipeline
echo "Starting ETL pipeline..."
python -m etl.run "$@"

echo "ETL pipeline completed successfully!"
