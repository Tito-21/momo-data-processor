#!/bin/bash
# AMAzing SMS - JSON Export Script
# This script exports processed data to JSON format

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
OUTPUT_FILE="data/processed/export_$(date +%Y%m%d_%H%M%S).json"
CATEGORY=""
START_DATE=""
END_DATE=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        -c|--category)
            CATEGORY="$2"
            shift 2
            ;;
        -s|--start-date)
            START_DATE="$2"
            shift 2
            ;;
        -e|--end-date)
            END_DATE="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  -o, --output FILE     Output file path (default: data/processed/export_TIMESTAMP.json)"
            echo "  -c, --category CAT    Filter by category"
            echo "  -s, --start-date DATE Filter by start date (YYYY-MM-DD)"
            echo "  -e, --end-date DATE   Filter by end date (YYYY-MM-DD)"
            echo "  -h, --help            Show this help message"
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

# Create output directory if it doesn't exist
mkdir -p "$(dirname "$OUTPUT_FILE")"

# Run export
echo "Exporting data to JSON..."
echo "Output file: $OUTPUT_FILE"

# Build export command
EXPORT_CMD="python -c \"
import sys
sys.path.append('$PROJECT_ROOT')
from api.db import DatabaseManager
import json
from datetime import datetime

db = DatabaseManager('$PROJECT_ROOT/data/db.sqlite3')

# Get messages with filters
messages = db.get_messages(
    skip=0,
    limit=100000,  # Large limit for export
    category='$CATEGORY' if '$CATEGORY' else None,
    start_date='$START_DATE' if '$START_DATE' else None,
    end_date='$END_DATE' if '$END_DATE' else None
)

# Prepare export data
export_data = {
    'export_timestamp': datetime.utcnow().isoformat(),
    'total_records': len(messages),
    'filters': {
        'category': '$CATEGORY' if '$CATEGORY' else None,
        'start_date': '$START_DATE' if '$START_DATE' else None,
        'end_date': '$END_DATE' if '$END_DATE' else None
    },
    'data': [message.dict() for message in messages]
}

# Write to file
with open('$OUTPUT_FILE', 'w', encoding='utf-8') as f:
    json.dump(export_data, f, indent=2, ensure_ascii=False)

print(f'Exported {len(messages)} records to $OUTPUT_FILE')
\""

eval $EXPORT_CMD

echo "Export completed successfully!"
echo "Output file: $OUTPUT_FILE"
