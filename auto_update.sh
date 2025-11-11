#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "================================================"
echo "Starting NBA data pull at $(date)"
echo "================================================"

# Activate virtual environment
source .venv/bin/activate

# Run the Python script
python nba_pull.py

# Check if it succeeded
if [ $? -eq 0 ]; then
    echo ""
    echo "Data pull successful! Committing changes..."
    
    git add data/
    
    # Check if there are changes to commit
    if git diff --staged --quiet; then
        echo "No changes to commit"
    else
        git commit -m "Automated daily NBA data update - $(date +%Y-%m-%d)"
        git push origin workflow
        
        echo ""
        echo "================================================"
        echo "✅ Completed successfully at $(date)"
        echo "================================================"
    fi
else
    echo ""
    echo "================================================"
    echo "❌ Data pull failed at $(date)"
    echo "================================================"
    exit 1
fi

# Deactivate virtual environment
deactivate