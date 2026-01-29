#!/bin/bash

# Script to visualize data from all Delta tables
# Usage: 
#   ./scripts/visualize-data.sh                      # Show all tables
#   ./scripts/visualize-data.sh --all               # Show all tables
#   ./scripts/visualize-data.sh --table bronze      # Show specific table
#   ./scripts/visualize-data.sh --table bronze --table silver-init  # Show multiple tables
#
# Available tables:
#   - bronze
#   - silver-init
#   - silver-match
#   - silver-purchase
#   - gold-daily-users

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "🎮 Starting Data Visualizer..."
echo ""

# Pass all arguments to the Python script
docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/src/visualizer/main.py "$@"

echo ""
echo "✅ Visualization complete!"
