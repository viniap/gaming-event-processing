#!/bin/bash

# Run Daily Aggregation Script
# This script runs the batch aggregation job for a specific date

set -e

echo "=============================================="
echo "  Daily Aggregation Job"
echo "=============================================="
echo ""

# Change to project root directory
cd "$(dirname "$0")/.."

# Parse command-line arguments
DATE=${1:-$(date +%Y-%m-%d)}

echo "📊 Running daily aggregation for date: $DATE"
echo ""

# Check if Spark cluster is running
echo "🔍 Checking Spark cluster status..."
cd build
if ! docker compose ps spark-master | grep -q "Up"; then
    echo "❌ Error: Spark master is not running."
    echo "   Please start the cluster first with: ./scripts/quickstart.sh"
    exit 1
fi
echo "✅ Spark cluster is running"
echo ""

# Run the batch aggregation job
echo "🚀 Starting batch aggregation job..."
docker compose run --rm \
    -e PYTHONPATH=/opt/bitnami/spark/jobs \
    -e AGGREGATION_DATE=$DATE \
    spark-master \
    spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --packages io.delta:delta-spark_2.12:3.2.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    /opt/bitnami/spark/jobs/src/aggregation/batch/main.py

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Daily aggregation completed successfully!"
    echo ""
    echo "📁 Results saved to: ./storage/gold/daily_users/"
    echo "📊 Visualize the data: ./scripts/visualize-data.sh"
else
    echo ""
    echo "❌ Daily aggregation failed. Check logs above for details."
    exit 1
fi

cd ..
echo ""
