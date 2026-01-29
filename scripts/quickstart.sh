#!/bin/bash

# Quick Start Script for Gaming Event Processing System
# This script sets up and starts all services in the correct order

set -e

echo "=============================================="
echo "  Gaming Event Processing - QuickStart"
echo "=============================================="
echo ""

# Change to project root directory
cd "$(dirname "$0")/.."

# Check if Docker is running
echo "🔍 Checking Docker..."
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker and try again."
    exit 1
fi
echo "✅ Docker is running"
echo ""

# Create necessary directories with proper permissions
echo "📁 Setting up storage directories..."
mkdir -p storage/bronze/events
mkdir -p storage/silver/{init,match,purchase}
mkdir -p storage/gold/daily_users
mkdir -p storage/checkpoints/{bronze_ingestion_init-events,bronze_ingestion_match-events,bronze_ingestion_purchase-events}
mkdir -p storage/checkpoints/{silver_init,silver_match,silver_purchase}
mkdir -p storage/checkpoints/{silver_init_kafka,silver_match_kafka,silver_purchase_kafka}
mkdir -p storage/checkpoints/{gold_purchases,gold_matches,gold_purchases_by_country,gold_matches_by_country}
mkdir -p storage/checkpoints/gold_purchases/state/0/0
mkdir -p storage/checkpoints/gold_matches/state/0/0
mkdir -p storage/checkpoints/gold_purchases_by_country/state/0/0
mkdir -p storage/checkpoints/gold_matches_by_country/state/0/0
mkdir -p build/data/{zookeeper,kafka}
mkdir -p build/logs/{zookeeper,spark}
mkdir -p build/metrics/spark

# Set permissions (use sudo if needed)
echo "🔐 Setting permissions..."
if [ -w storage ]; then
    chmod -R 777 storage build/data build/logs build/metrics 2>/dev/null || true
    # Set default permissions for new files/directories
    find storage -type d -exec chmod 777 {} \; 2>/dev/null || true
    find storage -type f -exec chmod 666 {} \; 2>/dev/null || true
else
    echo "   Using sudo for permissions..."
    sudo chmod -R 777 storage build/data build/logs build/metrics 2>/dev/null || true
    sudo find storage -type d -exec chmod 777 {} \; 2>/dev/null || true
    sudo find storage -type f -exec chmod 666 {} \; 2>/dev/null || true
fi
echo "✅ Directories created and permissions set"
echo ""

# Build Spark images if needed
echo "🔍 Checking Spark Docker images..."
if ! docker images | grep -q "spark-local.*3.5.0"; then
    echo "⚠️  Spark images not found. Building (this may take a few minutes)..."
    cd build
    ./build.sh
    cd ..
    echo "✅ Spark images built"
else
    echo "✅ Spark images found"
fi
echo ""

# Build producer image if needed
echo "🔍 Checking Producer Docker image..."
if ! docker images | grep -q "event-producer"; then
    echo "⚠️  Producer image not found. Building..."
    cd build
    docker build -t event-producer:latest -f Dockerfile.producer ..
    cd ..
    echo "✅ Producer image built"
else
    echo "✅ Producer image found"
fi
echo ""

# Step 1: Start infrastructure services (Kafka, Zookeeper, Spark)
echo "🚀 Step 1/5: Starting infrastructure services (Kafka, Zookeeper, Spark)..."
cd build
docker compose up -d zookeeper kafka spark-master spark-worker-1 spark-worker-2 spark-worker-3 spark-history-server
cd ..
echo "✅ Infrastructure services started"
echo ""

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready (30 seconds)..."
sleep 30
echo "✅ Kafka should be ready"
echo ""

# Step 2: Start event producer
echo "🚀 Step 2/5: Starting event producer..."
cd build
docker compose up -d event-producer
cd ..
echo "✅ Event producer started"
echo ""

# Wait for events to be generated
echo "⏳ Waiting for events to be generated (60 seconds)..."
sleep 60
echo "✅ Events should be flowing to Kafka topics"
echo ""

# Step 3: Start bronze ingestion jobs (sequentially)
echo "🚀 Step 3/5: Starting bronze ingestion jobs..."
cd build
echo "   Starting init ingestion..."
docker compose up -d bronze-ingestion-init
sleep 15
echo "   Starting match ingestion..."
docker compose up -d bronze-ingestion-match
sleep 15
echo "   Starting purchase ingestion..."
docker compose up -d bronze-ingestion-purchase
cd ..
echo "✅ Bronze ingestion jobs started"
echo ""

# Wait for bronze tables to be populated
echo "⏳ Waiting for bronze tables to be populated (45 seconds)..."
sleep 20
# Fix permissions during runtime to prevent checkpoint issues
echo "   Fixing runtime permissions..."
if [ -w storage ]; then
    chmod -R 777 storage 2>/dev/null || true
else
    sudo chmod -R 777 storage 2>/dev/null || true
fi
sleep 25
echo "✅ Bronze tables should have data"
echo ""

# Step 4: Start data quality (silver layer) job
echo "🚀 Step 4/5: Starting data quality job..."
cd build
docker compose up -d data-quality
cd ..
echo "✅ Data quality job started"
echo ""

# Wait for silver tables to be populated
echo "⏳ Waiting for silver tables to be populated (60 seconds)..."
sleep 60
echo "✅ Silver tables should have data"
echo ""

# Step 5: Start real-time aggregation (gold layer)
echo "🚀 Step 5/5: Starting real-time aggregation..."
cd build
docker compose up -d realtime-aggregation
cd ..
echo "✅ Real-time aggregation started"
echo ""

# Check service health
echo "🔍 Checking service status..."
cd build
docker compose ps
cd ..

echo ""
echo "=============================================="
echo "✅ Setup Complete!"
echo "=============================================="
echo ""
echo "📊 Services Running:"
echo "   - Zookeeper (port 2181)"
echo "   - Kafka (port 9092)"
echo "   - Spark Master UI: http://localhost:9090"
echo "   - Spark History: http://localhost:18080"
echo "   - Spark Workers (3 instances)"
echo "   - Event Producer (generating events)"
echo "   - Bronze Ingestion (3 jobs: init, match, purchase)"
echo "   - Data Quality (silver layer transformation)"
echo "   - Real-time Aggregation (minute-level metrics)"
echo ""
echo "� Tip: If you encounter permission errors, run in another terminal:"
echo "   ./scripts/fix-permissions.sh"
echo ""
echo "�📝 Next Steps:"
echo "   1. View logs: cd build && docker compose logs -f [service-name]"
echo "   2. Monitor services: watch -n 2 'cd build && docker compose ps'"
echo "   3. Visualize data: ./scripts/visualize-data.sh"
echo "   4. Run daily aggregation: ./scripts/run-daily-aggregation.sh"
echo "   5. Shutdown all: ./scripts/shutdown.sh"
echo ""
echo "📁 Data locations:"
echo "   - Bronze: ./storage/bronze/events/"
echo "   - Silver: ./storage/silver/{init,match,purchase}/"
echo "   - Gold: ./storage/gold/daily_users/"
echo ""
echo "🎉 System is ready!"
echo ""
