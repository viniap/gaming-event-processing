#!/bin/bash

# Shutdown Script for Gaming Event Processing System

set -e

echo "=========================================="
echo "Gaming Event Processing System - Shutdown"
echo "=========================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running."
    exit 1
fi

# Stop services
echo "🛑 Stopping all services..."
cd build
if docker compose down; then
    echo "✅ All services stopped successfully"
else
    echo "❌ Failed to stop services"
    exit 1
fi
cd ..

echo ""
echo "=========================================="
echo "✅ System shutdown complete!"
echo "=========================================="
echo ""
