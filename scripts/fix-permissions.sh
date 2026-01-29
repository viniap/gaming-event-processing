#!/bin/bash

# Permission Fixer Script
# Continuously fixes permissions on storage directories to prevent checkpoint errors

echo "🔐 Starting permission fixer..."

while true; do
    if [ -w storage ]; then
        chmod -R 777 storage 2>/dev/null || true
    else
        sudo chmod -R 777 storage 2>/dev/null || true
    fi
    sleep 30
done
