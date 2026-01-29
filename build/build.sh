#!/bin/bash
SPARK_VERSION="3.5.0"

# Build the Docker images
docker build -t spark-local:${SPARK_VERSION} -f Dockerfile.spark .
docker build -t spark-history-server-local:${SPARK_VERSION} -f Dockerfile.history .
