#!/bin/bash

# Exit on error
set -e

echo "Starting data preparation..."

# Create necessary HDFS directories
echo "Creating HDFS directories..."
hdfs dfs -mkdir -p /data
hdfs dfs -mkdir -p /index/data

# Check if parquet file exists in HDFS
hdfs dfs -put /app/a.parquet /a.parquet

if ! hdfs dfs -test -f /a.parquet; then
    echo "Parquet file not found in HDFS. Please ensure a.parquet is uploaded to HDFS root directory."
    exit 1
fi

# Run the Python script
echo "Running data preparation script..."
python3 prepare_data.py

# Verify the results
echo "Verifying results..."
echo "Checking /data directory:"
hdfs dfs -ls /data

echo "Checking /index/data directory:"
hdfs dfs -ls /index/data

echo "Data preparation complete!"