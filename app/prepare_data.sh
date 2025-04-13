#!/bin/bash

echo "Starting data preparation..."

echo "Creating HDFS directories..."
hdfs dfs -mkdir -p /data
hdfs dfs -mkdir -p /index/data

hdfs dfs -put /app/a.parquet /a.parquet

# Check if parquet file exists in HDFS
if ! hdfs dfs -test -f /a.parquet; then
    echo "Parquet file not found in HDFS. Please ensure a.parquet is uploaded to HDFS root directory."
    exit 1
fi

echo "Running data preparation script..."
python3 prepare_data.py

# uncomment the following line if you want to copy data from local to HDFS
# Be careful, this takes a long time
# echo "Copying data from local to HDFS..."
# hdfs dfs -put ./data /data

echo "Verifying results..."

echo "Checking /index/data directory:"
hdfs dfs -ls /index/data

echo "Data preparation complete!"