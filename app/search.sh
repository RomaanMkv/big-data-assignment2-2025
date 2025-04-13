#!/bin/bash

# Check if query is provided
if [ -z "$1" ]; then
    echo "Usage: $0 \"<search query>\""
    exit 1
fi

# Activate virtual environment
source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python)

# Python of the excutor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

echo "Searching for: $1"

# Run the search query
spark-submit \
    --master yarn \
    --deploy-mode client \
    --name "BM25 Search Query" \
    --archives /app/.venv.tar.gz#.venv \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python \
    --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=$(which python) \
    query.py "$1"

# Check if search was successful
if [ $? -ne 0 ]; then
    echo "Search failed!"
    exit 1
fi