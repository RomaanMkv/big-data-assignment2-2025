#!/bin/bash

# Exit on error
set -e

echo "Starting services..."
# Start ssh server
service ssh restart 

# Starting Hadoop and Cassandra services
bash start-services.sh

echo "Setting up Python environment..."
# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install required packages
pip install -r requirements.txt

# Package the virtual environment
venv-pack -o .venv.tar.gz

echo "Preparing data..."
# Prepare and load data
bash prepare_data.sh

echo "Running indexer..."
# Run the indexer on the prepared data
bash index.sh

echo "Testing search engine..."
# Run a test search query
bash search.sh "gucci dog"

echo "Setup complete! The search engine is ready to use."
echo "To search, use: bash search.sh \"your search query\""

# Keep container running
tail -f /dev/null