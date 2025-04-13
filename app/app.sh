#!/bin/bash


echo "Starting services..."
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

echo "Setting up Python environment..."
# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt

# Package the virtual env.
venv-pack -o .venv.tar.gz

echo "Preparing data..."
# Collect data
bash prepare_data.sh

echo "Running indexer..."
# Run the indexer
bash index.sh

echo "Testing search engine..."
# Run the ranker
bash search.sh "gucci dog"

echo "Setup complete! The search engine is ready to use."
echo "To search, use: bash search.sh \"your search query\""

# Keep container running
tail -f /dev/null