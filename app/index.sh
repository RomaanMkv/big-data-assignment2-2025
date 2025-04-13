#!/bin/bash

# Get input path, default to /index/data if not provided
HDFS_INPUT=${1:-/index/data}
HDFS_INTERMEDIATE=/tmp/index/intermediate
HDFS_FINAL=/tmp/index/final_dummy_output

source .venv/bin/activate

echo "Setting up Cassandra schema..."
python3 setup_cassandra.py

echo "Cleaning up HDFS directories..."
hdfs dfs -rm -r -f $HDFS_INTERMEDIATE
hdfs dfs -rm -r -f $HDFS_FINAL

echo "Running Indexer Pipeline 1: TF, DocLength..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.name="Indexer_Pipeline1_TF_DL" \
    -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
    -archives /app/.venv.tar.gz#.venv \
    -mapper ".venv/bin/python mapper1.py" \
    -reducer ".venv/bin/python reducer1.py" \
    -input $HDFS_INPUT \
    -output $HDFS_INTERMEDIATE

# Check if Pipeline 1 succeeded
if [ $? -ne 0 ]; then
    echo "Indexer Pipeline 1 failed!"
    exit 1
fi

echo "Running Indexer Pipeline 2: DF, N, TotalLength..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -D mapreduce.job.name="Indexer_Pipeline2_DF_N" \
    -files /app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py \
    -archives /app/.venv.tar.gz#.venv \
    -mapper ".venv/bin/python mapper2.py" \
    -reducer ".venv/bin/python reducer2.py" \
    -input $HDFS_INTERMEDIATE \
    -output $HDFS_FINAL

# Check if Pipeline 2 succeeded
if [ $? -ne 0 ]; then
    echo "Indexer Pipeline 2 failed!"
    exit 1
fi

echo "Cleaning up..."
hdfs dfs -rm -r -f $HDFS_INTERMEDIATE
hdfs dfs -rm -r -f $HDFS_FINAL

echo "Indexing complete!"
