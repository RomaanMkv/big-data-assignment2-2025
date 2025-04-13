#!/usr/bin/env python3
from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
import os

def create_spark_session():
    return SparkSession.builder \
        .appName('data preparation') \
        .master("local") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .getOrCreate()

def create_doc(row):
    try:
        filename = f"data/{sanitize_filename(str(row['id']) + '_' + row['title']).replace(' ', '_')}.txt"
        with open(filename, "w", encoding='utf-8') as f:
            f.write(row['text'])
        return True
    except Exception as e:
        print(f"Error creating document {row['id']}: {str(e)}")
        return False

def main():
    spark = create_spark_session()
    
    try:
        print("Reading parquet file...")
        df = spark.read.parquet("/a.parquet")
        
        total_count = df.count()
        n = min(1000, total_count)
        
        print(f"Total documents in parquet: {total_count}")
        print(f"Sampling {n} documents...")
        
        df = df.select(['id', 'title', 'text']) \
               .sample(fraction=100 * n / total_count, seed=0) \
               .limit(n)
        
        # Create individual document files
        print("Creating individual document files...")
        success_count = df.rdd.map(create_doc).filter(lambda x: x).count()
        print(f"Successfully created {success_count} document files")
        
        # Create combined TSV file
        print("Creating combined TSV file...")
        df.write.csv("/index/data", sep="\t", mode="overwrite")
        print("Successfully created combined TSV file")
        
    except Exception as e:
        print(f"Error during data preparation: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()