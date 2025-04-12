#!/usr/bin/env python3
import sys
import re
import math
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from functools import partial

def preprocess(text):
    """Preprocess text by converting to lowercase and removing non-alphanumeric characters"""
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)  # Keep word chars and spaces
    return [t for t in text.split() if t]

def get_cassandra_session():
    """Connect to Cassandra and return session"""
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search_engine')
    return cluster, session

def fetch_corpus_stats(session):
    """Fetch corpus statistics from Cassandra"""
    rows = session.execute("SELECT stat_key, stat_value FROM corpus_stats")
    stats = {row.stat_key: row.stat_value for row in rows}
    N = stats.get('N', 0)
    total_length = stats.get('total_doc_length', 0)
    avgDL = total_length / N if N > 0 else 0
    return N, avgDL

def fetch_term_stats(session, terms):
    """Fetch document frequencies for query terms"""
    placeholders = ','.join(['?'] * len(terms))
    query = f"SELECT word, df FROM term_stats WHERE word IN ({placeholders})"
    prepared = session.prepare(query)
    rows = session.execute(prepared, terms)
    return {row.word: row.df for row in rows}

def fetch_term_postings(session, term):
    """Fetch term frequency data for a term"""
    query = "SELECT doc_id, tf FROM term_frequency WHERE word = ?"
    prepared = session.prepare(query)
    rows = session.execute(prepared, [term])
    return [(row.doc_id, (term, row.tf)) for row in rows]

def fetch_doc_stats(session):
    """Fetch document statistics"""
    rows = session.execute("SELECT doc_id, title, doc_length FROM document_stats")
    return [(row.doc_id, (row.title, row.doc_length)) for row in rows]

def calculate_bm25_score(term_data, doc_data, N, avgDL, df_dict, k1=1.0, b=0.75):
    """Calculate BM25 score for a term-document pair"""
    (term, tf), (title, dl) = term_data, doc_data
    df = df_dict.get(term, 0)
    
    # Calculate IDF component
    if df == 0 or N == 0:
        return 0
    idf = math.log(N / df)
    
    # Calculate BM25 score
    numerator = (k1 + 1) * tf
    denominator = k1 * ((1 - b) + b * (dl / avgDL)) + tf
    if denominator == 0:
        return 0
        
    return idf * (numerator / denominator)

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 query.py \"your search query\"")
        sys.exit(1)
    
    # Get query from command line
    query = sys.argv[1]
    query_terms = preprocess(query)
    
    if not query_terms:
        print("No valid search terms in query")
        sys.exit(1)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("BM25 Search") \
        .getOrCreate()
    sc = spark.sparkContext
    
    # Connect to Cassandra
    cluster, session = get_cassandra_session()
    
    try:
        # Fetch corpus statistics
        N, avgDL = fetch_corpus_stats(session)
        
        # Fetch document frequencies for query terms
        df_dict = fetch_term_stats(session, query_terms)
        
        # Create broadcast variables
        N_b = sc.broadcast(N)
        avgDL_b = sc.broadcast(avgDL)
        df_dict_b = sc.broadcast(df_dict)
        
        # Create RDD of query terms that exist in the corpus
        valid_terms = [t for t in query_terms if t in df_dict]
        if not valid_terms:
            print("No matching terms found in the corpus")
            sys.exit(0)
            
        # Create RDD of term postings
        term_postings = []
        for term in valid_terms:
            term_postings.extend(fetch_term_postings(session, term))
        term_postings_rdd = sc.parallelize(term_postings)
        
        # Create RDD of document stats
        doc_stats = fetch_doc_stats(session)
        doc_stats_rdd = sc.parallelize(doc_stats)
        
        # Join postings with document stats
        joined_rdd = term_postings_rdd.join(doc_stats_rdd)
        
        # Calculate BM25 scores
        scores_rdd = joined_rdd.map(
            lambda x: (
                x[0],  # doc_id
                (
                    x[1][1][0],  # title
                    calculate_bm25_score(
                        x[1][0],  # term_data
                        x[1][1],  # doc_data
                        N_b.value,
                        avgDL_b.value,
                        df_dict_b.value
                    )
                )
            )
        )
        
        # Combine scores for same document
        final_scores_rdd = scores_rdd.reduceByKey(
            lambda a, b: (a[0], a[1] + b[1])
        )
        
        # Get top 10 results
        top_10 = final_scores_rdd.sortBy(
            lambda x: x[1][1],  # sort by score
            ascending=False
        ).take(10)
        
        # Print results
        print(f"\nTop 10 results for query: {query}\n")
        for i, (doc_id, (title, score)) in enumerate(top_10, 1):
            print(f"{i}. {title} (ID: {doc_id}, Score: {score:.4f})")
            
    finally:
        session.shutdown()
        cluster.shutdown()
        spark.stop()

if __name__ == "__main__":
    main()