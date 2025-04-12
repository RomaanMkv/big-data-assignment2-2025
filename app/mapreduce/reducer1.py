#!/usr/bin/env python3
import sys
from cassandra.cluster import Cluster
from collections import defaultdict

def connect_to_cassandra():
    """Connect to Cassandra and return session"""
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search_engine')
    return cluster, session

def prepare_statements(session):
    """Prepare CQL statements for batch operations"""
    return {
        'tf': session.prepare(
            "INSERT INTO term_frequency (word, doc_id, tf) VALUES (?, ?, ?)"
        ),
        'doc_stats': session.prepare(
            "INSERT INTO document_stats (doc_id, title, doc_length) VALUES (?, ?, ?)"
        )
    }

def main():
    # Connect to Cassandra
    cluster, session = connect_to_cassandra()
    prepared_stmts = prepare_statements(session)
    
    try:
        current_key = None
        current_values = []
        
        # Process input line by line
        for line in sys.stdin:
            # Parse input
            line = line.strip()
            if not line:
                continue
                
            # Split line into key and value
            key, *values = line.split('\t')
            
            # If we encounter a new key
            if current_key and current_key != key:
                # Process the previous group
                process_group(current_key, current_values, session, prepared_stmts)
                current_values = []
            
            current_key = key
            current_values.append(values)
            
        # Process the last group
        if current_key:
            process_group(current_key, current_values, session, prepared_stmts)
            
    finally:
        session.shutdown()
        cluster.shutdown()

def process_group(key, values, session, prepared_stmts):
    """Process a group of values for a key"""
    if key.startswith('!'):
        # Special keys
        if key == '!DOCLEN':
            # Document length records
            for doc_id, length in values:
                # Store temporarily, will be used when processing title
                doc_lengths[doc_id] = int(length)
                # Emit for next MapReduce job
                print(f"!TOTALDOCS\t1")
                print(f"!TOTALLENGTH\t{length}")
        elif key == '!TITLE':
            # Document title records
            for doc_id, title in values:
                if doc_id in doc_lengths:
                    # Insert document stats
                    session.execute(
                        prepared_stmts['doc_stats'],
                        (doc_id, title, doc_lengths[doc_id])
                    )
    else:
        # Term frequency records
        for doc_id, tf in values:
            # Insert term frequency
            session.execute(
                prepared_stmts['tf'],
                (key, doc_id, int(tf))
            )
            # Emit for document frequency calculation
            print(f"{key}\t1")

# Global dictionary to store document lengths temporarily
doc_lengths = {}

if __name__ == "__main__":
    main()
