import sys
from cassandra.cluster import Cluster

def connect_to_cassandra():
    """Connect to Cassandra and return session"""
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search_engine')
    return cluster, session

def prepare_statements(session):
    """Prepare CQL statements for batch operations"""
    return {
        'term_stats': session.prepare(
            "INSERT INTO term_stats (word, df) VALUES (?, ?)"
        ),
        'corpus_stats': session.prepare(
            "INSERT INTO corpus_stats (stat_key, stat_value) VALUES (?, ?)"
        )
    }

def main():
    cluster, session = connect_to_cassandra()
    prepared_stmts = prepare_statements(session)
    
    try:
        current_key = None
        current_count = 0
        total_docs = 0
        total_length = 0
        
        # Process input line by line
        for line in sys.stdin:
            # Parse input
            line = line.strip()
            if not line:
                continue
                
            # Split line into key and value
            key, value = line.split('\t')
            
            # If we encounter a new key
            if current_key and current_key != key:
                # Process the previous group
                process_group(current_key, current_count, session, prepared_stmts)
                current_count = 0
            
            current_key = key
            
            # Handle special keys
            if key == '!TOTALDOCS':
                total_docs += 1
            elif key == '!TOTALLENGTH':
                total_length += int(value)
            else:
                current_count += int(value)
            
        # Process the last group
        if current_key:
            process_group(current_key, current_count, session, prepared_stmts)
        
        # Store corpus stats
        session.execute(prepared_stmts['corpus_stats'], ('N', total_docs))
        session.execute(prepared_stmts['corpus_stats'], ('total_doc_length', total_length))
            
    finally:
        session.shutdown()
        cluster.shutdown()

def process_group(key, count, session, prepared_stmts):
    """Process a group of values for a key"""
    if not key.startswith('!'):
        # Store document frequency for term
        session.execute(prepared_stmts['term_stats'], (key, count))

if __name__ == "__main__":
    main() 