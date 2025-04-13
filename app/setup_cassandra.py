from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time

def wait_for_cassandra(cluster, max_attempts=10, delay=5):
    """Wait for Cassandra to become available"""
    for attempt in range(max_attempts):
        try:
            session = cluster.connect()
            session.shutdown()
            return True
        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_attempts} failed: {str(e)}")
            if attempt < max_attempts - 1:
                print(f"Waiting {delay} seconds before next attempt...")
                time.sleep(delay)
    return False

def create_keyspace_and_tables(cluster):
    """Create the keyspace and tables for the search engine"""
    session = cluster.connect()
    
    print("Creating keyspace...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS search_engine
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    
    session.set_keyspace('search_engine')
    
    print("Creating tables...")
    
    # Table for term frequency
    session.execute("""
        CREATE TABLE IF NOT EXISTS term_frequency (
            word text,
            doc_id text,
            tf int,
            PRIMARY KEY (word, doc_id)
        )
    """)
    
    # Table for document stats
    session.execute("""
        CREATE TABLE IF NOT EXISTS document_stats (
            doc_id text PRIMARY KEY,
            title text,
            doc_length int
        )
    """)
    
    # Table for term stats (document frequency)
    session.execute("""
        CREATE TABLE IF NOT EXISTS term_stats (
            word text PRIMARY KEY,
            df bigint
        )
    """)
    
    # Table for corpus stats
    session.execute("""
        CREATE TABLE IF NOT EXISTS corpus_stats (
            stat_key text PRIMARY KEY,
            stat_value bigint
        )
    """)
    
    print("Schema creation complete!")
    session.shutdown()

def main():
    print("Connecting to Cassandra...")
    cluster = Cluster(['cassandra-server'])
    
    if not wait_for_cassandra(cluster):
        print("Failed to connect to Cassandra after multiple attempts")
        return
    
    # Create schema
    try:
        create_keyspace_and_tables(cluster)
        print("Successfully initialized Cassandra schema")
    except Exception as e:
        print(f"Error initializing Cassandra schema: {str(e)}")
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    main() 