import sys
import re
import os

def preprocess(text):
    """Preprocess text by converting to lowercase and removing non-alphanumeric characters"""
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    return text.split()

def main():
    # Buffer for document content
    current_doc_content = []
    current_doc_id = None
    current_doc_title = None
    
    # Process input line by line
    for line in sys.stdin:
        # Skip empty lines
        if not line.strip():
            continue
        
        # Split line into fields (tab-separated)
        fields = line.strip().split('\t')
        
        # Expect 3 fields: doc_id, title, text
        if len(fields) != 3:
            continue
            
        doc_id, title, text = fields
        
        # Process the document
        tokens = preprocess(text)
        
        # Count term frequencies
        term_freq = {}
        for token in tokens:
            if token:
                term_freq[token] = term_freq.get(token, 0) + 1
        
        # Emit term frequencies
        for term, freq in term_freq.items():
            print(f"{term}\t{doc_id}\t{freq}")
        
        # Emit document length
        print(f"!DOCLEN\t{doc_id}\t{len(tokens)}")
        
        # Emit document title
        print(f"!TITLE\t{doc_id}\t{title}")

if __name__ == "__main__":
    main()
