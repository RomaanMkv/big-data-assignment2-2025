import sys

def main():
    # Identity mapper: read from stdin and write to stdout
    for line in sys.stdin:
        print(line.strip())

if __name__ == "__main__":
    main() 