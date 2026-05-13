import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from services.api_service import get_authors_from_api
from config.settings import DATA_DIR

def main():
    if len(sys.argv) != 2:
        print("Usage: python get_authors.py <number_of_authors>")
        sys.exit(1)
    
    num_authors = int(sys.argv[1])
    
    if get_authors_from_api(num_authors):
        print(f"Successfully processed {num_authors} authors")
    else:
        print("Failed to process authors")
        sys.exit(1)

if __name__ == "__main__":
    main()

