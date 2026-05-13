#!/bin/bash

search_dir="${1:-.}"

# Standardize skip patterns
# Added 'copy.sh' explicitly to the exclusion list
find "$search_dir" -type f \
    -not -path '*/.*' \
    -not -path "*/debug/*" \
    -not -path "*/__pycache__/*" \
    -not -name "copy.sh" | while read -r file; do

    echo "========================================"
    
    # Improved matching: checks if 'data' is a directory component in the path
    if [[ "$file" =~ (^|/)data/ ]]; then
        echo "FILE (Data - First 50 lines): $file"
        echo "========================================"
        head -n 50 "$file"
    else
        echo "FILE (Full Content): $file"
        echo "========================================"
        cat "$file"
    fi

    echo -e "\n"
done
