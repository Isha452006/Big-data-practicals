#!/usr/bin/env python3
import sys

# Read each line from input
for line in sys.stdin:
    line = line.strip()
    words = line.split()
    # Output each word with count 1
    for word in words:
        print(f"{word}\t1")
