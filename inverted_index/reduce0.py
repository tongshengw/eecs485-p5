#!/usr/bin/env python3
"""Reduce 0."""
import sys

res = 0
for line in sys.stdin:
    if "page_count" in line:
        res += 1
print(res)
