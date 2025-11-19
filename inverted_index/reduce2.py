#!/usr/bin/env python3
"""Reduce 2"""
import sys
import itertools
import math


def reduce_one_group(key, group, num_files):
    """Reduce one group."""
    group_list = list(group)
    num_docs_with_term = len(group_list)
    for g in group_list:
        line = str(g).strip()
        docid, freq = line.partition("\t")[2].split(",")
        idf = math.log10(num_files / (num_docs_with_term))
        print(f"{key}\t{docid},{idf},{freq}")



def keyfunc(line):
    """Return the key from a TAB-delimited key-value pair."""
    return line.partition("\t")[0]


def main():
    """Divide sorted lines into groups that share a key."""
    try:
        with open("total_document_count.txt", "r") as f:
            content = f.read()
            num_files = int(content)
    except Exception as e:
        print(e)
        exit(1)

    for key, group in itertools.groupby(sys.stdin, keyfunc):
        reduce_one_group(key, group, num_files)


if __name__ == "__main__":
    main()
