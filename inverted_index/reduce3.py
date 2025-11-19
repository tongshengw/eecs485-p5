#!/usr/bin/env python3
"""Reduce 3"""
import sys
import itertools


def reduce_one_group(key, group):
    """Reduce one group."""
    group_arr = list(group)
    for g in group_arr:
        _, values = g.strip().split("\t")
        docid, idf, freq = values.split(",")
        print(f"{docid}\t{key},{freq},{idf}")



def keyfunc(line):
    """Return the key from a TAB-delimited key-value pair."""
    return line.partition("\t")[0]


def main():
    """Divide sorted lines into groups that share a key."""
    for key, group in itertools.groupby(sys.stdin, keyfunc):
        reduce_one_group(key, group)


if __name__ == "__main__":
    main()
