#!/usr/bin/env python3
"""Reduce 1"""
import sys
import itertools

from collections import Counter

def reduce_one_group(key, group):
    """Reduce one group."""
    # one unique document id
    group_item = next(group)
    value = str(group_item).partition("\t")[2]

    g_arr = str(value).split()

    term_counter = Counter(g_arr)
    for term, freq in term_counter.items():
        print(f"{term}\t{key},{freq}")



def keyfunc(line):
    """Return the key from a TAB-delimited key-value pair."""
    return line.partition("\t")[0]


def main():
    """Divide sorted lines into groups that share a key."""
    for key, group in itertools.groupby(sys.stdin, keyfunc):
        reduce_one_group(key, group)


if __name__ == "__main__":
    main()
