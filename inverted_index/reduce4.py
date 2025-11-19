#!/usr/bin/env python3
"""Reduce 4"""
import sys
import itertools
import math


def reduce_one_group(key, group):
    """Reduce one group."""
    group_arr = list(group)
    normalisation_sq = 0
    for g in group_arr:
        _, values = g.strip().split("\t")
        term, tf, idf = values.split(",")
        normalisation_sq += (int(tf) * float(idf)) ** 2

    for g in group_arr:
        _, values = g.strip().split("\t")
        term, tf, idf = values.split(",")
        print(f"{int(key) % 3}\t{term},{key},{tf},{idf},{math.sqrt(normalisation_sq)}")



def keyfunc(line):
    """Return the key from a TAB-delimited key-value pair."""
    return line.partition("\t")[0]


def main():
    """Divide sorted lines into groups that share a key."""
    for key, group in itertools.groupby(sys.stdin, keyfunc):
        reduce_one_group(key, group)


if __name__ == "__main__":
    main()
