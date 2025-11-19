#!/usr/bin/env python3
"""Reduce 5"""
import sys
import itertools


from collections import defaultdict

def reduce_one_group(key, group):
    """Reduce one group."""
    group_arr = list(group)
    # print(group_arr)
    term_dict = defaultdict(list)
    term_idf = {}
    for g in group_arr:
        _, values = g.strip().split("\t")
        term, docid, tf, idf, norm = values.split(",")
        term_idf[term] = idf
        term_dict[term].append((docid, tf, norm))
    for term_arr in term_dict.values():
        term_arr.sort()

    for term, values in sorted(term_dict.items()):
        idf = term_idf[term]
        output_str = f"{term} {idf} "
        for docid, tf, norm in values:
            output_str += f"{docid} {tf} {norm} "
        print(output_str.strip())



def keyfunc(line):
    """Return the key from a TAB-delimited key-value pair."""
    return line.partition("\t")[0]


def main():
    """Divide sorted lines into groups that share a key."""
    for key, group in itertools.groupby(sys.stdin, keyfunc):
        reduce_one_group(key, group)


if __name__ == "__main__":
    main()
