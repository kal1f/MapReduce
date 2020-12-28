#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mapper.py"""

import sys
import re

out = {}


def read_stdin():
    readline = sys.stdin.readline()
    while readline:
        yield readline
        readline = sys.stdin.readline()


for line in read_stdin():

    line = line.upper()
    line = re.sub(r'[^A-Z\s]+', '', line)
    line = line.strip()
    words = line.split()

    for word in words:
        if word not in out:
            out[word] = 1
        else:
            out[word] += 1

for _, word in enumerate(out):
    print('%s\t%s' % (word, out[word]))