#!/usr/bin/python

'''
J.T. Liso and Sean Whalen
COSC 560 Programming Assignment 2
31 March 2018
'''

import sys

total = 0

for line in sys.stdin:
	word, count = line.split()
	total += count

	print word, total
