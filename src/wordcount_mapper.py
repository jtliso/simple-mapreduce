#!/usr/bin/python

'''
J.T. Liso and Sean Whalen
COSC 560 Programming Assignment 2
31 March 2018
'''

import sys

for l in sys.stdin:
	line =  l.split()
	
	for word in line:
		word = word.lower().strip()
		print word, 1
