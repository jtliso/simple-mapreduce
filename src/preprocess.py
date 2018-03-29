# J.T Liso and Sean Whalen
# COSC 560 Programming Assignment 2
# Preprocessing module to add line numbers to a file

import sys
import os

if(len(sys.argv) != 2):
  print "USAGE: python preprocessing.py directory"

files = []

#get the file names in the directory
for dirname, _, filename in os.walk(sys.argv[1]):
  for f in filename:
    files.append(os.path.join(dirname, f))

for fname in files:
  # read in the file
  with open(fname, 'r') as f:
      data = f.readlines()

  # add the line numbers to the file
  with open(fname, 'w') as f:
      for (linenum, line) in enumerate(data):
          f.write('%d  %s' % (linenum + 1, line))
