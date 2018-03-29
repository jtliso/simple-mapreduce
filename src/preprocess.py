# J.T

import sys

with open(sys.argv[1], 'r') as f:
    data = f.readlines()

with open(sys.argv[1], 'w') as f:
    for (linenum, line) in enumerate(data):
        f.write('%d  %s' % (linenum + 1, line))
