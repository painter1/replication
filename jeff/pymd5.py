#!/usr/apps/esg/cdat6.0a/bin/python

import sys
import hashlib

def md5( filename ):
    # returns the file's md5 checksum in hex format.
    # For performance on big files, this reads the file one chunk at a time.
    # based on something on the Web, tuned for large files on gdo2.
    # Note (jfp 2012.01.24): on a large test file, this (and wrapper code) took
    # 3.6 seconds, but /opt/sfw/bin/md5sum took 10.0 seconds.  As the core code of
    # each is probably written in C, this probably means md5sum does Sun I/O poorly.
    md5 = hashlib.md5()
    with open(filename,'rb') as f: 
        for chunk in iter(lambda: f.read(256*md5.block_size), ''): 
            md5.update(chunk)
    return md5.hexdigest()

if __name__ == '__main__':
    if len( sys.argv ) > 1:
        file = sys.argv[1]
        print md5( file ), file
    else:
        print "please provide a filename"

