#!/usr/apps/esg/cdat6.0a/bin/python

# This is a command-line wrapper for the function mv2scratch() in cmip_gc.py.
# Here's what it does:
#    """Moves a file in a dirpath under scratch/_gc/ to the corresponding location under scratch/"""
# I expect globbing to be done by the shell, not by me.

import sys, os
from cmip_gc import mv2scratch

if __name__ == '__main__':
    if len( sys.argv ) > 1:
        # e.g.
        # gc.py /css01-cmip5/scratch/cmip5/output1/LASG-CESS/FGOALS-g2/amip/mon/atmos/Amon/r1i1p1
        # Note that there should be a full directory path down to the ensemble, and no farther.
        # But wildcards are allowed.
        filenames = sys.argv[1:]
    else:
        print """Usage: mv2scratch.py filenames
        e.g.    mv2scratch.py myfile1 myfile2
        is, if the $PWD==/css01-cmip5/scratch/_gc/status10/foo/bar/, equivalent to:
        mv  /css01-cmip5/scratch/_gc/status10/foo/bar/myfile1  /css01-cmip5/scratch/status10/foo/bar/myfile1
        mv  /css01-cmip5/scratch/_gc/status10/foo/bar/myfile2  /css01-cmip5/scratch/status10/foo/bar/myfile2
        """

    for filename in filenames:
        if os.path.isfile(filename):
            dirname = os.path.dirname( os.path.abspath( filename ) )
            mv2scratch( filename, dirname )
        else:
            print "not a file, won't move", filename
