#!/usr/apps/esg/cdat6.0a/bin/python

# This is a command-line wrapper for the function mv2scratch() in cmip_gc.py.
# Here's what it does:
    """Moves a file in a dirpath under scratch/_gc/ to the corresponding location under scratch/"""

from cmip_gc import mv2scratch

if __name__ == '__main__':
    if len( sys.argv ) > 2:
        # e.g.
        # gc.py /css01-cmip5/scratch/cmip5/output1/LASG-CESS/FGOALS-g2/amip/mon/atmos/Amon/r1i1p1
        # Note that there should be a full directory path down to the ensemble, and no farther.
        # But wildcards are allowed.
        filename = sys.argv[1]
        dirpath = sys.argv[2]
    else:
        print """Usage: mv2scratch.py filename dirpath
        e.g.    mv2scratch.py myfile /css01-cmip5/scratch/_gc/status10/foo/bar/
        is equivalent to
        mv  /css01-cmip5/scratch/_gc/status10/foo/bar/myfile  /css01-cmip5/scratch/status10/foo/bar/myfile
        """

    mv2scratch( filename dirpath )
