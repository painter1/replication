# Simple garbage collector for replicated CMIP5 data.
# This moves data to another directory.  Deletion should not happen until someone
# has looked over the purportedly obsolete data.

# This could be implemented either by moving bad data or by moving good data.
# There's not a big difference in performance or coding between the two.
# I decided to do it by moving good data because there is a kind of status propagation which
# may turn out to be easier to implement this way.  If anything in a dataset is "good",
# i.e. in the database, then probably everything else in the dataset should be made good, i.e.
# added to the dataset (But rarely a file may have been withdrawn without updating the
# dataset's version number.)
# Thus the first step is to move all the data to another directory.
# Then move it back if it's "good", which means that it's in the database, and the database
# says that we have it (even if incomplete or corrupted).
# Also move anything in the same dataset as a "good" file, but if it's not in the database
# we should remember that problem.
# In the process we will build lists describing the "possibly bad" files.  Some kinds of
# "possibly bad" will suggest that the database needs an update, others that the file
# should be deleted.
# Note that any file in an unexpected location will be "possibly bad".

import os, shutil, glob
import sqlalchemy
from esgcet.config import loadConfig
from sqlalchemy import sql

def mv2scratch( filename, dirpath ):
    """Moves a file in a dirpath under scratch/_gc/ to the corresponding location under scratch/"""
    scpath = dirpath.replace('/scratch/_gc/','/scratch/',1)
    oldpath = os.path.join(dirpath,filename)
    #newpath = os.path.join(scpath,filename)
    print "moving from",oldpath,"\nto",scpath
    if not os.path.isdir(scpath):
        os.makedirs(scpath)
    shutil.move( oldpath, scpath )

def mvgood2scratch( filename, abspath, dirpath, engine ):
    """Checks whether file identified by abspath is in the database, listed as present
    (status>=20 or <0).
    If so, moves it from a path dirpath containing .../scratch/_gc/... to the corresponding path
    containing .../scratch/...
    Regardless of the database status, any zero-length file will be deleted.
    Returns True if the file was moved, False if it wasn't.
    engine is an SQLAlchemy engine."""
    filepath = os.path.join(dirpath,filename)
    if os.path.getsize(filepath)==0:
        os.remove(filepath)
        # ...Cleaning nonexistent files out of the database will have to be done anyway.
        # That will be a separate job.

    # Do the Python equivalent of SELECT status FROM replica.files WHERE abs_path=abspath
    sqlst = "SELECT status FROM replica.files WHERE abs_path='%s';"%abspath
    report = engine.execute(sql.text(sqlst)).fetchall()   # should be [(100,)] if status=100 e.g.
    print "abspath=",abspath,"report=",report
    if report==[]:
        print "not found in database"
        return False
    status = report[0][0]
    if status>=20 or status<0: # normal
    #if True:  # testing
        mv2scratch( filename, dirpath )
        return True
    else:
        return False

topdir = '/css01-cmip5/'
facets = [ 'cmip5', 'output1', 'LASG-CESS', 'FGOALS-g2', 'amip', 'mon', 'atmos', 'Amon', 'r1i1p1']
facetsdir = apply( os.path.join, facets )
# This example contains some but little data, hence a good starting point for testing
# The length of facets should be the same as in this example, but a facet may be wildcarded with '*'.
# ... e.g. facetsdir = 'cmip5/output1/LASG-CESS/FGOALS-g2/amip/mon/atmos/Amon/r1i1p1/'

scratchdir = os.path.join(topdir,'scratch/',facetsdir)
gcdir = os.path.join(topdir,'scratch/_gc/',facetsdir)

print "scratchdir=",scratchdir
print "gcdir=",gcdir
if os.path.isdir(scratchdir):
    if os.path.isdir(gcdir):
        print "WARNING, gcdir %s already exists,\n will not move from scratchdir %s"%\
              (gcdir,scratchdir)
    else:
        shutil.move( scratchdir, gcdir )
else:
    raise Exception("source directory %s doesn't exist"%scratchdir)
if not os.path.isdir(gcdir):
    raise Exception("gcdir %s doesn't exist"%gcdir)

config = loadConfig(None)
engine = sqlalchemy.create_engine(config.get('replication', 'esgcet_db'), echo=False, pool_recycle=3600)

# os.walk isn't going to work very well.  I would have to parse the path to identify
# the dataset.  It's better to start by identifing the datasets...

for gcdsdir in glob.glob( gcdir ):
    fac1dir = gcdsdir[ len(os.path.join(topdir,'scratch/_gc/')): ]  # one choice of facets
    # ...gcdsdir is the root directories for the dataset now in .../scratch/_gc/...
    # Below this directory are ones for versions and variables, and possibly bad? directories
    # for files which failed a checksum.
    versiondirs = os.listdir( gcdsdir )  # should be version directories e.g. v20120913/
    for versd in versiondirs:
        verspath = os.path.join(gcdsdir,versd)
        if not os.path.isdir(verspath): continue
        vardirs = os.listdir(verspath)
        mvstatus = False  # True if any file in this dataset+version should be moved
                          # back to scratch/.
        for vard in vardirs:
            varpath = os.path.join(verspath,vard)
            dirpath = varpath
            if not os.path.isdir(varpath): continue
            filenames = os.listdir(varpath) # mostly files, may also have bad? directories
            for filename in filenames:
                filep = os.path.join(varpath,filename)
                if os.path.isfile(filep):
                    abspath = os.path.join( fac1dir, versd, vard, filename )
                    mvstatus = mvstatus or mvgood2scratch( filename, abspath, dirpath, engine )
        if mvstatus is True:
            # A file was moved back to scratch, others in the same dataset+version should be moved.
            for vard in vardirs:
                varpath = os.path.join(verspath,vard)
                dirpath = varpath
                if not os.path.isdir(varpath): continue
                filenames = os.listdir(varpath) # mostly files, may also have bad? directories
                for filename in filenames:
                    filep = os.path.join(varpath,filename)
                    if os.path.isfile(filep):
                        mv2scratch( filename, dirpath )
                    if os.path.isdir(filep) and filep.find('bad')==0:
                        for filen in os.listdir(filep):
                            if os.path.isfile(filep):
                                mv2scratch( filename, dirpath )
# Clean up
for gcdsdir in glob.glob( gcdir ):
    for dirpath,dirnames,filenames in os.walk(gcdsdir, topdown=False):
        try:
            os.rmdir(dirpath)
            # os.rmdir removes a directory only if it's empty
        except OSERROR:
            pass

            
