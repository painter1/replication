#!/usr/apps/esg/cdat6.0a/bin/python

# Simple garbage collector for replicated CMIP5 data.
# This moves data to another directory.  Deletion should not happen until someone
# has looked over the purportedly obsolete data.

# Before running this script, do a harvest (ingest) and run a verify to update the replication
# database before running this script.  The reason is that this script is based on comparing a file
# with entries in the database.  In particular, it rejects any data not listed in the database.

# This script requires data to be organized as unpublished data usually is organized on CSS
# at PCMDI.
# That is, <anything>/scratch/<abs_path>/version/variable/file.nc and
# sometimes bad-checksum files in  <anything>/scratch/<abs_path>/version/variable/bad?/file.nc.
# I plan to generalize this as necessary, but no more.

# This could be implemented either by moving bad data or by moving good data.
# There's not a big difference in performance or coding between the two.
# Nevertheless, "moving good data" is superior for two reasons.
# 1. Most important, the "maybe bad" files are out of the regular directory hierarchy,
# where we can look at them without interfering with normal operations.
# 2. It makes it simple to reject files which are in the wrong place, i.e. a directory which
# should have no files.
#
# Thus the first step is to move all the data to another directory.
# Then move it back if it's "good", which means that it's in the database, and the database
# says that we have it (even if incomplete or corrupted).
# Also move anything in the same dataset as a "good" file, but if it's not in the database
# we should remember that problem.
# In the process we will build lists describing the "possibly bad" files.  Some kinds of
# "possibly bad" will suggest that the database needs an update, others that the file
# should be deleted.
# Note that any file in an unexpected location will be "possibly bad".

import os, shutil, glob, sys, re
import sqlalchemy
from esgcet.config import loadConfig
from sqlalchemy import sql
from pprint import pprint

goodfiles = []
badfiles = []
whys = { 'good':0,
         'not in database, but there is no known later version':0,
         'not in database, obsolete, have latest version':0,
         'not in database, obsolete, do not have the latest version':0,
         'we have a later version':0,
         'db status says we do not have it':0,
         'zero length':0
         }

def listgood( filename ):
    global badfiles
    goodfiles.append(filename)
    if filename in badfiles: badfiles.remove(filename)
    whys['good'] += 1

def listbad( filename, why ):
    global badfiles
    if filename not in badfiles: badfiles.append(filename)
    if filename in goodfiles: goodfiles.remove(filename)
    whys[why] += 1

def mv2scratch( filename, dirpath ):
    """Moves a file in a dirpath under scratch/_gc/ to the corresponding location under scratch/"""
    scpath = dirpath.replace('/scratch/_gc/','/scratch/',1)
    oldpath = os.path.join(dirpath,filename)
    #newpath = os.path.join(scpath,filename)
    print "moving from",oldpath,"\nto",scpath
    if not os.path.isdir(scpath):
        os.makedirs(scpath)
    shutil.move( oldpath, scpath )
    listgood( filename )

def mv2trash( filename, dirpath, trashdir, why ):
    """Moves a file in dirpath under scratch/_gc/ to a corresponding location under
    scratch/_gc/trashdir.
    This lets you collect files of the same type of badness in the same place."""
    nwpath = os.path.normpath( dirpath.replace('/scratch/_gc/', '/scratch/_gc/'+trashdir+'/') )
    oldpath = os.path.join(dirpath,filename)
    #newpath = os.path.join(scpath,filename)
    print "moving from",oldpath,"\nto",nwpath
    if not os.path.isdir(nwpath):
        os.makedirs(nwpath)
    shutil.move( oldpath, nwpath )
    listbad( filename, why )

def abspath2vers( abspath ):
    """Extracts the version directory from abspath, e.g. abspath=
    'cmip5/output1/LASG-CESS/FGOALS-g2/amip/mon/atmos/Amon/r1i1p1/v2/rsutcs/rsutcs_Amon_FGOALS-g2_amip_r1i1p1_197901-198812.nc'.
    Does not check whether it's a valid version."""
    fdirs = abspath.split('/')
    if len(fdirs)<=9:
        return None
    else:
        return fdirs[9]

def verskey( verstr ):
    """Returns an integer key for comparing versions, e.g. 20130512 or 3.
    The input is a version string, e.g. 'v20130512', '20130512', 'v3', or '3'.
    There is no checking for whether verstr is a valid version string.
    Note that the key ensures that single-digit version numbers such as '3' will be less
    than standard DRS date-based version numbers, which require eight digits."""
    if verstr[0]=='v':
        vstr = verstr[1:]
    else:
        vstr = verstr
    return int(vstr)    

def existing_versions( filename, abspath, dirpath, engine ):
    """Identifies existing versions of a file for which the file has been verified.
    This file's version and all the versions are returned; the list of all versions is
    sorted.  If there is a problem identifying a version, then the first return value is None.
    This is based on the replication database."""
    vers = abspath2vers(abspath)
    if not check_versiondir(vers):
        print "WARNING: cannot find the version from abspath.  vers=",vers,"fdirs=",fdirs
        return None,[]

    ap_templ = abspath.replace(vers,'%')
    # Do the Python equivalent of
    # SELECT abs_path FROM replica.files WHERE status>=100 AND abs_path LIKE ap_templ;
    sqlst = "SELECT abs_path FROM replica.files WHERE status>=100 AND abs_path LIKE '%s';"%ap_templ
    report = engine.execute(sql.text(sqlst)).fetchall()
    
    verss = [ abspath2vers(ap[0]) for ap in report ]
    verss.sort(reverse=True,key=verskey)
    if all( map(check_versiondir, verss))==True:
        return vers, verss
    else:
        print "WARNING, something didn't look like a version in", verss
        return None,[]

def mvgood2scratch( filename, abspath, dirpath, engine ):
    """Checks whether file identified by abspath is in the database, listed as present
    (status>=20 or <0).
    If so, moves it from a path dirpath containing .../scratch/_gc/... to the corresponding path
    containing .../scratch/...
    Regardless of the database status, any zero-length file will be deleted.
    Returns True if the file was moved, False if it wasn't.
    engine is an SQLAlchemy engine.
    The filename will be put in one of the lists goodfiles or badfiles."""
    filepath = os.path.join(dirpath,filename)
    if os.path.getsize(filepath)==0:
        os.remove(filepath)
        # ...Cleaning nonexistent files out of the database will have to be done anyway.
        # That will be a separate job.
        listbad(filename,"zero length")
        return False

    # Do the Python equivalent of SELECT status FROM replica.files WHERE abs_path=abspath
    sqlst = "SELECT status FROM replica.files WHERE abs_path='%s';"%abspath
    report = engine.execute(sql.text(sqlst)).fetchall()   # should be [(100,)] if status=100 e.g.
    print "abspath=",abspath,"report=",report
    if report==[]:
        why = "not in database, "
        vers = abspath2vers(abspath)
        print abspath,"not found in database, size=",os.path.getsize(filepath),"version=",vers

        # Is another version in the database?
        abspath_anyvers = abspath.replace('/'+vers+'/', '/%/')
        sqlst = "SELECT abs_path,status FROM replica.files WHERE abs_path LIKE '%s';"\
                % abspath_anyvers
        report = engine.execute(sql.text(sqlst)).fetchall()   # e.g. [(path1,status1),...]
        vershvs = [ abspath2vers(r[0]) for r in report if r[1]>=100 or r[1]==30 ]
        versnhvs = [ abspath2vers(r[0]) for r in report if r[1]<30 ]
        vershvs = list(set([v[1:] if v[0]=='v' else v for v in vershvs]))
        versnhvs = list(set([v[1:] if v[0]=='v' else v for v in versnhvs]))
        vershvs.sort(key=verskey)
        versnhvs.sort(key=verskey)
        if len(vershvs)>0 and verskey(vershvs[-1])>verskey(vers):
            print "  Not in database; but we have a later version."
            why += "obsolete, have latest version"
            trashdir = "notdb_hv_latest"
        elif len(versnhvs)>0 and verskey(versnhvs[-1])>verskey(vers):
            print "  Obsolete; we don't have the latest version."
            why += "obsolete, do not have the latest version"
            trashdir = "notdb_donthv_latest"
        else:
            why += "but there is no known later version"
            trashdir = "notdb_no_later"
        print "  versions in database which we have:", vershvs
        print "  versions in database, we do not have:", versnhvs
        mv2trash( filename, dirpath, trashdir, why )
        #listbad(filename, why)

        return False
    status = report[0][0]
    if status>=20 or status<0:
        # It looks like we should keep this.
        vers,verss = existing_versions(filename, abspath, dirpath, engine )
        # Note that existing versions returns verss sorted so the latest is first.
        if vers is not None and len(verss)>0 and verskey(vers)!=verskey(verss[0]):
            print "abspath version",vers,"is older than",verss[0],"which we also have"
            trashdir = "old_hv_latest"
            mv2trash( filename, dirpath, trashdir, "we have a later version" )
            #listbad(filename, "we have a later version")
            return False
        else:
            mv2scratch( filename, dirpath )
            return True
    else:
        print abspath,"status=",status,"\n  size=",os.path.getsize(filepath)
        trashdir = "status10"
        mv2trash( filename, dirpath, trashdir, "db status says we do not have it" )
        #listbad(filename,"db status says we do not have it")
        return False

def gc_mvall( scratchdir ):
    """first step of gc, move all files from /scratch/ to /scratch/_gc/."""
    sdirs = glob.glob(scratchdir)
    for scratchdir in sdirs:
        gcdir = scratchdir.replace( '/scratch/', '/scratch/_gc/' )
        print "scratchdir=",scratchdir
        print "gcdir =",gcdir
        if os.path.isdir(scratchdir):
            if os.path.isdir(gcdir):
                print "WARNING, gcdir %s already exists,\n will not move from scratchdir %s"%\
                      (gcdir,scratchdir)
            else:
                shutil.move( scratchdir, gcdir )
        else:
            #raise Exception("source directory %s doesn't exist"%scratchdir)
            print "WARNING", "source directory %s doesn't exist"%scratchdir
            print "Nothing will be moved from scratch to scratch/_gs."
        if not os.path.isdir(glob.glob(gcdir)[0]):
            raise Exception("gcdir %s doesn't exist"%gcdir)

def gc_mvgood( topdir, gcdir ):
    """second step of gc, move good files from /scratch/_gc/ to /scratch/."""
    config = loadConfig(None)
    engine = sqlalchemy.create_engine(config.get('replication', 'esgcet_db'), echo=False,
                                      pool_recycle=3600)

    # os.walk isn't going to work very well.  I would have to parse the path to identify
    # the abs_path, which encodes the facets and version of the dataset, etc.
    # It's easier to start with those pieces of the path, and stick them together...

    for gcdsdir in glob.glob( gcdir ):
        fac1dir = gcdsdir[ len(os.path.join(topdir,'scratch/_gc/')): ]  # one choice of facets
        # ...gcdsdir is the root directories for the dataset now in .../scratch/_gc/...
        # Below this directory are ones for versions and variables, and possibly bad? directories
        # for files which failed a checksum.
        if fac1dir.endswith("withdrawn"):   # leave this facet directory in _gc, all versions
            # rare, seen for LASG probably it's a name change done by hand
            continue
        versiondirs = os.listdir( gcdsdir )  # should be version directories e.g. v20120913/
        for versd in versiondirs:
            verspath = os.path.join(gcdsdir,versd)
            if not os.path.isdir(verspath): continue
            if not check_versiondir( versd ):
                raise Exception("%s does not look like a version directory"%versd)
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
                # A file was moved back to scratch, others in the same dataset+version should be
                # moved.
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
                                if os.path.isfile(filen):
                                    mv2scratch( filename, dirpath )

def delete_empty_dirs( dirwc ):
    """Clean-up: delete empty directories in dirwc, which may be wildcarded."""
    for dir in glob.glob( dirwc ):
        for dirpath,dirnames,filenames in os.walk(dir, topdown=False):
            try:
                os.rmdir(dirpath)
                # os.rmdir removes a directory only if it's empty
            except OSError:
                pass

def check_versiondir( versd ):
    """Checks whether versd looks like a directory from a version number, e.g.
    2, v2, v20121109, or 20121109.  Returns True or False."""
    if check_drsversiondir(versd):
        return True
    # Not a DRS version number.  Check for '2', 'v2', etc.
    # In practice, such numbers have only a single digit.
    if len(versd)>2:
        return False
    if len(versd)==2 and versd[0]!='v':
        return False
    if len(versd)==2:
        versd = versd[1:]
    matches = re.findall( '^\d$', versd )
    if len(matches)==1:
        return True
    else:
        return False

def check_drsversiondir( versd ):
    """Checks whether versd looks like a directory from a DRS version number, e.g.
    v20121109 or 20121109.  Returns True or False."""
    if len(versd)<8 or len(versd)>9:
        return False
    if len(versd)==9 and versd[0]!='v':
        return False
    if len(versd)==9:
        versd = versd[1:]
    matches = re.findall( '^201\d[0,1]\d[0,1,2,3]\d$', versd )
    if len(matches)==1:
        return True
    else:
        return False

def check_facetsdir( topdir, facetsdir ):
    """Checks whether facetsdir is like what we're expecting.
    Prints out all the source scratch dirs, and requires confirmation from the user.
    Returns a list of the source scratch directories."""

    scratchdir = os.path.join(topdir,'scratch/',facetsdir)
    gcdir = os.path.join(topdir,'scratch/_gc/',facetsdir)
    sdirs = glob.glob(scratchdir)
    gdirs = glob.glob(gcdir)
    if len(sdirs)==0:
        if len(gdirs)==0:
            raise Exception(
                "There is no source directory matching %s\n and no target directory matching %s."%
                (scratchdir,gcdir) )
        else:
            print "WARNING: There is no source directory matching %s."%scratchdir
            print "   Nothing will be moved from scratch/ to scratch/_gc/,"
            print "but we will try to move files the other way."
    else:
        # As a sanity check, does the last facet look like an ensemble, e.g. r1i12p2 ?
        for sdir in sdirs: # e.g. sdir='/cmip5/scratch/cmip5/prod/inst/mod/exp/mon/atmos/Amon/r1i1p1
            topdf = topdir.split('/')  # If topdir='/cmip5', topdf=['','cmip5']
            facets = sdir.split('/')[1+len(topdf):] # The 1+ is for 'scratch/'
            if len(facets)!=9:
                raise Exception("should have 9 facets, have %i in %s"%(len(facets),facets))
            ensfacet = facets[-1]
            matches = re.findall( 'r\d+i\d+p\d+', ensfacet )  # e.g. ['r1i12p2']
            if len(matches)!=1 or matches[0]!=ensfacet:
                matches = re.findall( 'r\d+i\d+p\d+.*withdrawn', ensfacet )  # e.g. ['r1i12p2']
                if len(matches)==0:
                    raise Exception("%s should be an ensemble facet, doesn't look like one"%
                                    ensfacet)

    print "Data in the following directories will be cleaned out, with possibly-bad files put"
    print "  in a temporary .../scratch/_gc/... directory:"
    pprint( sdirs )
    if len(gdirs)>0:
        print "These _gc directories already exist, and any good files in them will be moved to"
        print "regular scratch directories:"
        pprint( gdirs )
        print '\n'
    if len(sdirs)<10 and len(gdirs)<10:
        # An interactive check is nice, but less helpful if there are lots of datasets.
        # It's worse than useless if running under something else (e.g. nohup) but that happens
        # mainly with a large collection of datasets.
        ok = raw_input("Is this ok? (Type y or n, and newline)")
        if ok[0]!='y' and ok[0]!='Y':
            raise Exception("Aborted by user.")
    return sdirs

def gc( topdir, facetsdir ):
    global badfiles
    print "Entering CMIP gc with topdir=",topdir,"and"
    print "  facetsdir=",facetsdir
    sdirs = check_facetsdir(topdir,facetsdir)
    scratchdir = os.path.join(topdir,'scratch/',facetsdir)
    gcdir = os.path.join(topdir,'scratch/_gc/',facetsdir)

    # This doesn't work because glob.glob() hasn't been called on scratchdir.
    # It's also not necessary except for files which aren't in the expected places.
    for sdir in sdirs:
        for root, directories, filenames in os.walk(sdir):
            badfiles += filenames
    print "jfp initially, badfiles=",
    pprint(badfiles)
    print "jfp number of badfiles=",len(badfiles),"\n"

    gc_mvall( scratchdir )
    gc_mvgood( topdir, gcdir )
    delete_empty_dirs( os.path.join(topdir,'scratch/_gc/') )
    print len(goodfiles), "good files, in /scratch/:"
    pprint( goodfiles )
    print len(badfiles), "bad files, in /scratch/_gc/:"
    pprint( badfiles )
    print len(goodfiles),"good files; ",len(badfiles),"bad files."
    print "reasons for files being good or bad:"
    pprint( whys )

            
if __name__ == '__main__':
    if len( sys.argv ) > 1:
        # e.g.
        # gc.py /css01-cmip5/scratch/cmip5/output1/LASG-CESS/FGOALS-g2/amip/mon/atmos/Amon/r1i1p1
        # Note that there should be a full directory path down to the ensemble, and no farther.
        # But wildcards are allowed.
        srcpath = sys.argv[1]
        scratchloc = srcpath.find('/scratch/')
        topdir = srcpath[0:scratchloc]
        facetsdir = srcpath[scratchloc+9:]
    else:
        print "running test problem"
        print "If you do not want that, you should provide a source path, from the root to the"
        print "ensemble directory.  After /scratch/, * wildcards are allowed"
        topdir = '/css01-cmip5/'
        facets = [ 'cmip5', 'output1', 'LASG-CESS', 'FGOALS-g2', 'amip', 'mon', 'atmos', 'Amon', 'r1i1p1']
        facetsdir = apply( os.path.join, facets )
        # This example contains some but little data, hence a good starting point for testing
        # The length of facets should be the same as in this example, but a facet may be wildcarded
        # with '*'.
        # ... e.g. facetsdir = 'cmip5/output1/LASG-CESS/FGOALS-g2/amip/mon/atmos/Amon/r1i1p1/'

    gc( topdir, facetsdir )
