#!/usr/local/cdat/bin/python

# Run esgquery_index and use it generate a download list compatible with Estani's Download.py
# To run this as a standalone program from a command line, first edit facets_default and the
# default arguments to download_list below, and provide one command-line option, statusfile.

import subprocess, pprint, sys
from esgcet.config import loadConfig
from string import lstrip,rstrip
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, Boolean, DateTime, sql, ForeignKeyConstraint, orm

# List of pairs institute names.  In each pair, the first is the form used in the P2P system
# and the second is the form used in the gateway system.
institutepgl = [('ANL', 'ANL'), ('BCC', 'BCC'), ('CCCMA', 'CCCma'), ('CNES', 'CNES'),
                ('CNRM-CERFACS', 'CNRM-CERFACS'), ('COLA-CFS', 'COLA-CFS'), ('ICHEC', 'ICHEC'),
                ('INM', 'INM'), ('IPSL', 'IPSL'), ('LASF-CESS', 'LASG-CESS'), ('LASG-IAP', 'LASG-IAP'),
                ('LLNL', 'LLNL'), ('MIROC', 'MIROC'), ('MOHC', 'MOHC'), ('MPI-M', 'MPI-M'), ('MRI', 'MRI'),
                ('NASA GISS', 'NASA-GISS'), ('NASA GMAO', 'NASA-GMAO'), ('NASA GSFC', 'NASA-GSFC'),
                ('NASA JPL', 'NASA-JPL'), ('NCAR', 'NCAR'), ('NCC', 'NCC'), ('NOAA GFDL', 'NOAA-GFDL'),
                ('NOAA-NCEP', 'NOAA-NCEP'), ('PNNL', 'PNNL'), ('REMSS', 'REMSS')]
# Use this list to build dictionaries used to guarantee that an institute name will be in the proper form.
institute_p = {}
institute_g = {}
for p,g in institutepgl:
    institute_g[g] = g
    institute_g[p] = g
    institute_p[g] = p
    institute_p[p] = p

def dataset2facets( dset ):
    """from the name of a dataset, e.g. 'cmip5.output1.IPSL.IPSL-CM5A-LR.rcp45.mon.ocean.Omon.r1i1p1'
    this function computes and returns a facet dictionary, e.g. fd['project']='cmip5'."""
    facets = dset.split('.')
    if len(facets)!=9:
        raise Exception("unexpected dataset format; expect 9 items separated by periods")
    fd={}
    fd['project'] = facets[0]
    fd['product'] = facets[1]
    fd['institute'] = facets[2]
    fd['model'] = facets[3]
    fd['experiment'] = facets[4]
    fd['time_frequency'] = facets[5]
    fd['realm'] = facets[6]
    fd['cmor_table'] = facets[7]
    fd['ensemble'] = facets[8]
    return fd

def facets2dataset( facets ):
    """from a dictionary of facets, e.g. facets['project']='cmip5', this function computes and
    returns the corresponding dataset name"""

    # Put the institute and model names in their gateway forms.
    # For example, the replication database wants CCCma, not CCCMA; and never is a space acceptable.
    institute = facets['institute']
    if institute in institute_g.keys():
        institute = institute_g[institute]
    model = facets['model']
    if model=='INM-CM4' or model=='inm-cm4':
        model = inmcm4

    facetl = [ facets['project'].lower(), facets['product'], institute, model,\
               facets['experiment'], facets['time_frequency'], facets['realm'], facets['cmor_table'],\
               facets['ensemble'] ]
    return '.'.join(facetl)
    


# The facets to which we shall restrict searches; for SQL compatibility use '%' for 'not specified':
facets_default = { 'project':'CMIP5', 'product':'output1', 'institute':'NOAA GFDL', 'model':'GFDL-ESM2M',\
           'experiment':'historical', 'time_frequency':'3hr', 'realm':'atmos', 'cmor_table':'3hr',\
           'ensemble':'r1i1p1' }

def download_list( facets=facets_default, downloadlist="download-mon", tempfile="esg.tmp",\
                   reuse_tempfile=False, forcedl=False,\
                   statusfile=None, head='/cmip5/scratch', serviceurl="", limit=0 ):
    """generates a download list suitable for Estani's Download.py.
    Inputs are: facets, a dictionary of 9 facets which specify the datasets to be downloaded,
    (You can generate facets with the function dataset2facets)
    downloadlist, name of the file to write to,
    tmpfile, name of a temporary file (used for output from esgquery_index)
    (for debugging purposes you can set reuse_tempfile=True to use a file previously generated)
    forcedl, set True to force download even of files we already have
    statusfile, name of a file to which warnings and debugging information are written; defaults to stdout
    head, the first part of the download path (after which is the abs_path of the replica database)
    limit, the maximum number of files to process; 0 for no limit (mainly for testing)
    serviceurl, the argument of esgquery_index
    """
    if statusfile==None: statusfile = sys.stdout
    else: statusfile = open(statusfile,'a+')

    # 
    # Put the institute name is in its P2P form for esgquery_index.
    # For example, esgquery_index wants CCCMA, not CCCma; and sometimes a space rather than a hyphen.
    institute = facets['institute']
    if institute in institute_p.keys():
        institute = institute_p[institute]
    facets['institute'] = institute
    # Also fix the project name - it's CMIP5 for the P2P system, cmip5 for the gateway system.
    facets['project'] = facets['project'].upper()   # same for cmip5/CMIP5
    # Also, in the one case I know of where this is a problem, put the model in its P2P form:
    if facets['model']=='inmcm4':
        facets['model'] = 'INM-CM4'

    # The fields to get with esgquery:
    fields = facets.keys()+[ 'url', 'latest', 'variable', 'title', 'size', 'checksum', 'checksum_type' ]

    # ...note about versions: the version field (when you do esgquery_index on a file) is file_version,
    # not the dataset version which is normally meant by "version".  We can get the dataset version by
    # extgracting the dataset_id field for the file, then calling
    # "esgquery_index -q id=<dataset_id> --fields version -p".
    # But it will be simpler to extract the dataset version from a file_id, as is done below.

    pp = pprint.PrettyPrinter(stream=statusfile)
    if not reuse_tempfile:
        ffiles = open(tempfile,'w')

        arg1= "-q "+ ','.join([ i+"='"+facets[i]+"'" for i in facets.keys() if facets[i] is not '%' ])
        arg2="--fields "+','.join(fields)+" --type f -p"
        arg3 = "" # various options
        if len(serviceurl)>0:
            arg3 = arg3 + " --serviceurl "+serviceurl
        if limit>0:
            arg3 = arg3 + " --limit %d"%(limit)
        subprocess.call(['echo','esgquery_index',arg1,arg2,arg3],stdout=ffiles)
        print 'esgquery_index'+' '+arg1+' '+arg2+' '+arg3
        subprocess.call(['esgquery_index'+' '+arg1+' '+arg2+' '+arg3],stdout=ffiles,shell=True)
        ffiles.close()
    ffiles = open(tempfile)
    dll = []  # each list entry will be a dictionary, download information for one file
    # Thus I'm assuming that esgquery_index is sorted by the file id.  If this ever turns out not
    # to be true, I can sort it or make dll a dict.
    fileid = None
    for line in ffiles:
        cols=line.split('|')
        # len(cols)=1:header or footer.  otherwise,usually:
        # cols[0]='',cols[1]=dataset.file,cols[2]=host,cols[3]=field,cols[4:len-1]=value,cols[len-1]=garbage
        # The original id (first column) is cols[1]+'.'+cols[2].
        if len(cols)<6: continue   # probably 6 columns is real; anything less means header or footer
        if cols[1]!=fileid:    # first hit on this file.
            fileid = cols[1]
            fd = {'fileid':lstrip(rstrip(fileid))}
            dll.append(fd)
        field = lstrip(rstrip(cols[3]))
        fd[field] = [ lstrip(rstrip(val)) for val in cols[4:len(cols)-1] ]

    # pp.pprint(dll)

    fdllist = open(downloadlist,'w')
    dllist = []
    for fd in dll:
        # Form this file's line in download list, first separately getting its fields (columns).
        # Don't bother if this isn't the latest version of the file:
        if fd['latest'][0]!='true':
            # print "file",fd['fileid'],"is not the latest"
            continue
        out0 = fd['url'][0]
        out2 = fd['size'][0]
        out3 = fd.get( 'checksum', ['DUMMY'] )[0]
        out4 = fd.get( 'checksum_type', ['md5'] )[0].lower()
        # When we write to a file, out1 will be the target path, of the form  head/project/product/institute/...
        # But for now, out2 will be abs_path, of the form project/product/institute/...
        # ...institute/model/experiment/time_frequency/realm/cmor_table/ensemble/<version>/variable/title
        # To get the version, take advantage of the fact that the fileid always begins with the same fields,
        # project.product.institute.model.experiment.time_frequency.realm.cmor_table.ensemble.version.filename:
        version = fd['fileid'].split('.')[9]
        institute = fd['institute'][0]
        # Put the institute name is in its gateway form.
        if institute in institute_g.keys():
            institute = institute_g[institute]
        model = fd['model'][0]
        if model=="INM-CM4" or model=="inm-cm4":
            model = "inmcm4"
        out1 = '/'.join( [ fd['project'][0].lower(), fd['product'][0], institute, fd['model'][0],\
                           fd['experiment'][0], fd['time_frequency'][0], fd['realm'][0], fd['cmor_table'][0],\
                           fd['ensemble'][0], version, fd['variable'][0], fd['title'][0] ] )
        dllist.append([ out0, out1, out2, out3, out4 ]) 
        # fdllist.write('\t'.join([ out0, head+'/'+out1, out2, out3, out4 ])+'\n')
    

    # At this point we have in dllist a download list, constructed from the output of esgquery_index
    # (i.e. the P2P system).
    # Now we shall to compare it with files known to the replication database.  If we already have the
    # file, don't get it again.  If we have the exact same file stored under a different version number
    # (very common), re-use it.  If the file _isn't_ in the database, issue a warning and don't get it
    # (because we wouldn't be able to keep track of it after downloading it).

    config = loadConfig(None)
    engine = sqlalchemy.create_engine(config.get('replication', 'esgcet_db'), echo=False, pool_recycle=3600)

    # files0 is the files we want but already have, expresses as an abs_path; from the replication database.
    # Note:  this could be merged with the check for older files - would save a database access
    # but increase code complexity a little.
    # Note: unfortunately, esgquery_index and postgres seem to do output sorting and hence limits
    # a little differently.
    dstr = facets2dataset(facets)
    if not forcedl:
        sql0 = "SELECT abs_path FROM replica.files WHERE dataset_name LIKE '"+dstr+"' AND status>=30;"
        files0 = engine.execute( sql.text( sql0 ) ).fetchall()
        files0 = [ f[0] for f in files0 ]
        # pp.pprint "From %s files0=\n"%(sql0)
        # pp.pprint(files0)
        # Of couse, we don't want files we already have, so take them out of the download list:
        # Making a set out of files0 should convert the deletion process from O(n^2) to O(n), with
        # bigger constants; I'm not sure whether it's worth it.  Usually 1,000<n<100,000.
        # Also, the database and esgquery_index have different naming conventions, e.g. institute
        # CCCma/CCCMA so the comparison has to be made case-insensitive (at least; but I believe that
        # urls and file paths derived from the P2P system will hyphenate the same way).
        sfiles0 = set([f.lower() for f in files0])
        ldllist0 = len(dllist)
        dllist = [ row for row in dllist if row[1].lower().replace('inm-cm4','inmcm4') not in sfiles0 ]
        #...if there are any more mismatch cases not handled by lower(), then I'll have to do
        # a more complicated fix - break up into facets, subsitute with tables, then recombine.
        statusfile.write("dllist reduced from %d to %d lines by removing files we already have\n"%\
                         (ldllist0, len(dllist)) )

    # files1 is the files we want and don't have; from the replication database.
    # It should correspond to the download list, but probably doesn't because they're based on different
    # harvests.
    # I don't want to deal with files which are missing from the database.  Rather than try
    # to fix the database, we'll take them out of the download list too.
    if forcedl:
        sql1 = "SELECT abs_path FROM replica.files WHERE dataset_name LIKE '"+dstr+"';"
    else:
        sql1 = "SELECT abs_path FROM replica.files WHERE dataset_name LIKE '"+dstr+"' AND status<30;"
    print sql1
    files1 = engine.execute( sql.text( sql1 ) ).fetchall()
    files1 = [ f[0] for f in files1 ]
    # pp.pprint "From %s files1=\n"%(sql0)
    # pp.pprint(files1)
    sfiles1 = set([f.lower() for f in files1])
    ldllist0 = len(dllist)
    # pp.pprint(dllist)
    print [ row[1].lower().replace('inm-cm4','inmcm4') for row in dllist ][0]
    dllist2 = [ row for row in dllist if row[1].lower().replace('inm-cm4','inmcm4') in sfiles1 ]
    #...if there are any more mismatch cases not handled by lower(), then I'll have to do
    # a more complicated fix - break up into facets, subsitute with tables, then recombine.
    statusfile.write(("dllist reduced from %d to %d lines by removing files not known to the replication"+\
                     " database.\n")%(ldllist0, len(dllist2)) )
    if len(dllist2)<ldllist0:
        statusfile.write( "WARNING: This change discards the following download list files.\n" )
        statusfile.write( "Maybe it's time for another harvest!\n" )
        if statusfile!=sys.stdout:  # don't write too much to the screen
            pp.pprint( [ row[1] for row in dllist if row[1].lower().replace('inm-cm4','inmcm4') not in sfiles1 ] )
            #...if there are any more mismatch cases not handled by lower(), then I'll have to do
    # a more complicated fix - break up into facets, subsitute with tables, then recombine.
        else:
            statusfile.write("(filenames not printed)\n")
    dllist = dllist2

    # If there be no output limits, and the relevant harvests up-to-date, then there should be a
    # 1:1 correspondence between files1 and dllist (because no file should appear twice in either one).
    # So check for that.
    if limit<=0:
        if len(files1)!=len(dllist):
            statusfile.write("WARNING: esgquery and database produced different numbers of files to download!")
            statusfile.write(" esgquery: %d;  database: %d\n"%( len(dllist), len(files1) ) )
            if statusfile!=sys.stdout:
                print "WARNING: esgquery and database produced different numbers of files to download!",\
                      len(dllist), len(files1)
    # _Maybe_ these sorts will help in finding the row efficiently in the older-version search below;
    # this needs investigation if it matters.
    files1.sort( key=( lambda i: i.lower() ) )
    dllist.sort( key=( lambda i: i[1].lower() ) )

    # files2 is the same as files1 but with the SQL wildcard % in place of the version
    # example of the following:
    # f = cmip5/output1/CCCma/CanCM4/decadal2008/mon/atmos/Amon/r1i1p1/v20111027/cl/cl_etc.nc
    # fs=[cmip5,output1,CCCma,CanCM4,decadal2008,mon,atmos,Amon,r1i1p1,v20111027,cl,cl_etc.nc]
    # fs=[cmip5,output1,CCCma,CanCM4,decadal2008,mon,atmos,Amon,r1i1p1,%,cl,cl_etc.nc]
    # g = cmip5/output1/CCCma/CanCM4/decadal2008/mon/atmos/Amon/r1i1p1/%/cl/cl_etc.nc
    files2=files1[:]
    for i in range(len(files2)):
        fs = files1[i].split('/')
        fs[9] = '%'
        files2[i] = '/'.join(fs)
    # pp.pprint "From %s files2=\n"%(sql0)
    # pp.pprint(files2)

    # Now look for older versions of each file in dllist/files1/files2:
    nnomatch = 0
    for i in range(len(files2)):
        fil2 = files2[i]
        fil1 = files1[i]
        sql2 = "SELECT abs_path,checksum,checksum_type FROM replica.files WHERE abs_path LIKE '"+fil2+\
               "' AND status>=30;"
        hvf = engine.execute( sql.text( sql2 ) ).fetchall()   # list of (abs_path,checksum,checksum_type)
        for fi in hvf:
            # If abs_path is in the download list, this is a file we have which is the same as a file we
            # want, other than version number.  If the checksum matches, we don't have to download -
            # just copy from one version's directory to the new version's directory.
            # Of course, don't bother to do anything if the dllist already refers to another local copy.

            row = next((r for r in dllist if r[1]==fil1),None)  # the row which matches fil1; None if no match
            # ...the above use of a generator expression will only search until the row is found.
            # Thanks to ionous blog: http://dev.ionous.net/2009/01/python-find-item-in-list.html
            # >>> I would like it to start at the previous match; that will usually get the next
            # >>> match in just one try if the lists are sorted first; look into this later.
            if row==None:
                # The database has a file, abs_path==fil1, which the P2P system (i.e. dllist) doesn't
                # know about.  That is, the P2P and gateway systems are inconsistent with one another.
                # This shouldn't happen, but often does...
                if statusfile!=sys.stdout:  # don't write too much to the screen!
                    statusfile.write( "WARNING, can't find match for database file %s\n"%(fil1) )
                    nnomatch = nnomatch+1
                continue
            statusfile.write( fil1+'\n' )
            pp.pprint( row )
            statusfile.write( fi[0]+'\n' )
            if fi[1].upper()==row[3].upper() and fi[1].upper()!='DUMMY' and\
                   fi[2].lower()==row[4].lower() and row[0].find("file")!=0:
                # checksums match, aren't "DUMMY", so change the download url to do a local copy
                # >>>> for the moment, assume that we know where the file is.<<<<
                # >>>> later, check that it's here, and if not look other places...
                row[0] = "file://"+head+'/'+fi[0]

    statusfile.write("%d failures to find a P2P file matching a file needed by the database\n"%(nnomatch))
    if statusfile!=sys.stdout:
        print "%d failures to find a P2P file matching a file needed by the database\n"%(nnomatch)
    for row in dllist:
        fdllist.write('\t'.join([ row[0], head+'/'+row[1], row[2], row[3], row[4] ])+'\n')
    ffiles.close()
    fdllist.close()
    if statusfile is not sys.stdout: statusfile.close()

if len(sys.argv)<2:
    # no arguments, this is just being imported for the functions.
    pass
else:
    # an argument means this is run as a standalone command-line program
    import getopt
    try:
        facetopts = [k+'=' for k in facets_default.keys()]
        optopts = ["statusfile=","force"]
        args, lastargs = getopt.getopt(sys.argv[1:], "", facetopts+optopts )
    except getopt.error:
        print "error in input arguments", sys.exc_info()[:3]
        sys.exit(1)

    forcedl = False
    print optopts
    print args
    #parse arguments
    for flag, arg in args:
        print flag, arg
        if flag[2:] in facets_default.keys():
            facets_default[flag[2:]] = arg
        elif flag[1:] in facets_default.keys():
            facets_default[flag[1:]] = arg
        elif flag[2:] in optopts:
            if flag[2:]=="force":
                forcedl = True
            else:
                print "for now, ignoring option",flag
        else:
            print "ignored option",flag

    print facets_default, forcedl
    download_list(statusfile="esg.status",forcedl=forcedl)
