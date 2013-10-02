#!/usr/apps/esg/cdat6.0a/bin/python
""" get the status of datasets and of their files, from the esgcet database.
 This script only reports on datasets which have been put on a download list (status 20 or -1),
 but haven't been completed (generally status 30 or above).
 The main purpose is to identify those datasets which can be completed by
 downloading just a few more files."""
# This is straightforward but really slow...

# Usage:
# 1. export PYTHONPATH=$PYTHONPATH:/export/home/painter/src/esgf-contrib/estani/python/
#    if using Estani's script to generate download list; or, if using Jeff's script:
#    export PYTHONPATH=$PYTHONPATH:/export/home/painter/pytools/
# 2. Set the hard-coded inputs just below.  Review the code, as it is intended to be
#    changed as needed.
# 3. If you chose to make download lists, concatenate them and edit them as needed.
# Here are the most important inputs.  Change them here:
like = "'cmip5.output1.MIROC.%.mon.atmos.%'"     # the "LIKE" part of the query to identify datasets to work on
make_dl_lists = 9 # Set to True or to the minimum priority number to make download lists, or
                      # set to False to make no download lists.  Making download lists is a lot
                      # slower and confuses the output, but it can be very useful.
use_estani_script = True # True to use Estani's script, False to use Jeff's (based on a P2P script)

# imports copied from harvest_cmip5.py, maybe not all needed...
import sqlalchemy
import os,sys
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, Boolean, DateTime, sql, ForeignKeyConstraint, orm
import time, datetime
# imports added as needed...
from esgcet.config import loadConfig

sql_ds20 = "SELECT name FROM replica.datasets WHERE name LIKE "+like+" AND (status=20 OR status=-1);"
#old sql_dslt100 = "SELECT name FROM replica.datasets WHERE name LIKE "+like+" AND status<30;"
sql_ds = sql_ds20
#old sql_nfa1 = "SELECT COUNT(*) FROM replica.files WHERE dataset_name='"

config = loadConfig(None)
engine = sqlalchemy.create_engine(config.get('replication', 'esgcet_db'), echo=False, pool_recycle=3600)

datasets = engine.execute( sql.text( sql_ds ) ).fetchall()
tnfiles = tnfiles010 = tnfiles20 = tnfiles30 = tnfiles100 = tnfiles_m1 = 0
fmt = "%80s %1s %12s %12s %12s %12s %12s %12s"
print fmt % ("dataset","pri","files","status 0/10","status 20","status 30","status 100","status -1")
print fmt % (" ",      "   "," ",    "no attempt","chose to dl","downloaded","verified","error")
for ds in datasets:
    dstr = ds[0]
    #old nfiles = engine.execute(sql.text( sql_nfa1+ds[0]+"';" ) ).fetchall()[0][0]
    # if nfiles==0, it's probably an output2 dataset; or it could be a catalog error.
    #old if nfiles==0: continue
    #old nfiles010 = engine.execute(sql.text( sql_nfa1+ds[0]+"' AND status>=0 AND status<=10;" ) ).fetchall()[0][0]
    #old nfiles20 = engine.execute(sql.text( sql_nfa1+ds[0]+"' AND status=20;" ) ).fetchall()[0][0]
    # Note re status 20: At present (2012.02.23), create_download_lists() in replica_manager.py
    # only sets status 20 for datasets, not individual files; so I expect nfiles20==0 always.
    # If I want to set a status to 20, the place is either in protocol_handler.py,getFile()
    # or in Download.py, where getFile() is called.
    # 2012.03.07: Now verify_datasets will set file status to 20 if it exists but is
    # incomplete.
    #old nfiles30 = engine.execute(sql.text( sql_nfa1+ds[0]+"' AND status=30;" ) ).fetchall()[0][0]
    #old nfiles100 = engine.execute(sql.text( sql_nfa1+ds[0]+"' AND status=100;" ) ).fetchall()[0][0]
    #old nfiles_m1 = engine.execute(sql.text( sql_nfa1+ds[0]+"' AND status=-1;" ) ).fetchall()[0][0]

    # experimental one-liner
    allnfd = {}
    sqlall = "SELECT status, COUNT(status) AS nfiles FROM replica.files WHERE dataset_name='"+\
          ds[0]+"' GROUP BY status;"
    allnf = engine.execute(sql.text(sqlall)).fetchall()
    nfiles = sum( f[1] for f in allnf )
    if nfiles==0: continue
    for status,nfs in allnf: allnfd[status]=nfs
    nfiles010 = allnfd.get(0,0) + allnfd.get(10,0)
    nfiles20 = allnfd.get(20,0)
    nfiles30 = allnfd.get(30,0)
    nfiles100 = allnfd.get(100,0)
    nfiles_m1 = allnfd.get(-1,0)
    # print "allnfd=",allnfd,type(allnfd.keys()[0]),type(allnfd[allnfd.keys()[0]])


    tnfiles += nfiles
    tnfiles010 += nfiles010
    tnfiles20 += nfiles20
    tnfiles30 += nfiles30
    tnfiles100 += nfiles100
    tnfiles_m1 += nfiles_m1
    if nfiles100<nfiles and nfiles100>0.5*nfiles:
        if dstr.find("mon.atmos")>0:
            if dstr.find(".historical.")>0 or dstr.find(".rcp85.")>0 or dstr.find(".rcp45.")>0\
               or dstr.find(".piControl.")>0:
                dpri = 9
                # print ' '*80,"...urgent download candidate"
            elif dstr.find(".decadal")<0:
                dpri = 8
                # print ' '*80,"...excellent download candidate"
            else:
                dpri = 7
                # print ' '*80,"...strong download candidate"
        else:
            if dstr.find(".decadal")<0:
                dpri = 6
                # print ' '*80,"...good download candidate"
            else:
                dpri = 5
                # print ' '*80,"...average download candidate"
    elif nfiles100<nfiles and dstr.find("mon.atmos")>0:
        dpri = 2
    else:
        dpri = 0
    print fmt % (dstr,dpri,nfiles,nfiles010,nfiles20,nfiles30,nfiles100,nfiles_m1)
    if make_dl_lists is False:
        continue
    if dpri>=make_dl_lists:
        # Create a download list for just this dataset.
        # First, generate a filename, using time to make it unique.
        tt=time.localtime().__reduce__()[1][0]
        ts = '.'.join(str(i) for i in tt)
        outfile = "download_"+dstr+'_'+ts
        print "creating download list",outfile
        if use_estani_script:
            import replica_manager
            replica_manager.dataset_match = dstr
            dataset_type = 'list.repo.pcmdi'  # You have to 'know' this one!  pcmdi is most common
            replica_manager.create_download_lists( outfile, dataset_type )
        else:
            from esg2dlist import *
            facets = dataset2facets( dstr )
            download_list( facets=facets, downloadlist=outfile, statusfile="esg.status" )
        # Probably the download list will need editing.
        

print fmt % (str(len(datasets))+" datasets, totals"," ",\
             tnfiles,tnfiles010,tnfiles20,tnfiles30,tnfiles100,tnfiles_m1)
