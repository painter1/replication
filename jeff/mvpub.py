#!/usr/apps/esg/cdat6.0a/bin/python

# Moves already-published data from scratch to where it belongs.
# The destination comes from esgquery_index output (which must be supplied).
# Usage:   First run esgquery_index and filter the output; see below.  Then:
#          mvpub esg.out
# WARNING: Before running this, you have to verify the files, i.e. make sure that each has the
# correct length and checksum.  This script doesn't compute checksums, or even verify lengths.
#
# Sample esgquery_index line:
#  nohup esgquery_index --type f -q project=CMIP5,product=output1,institute=MOHC,experiment=rcp85,replica=True --fields url -p | grep llnl | grep -v cmip5_css01 | grep -v cmip5_css02 > esg.out &
#
# Sample of esgquery_index output (it has to look like this):
#| cmip5.output1.MOHC.HadGEM2-CC.rcp85.mon.atmos.Amon.r2i1p1.v20111215.ua_Amon_HadGEM2-CC_rcp85_r2i1p1_203012-205511.nc|pcmdi9.llnl.gov                                      | url   | http://pcmdi9.llnl.gov/thredds/fileServer/cmip5_data/cmip5/output1/MOHC/HadGEM2-CC/rcp85/mon/atmos/Amon/r2i1p1/ua/1/ua_Amon_HadGEM2-CC_rcp85_r2i1p1_203012-205511.nc|application/netcdf|HTTPServer                                                     |
import os, sys, shutil

def firstpath(line):
    # Example:
    # s2="cmip5.output1.MOHC.HadGEM2-CC.rcp85.6hr.atmos.6hrPlev.r2i1p1.v20111217.psl_6hrPlev_HadGEM2-CC_rcp85_r2i1p1_2005120106-2006120100.nc|pcmdi9.llnl.gov"
    # s3="cmip5.output1.MOHC.HadGEM2-CC.rcp85.6hr.atmos.6hrPlev.r2i1p1.v20111217.psl_6hrPlev_HadGEM2-CC_rcp85_r2i1p1_2005120106-2006120100.nc"
    # s4="cmip5/output1/MOHC/HadGEM2-CC/rcp85/6hr/atmos/6hrPlev/r2i1p1/v20111217/psl_6hrPlev_HadGEM2-CC_rcp85_r2i1p1_2005120106-2006120100.nc"
    # filename="psl_6hrPlev_HadGEM2-CC_rcp85_r2i1p1_2005120106-2006120100.nc"
    # varname='psl'
    # s6="cmip5/output1/MOHC/HadGEM2-CC/rcp85/6hr/atmos/6hrPlev/r2i1p1/v20111217/psl/psl_6hrPlev_HadGEM2-CC_rcp85_r2i1p1_2005120106-2006120100.nc"
    s1 = line.split(' ')
    s2 = s1[1]
    s3 = s2.split('|')[0]
    s4 = s3[0:-3].replace('.','/')+s3[-3:]
    filename = s4.split('/')[-1]
    varname = filename.split('_')[0]
    s6 = s4[0:-len(filename)]+varname+'/'+filename
    return s6

def secondpath(line):
    # Example:
    # s2="http://pcmdi9.llnl.gov/thredds/fileServer/cmip5_data/cmip5/output1/MOHC/HadGEM2-CC/rcp85/6hr/atmos/6hrPlev/r2i1p1/psl/1/psl_6hrPlev_HadGEM2-CC_rcp85_r2i1p1_2005120106-2006120100.nc|application/netcdf|HTTPServer"
    # url="http://pcmdi9.llnl.gov/thredds/fileServer/cmip5_data/cmip5/output1/MOHC/HadGEM2-CC/rcp85/6hr/atmos/6hrPlev/r2i1p1/psl/1/psl_6hrPlev_HadGEM2-CC_rcp85_r2i1p1_2005120106-2006120100.nc"

    s1 = [s for s in line.split(' ') if len(s)>0]
    s2 = s1[5]
    url = s2.split('|')[0]
    hdrstr = "http://pcmdi9.llnl.gov/thredds/fileServer/cmip5_data/"
    iurl = url.find(hdrstr) + len(hdrstr)
    return url[iurl:],url
    
def double_publish_warning( frompath, pubpath, url ):
    if url.find('cmip5_css')<0 and pubpath[0:4]=='/css':
        # i.e. the url doesn't point to CSS but pubpath does.
        print "possible doubly-published file"
        print "scratch:",frompath
        print "data:",pubpath
        print "url:",url
        # Note: to tell for sure that it's a duplicate file, we need version numbers, at least.
        # If we don't trust the publisher database, we still could compute checksums.

def get_status( abspath ):
    """returns the status of a file defined by abspath, or None if the file cannot be found"""
    sqline = "SELECT  <<<<<<<<<<<<<<<<

eqifile = sys.argv[1]  # name of file containing esgquery_index ouput
f = open(eqifile)
lineno = 0
moveno = 0
hv_css01 = 0
hv_css02 = 0
hv_gdo = 0
still_missing = 0
for line in f:
    lineno += 1
    #if lineno >1920:
    #    lineno -=1
    #    break  # just testing
    abspath = firstpath(line)
    urlpath,url = secondpath(line)
    from1 = '/css01-cmip5/scratch/'+abspath
    from2 = '/css02-cmip5/scratch/'+abspath
    if os.path.isfile(from1) and os.path.getsize(from1)>0:
        frompath = from1
    elif os.path.isfile(from2) and os.path.getsize(from2)>0:
        frompath = from2
    else:
        frompath = None
    pubpath_css01 = '/css01-cmip5/data/'+urlpath
    pubpath_css02 = '/css02-cmip5/data/'+urlpath
    pubpath_gdo = '/css02-cmip5/cmip5/data/'+urlpath
    if os.path.isfile(pubpath_css01) and os.path.getsize(pubpath_css01)>0:
        double_publish_warning( frompath, pubpath_css01, url )
        hv_css01 += 1
    elif os.path.isfile(pubpath_css02) and os.path.getsize(pubpath_css02)>0:
        double_publish_warning( frompath, pubpath_css02, url )
        hv_css02 += 1
    elif os.path.isfile(pubpath_gdo) and os.path.getsize(pubpath_gdo)>0:
        hv_gdo += 1
    else:
        if frompath is None:
            still_missing +=1
            continue
        # File exists in scratch, move to pubpath_gdo.
        moveno +=1
        print "mv",frompath," ",pubpath_gdo
        destpath = os.path.dirname(pubpath_gdo)
        try:
            os.makedirs(destpath)
            #print "just made",destpath
        except OSError as e:
            if e.strerror!="File exists":
                raise e
            else:
                #print destpath,"already exists; continuing"
                pass
        # For some tests, don't actually move anything:
        #shutil.move( frompath, pubpath_gdo )

print lineno,"lines found in",eqifile
print hv_css01,"files are published on CSS-01"
print hv_css02,"files are published on CSS-02"
print hv_gdo,"files are published on GDO2"
print moveno,"files moved from scratch"
print still_missing,"files are still missing"

