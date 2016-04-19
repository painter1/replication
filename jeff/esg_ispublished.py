#!/usr/apps/esg/cdat6.0a/bin/python

# Uses the tools of esgquery_index to determine whether a named file has been published
# at LLNL.

from esgquery_index import *
from pprint import pprint

def ispublished( filename, version=None ):
    global DEFAULT_QUERY_SERVICE
    service = DEFAULT_QUERY_SERVICE
    if version is not None and version[0]=='v':
	version = version[1:]
    try:
        variable, cmor_table, model, experiment, ensemble, timesnc =\
                  filename.split('_')
    except:  # some filenames don't have a time in them
        variable, cmor_table, model, experiment, ensemble =\
                  filename.split('_')
        ensemble = ensemble[:-3]  # the rest of it is .nc
        timesnc = '.nc'
        print "jfp ensemble=",ensemble,"timesnc=",timesnc
    if version is None:
	    query = service+'?'+'type=File&fields=size,id&variable=%s&cmor_table=%s&model=%s&experiment=%s&ensemble=%s&offset=0&limit=100'%(variable,cmor_table,model,experiment,ensemble)
    else:
    	query = service+'?'+'type=File&fields=size,id&variable=%s&cmor_table=%s&model=%s&experiment=%s&ensemble=%s&version=%s&offset=0&limit=100'%(variable,cmor_table,model,experiment,ensemble,version)
    chunk = readChunk( service, query )
    results, numFound, numResults = parseResponse(chunk, True) #jfp debugging
    #print "jfp results="
    #pprint(results)
    found_tok = [ ifv for ifv in results if ifv[0].find(timesnc)>0 ]
    goods = [ ifv for ifv in found_tok if ifv[0].find('.llnl.gov')>0 ]
    printable = [ ifv for ifv in goods if ifv[1]=='size' and ifv[2]>0 ]
    print "jfp found",len(printable),"items from LLNL with the right file name"
    pprint( printable )
    verss = [ ifv[0].split('.')[9] for ifv in printable ]
    print "jfp versions:",verss
    return len(goods)>0

if __name__=='__main__':
    if len(sys.argv)<2:
        print "name a file!"
    elif len(sys.argv)==2:
	print ispublished(sys.argv[1])
    else:
        print ispublished(sys.argv[1],sys.argv[2])

