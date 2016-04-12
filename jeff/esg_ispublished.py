#!/usr/apps/esg/cdat6.0a/bin/python

# Uses the tools of esgquery_index to determine whether a named file has been published
# at LLNL.

from esgquery_index import *
from pprint import pprint

def ispublished( filename ):
    global DEFAULT_QUERY_SERVICE
    service = DEFAULT_QUERY_SERVICE
    variable, cmor_table, model, experiment, ensemble, timesnc =\
              filename.split('_')
    query = service+'?'+'type=File&fields=size,id&variable=%s&cmor_table=%s&model=%s&experiment=%s&ensemble=%s&offset=0&limit=100'%(variable,cmor_table,model,experiment,ensemble)
    chunk = readChunk( service, query )
    results, numFound, numResults = parseResponse(chunk, True) #jfp debugging
    found_tok = [ ifv for ifv in results if ifv[0].find(timesnc)>0 ]
    found_tok_llnl = [ ifv for ifv in found_tok if ifv[0].find('.llnl.gov')>0 ]
    printable = [ ifv for ifv in found_tok_llnl if ifv[1]=='size' ]
    print "jfp found",len(printable),"items from LLNL with the right file name"
    pprint( printable )
    return len(found_tok_llnl)>0

if __name__=='__main__':
    print ispublished(sys.argv[1])
