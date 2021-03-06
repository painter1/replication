#!/usr/apps/esg/cdat6.0a/bin/python

# slightly modified from
# /usr/apps/esg/cdat6.0a/lib/python2.7/site-packages/esgcet-2.10.1-py2.7.egg/EGG-INFO/scripts/esgquery_index 
# in order to use the script's familiar interface within Python code

import sys
import getopt
import string
import re
import pprint

from lxml import etree
from urllib2 import urlopen, HTTPError

DATASET = 1
FILE = 2
MODEL = 3
typeCode = {'d': DATASET,
            'f': FILE,
            'm': MODEL,
            }
typeName = {DATASET:'Dataset',
            FILE:'File',
            MODEL:'Model',
            }
DEFAULT_FORMAT = "narrow"
DEFAULT_CHUNKSIZE = 1000
#DEFAULT_QUERY_SERVICE = "http://pcmdi9.llnl.gov/esg-search/search" original
DEFAULT_QUERY_SERVICE = "http://pcmdi.llnl.gov/esg-search/search"  # works Apr2016
MAX_RECORDS = 10000000                  # Maximum number of records

usage = """Usage:
    esgquery_index [Options]

    Query an ESGF index node. Return the results in tabular form as defined by --format.

    Various types of queries are supported:

    - faceted search (--facet-query)
    - free text search (--free-text)
    - search for available facet values (--facets)
    - specification of result type (--type)
    - return counts only (--count)

    The different search types can be combined in a single query. The script formulates
    an HTTP query to the SOLR query service defined by --service-url. The resulting XML
    document is parsed, and the results are extracted and displayed in the format defined
    by --format.

    By default, all possible metadata fields associated the result objects are displayed.
    To limit the output fields, list them with the --fields option. Similarly the --limit
    option specifies the maximum number of results.

Options:

    --count
       Return the record count only.

    -d character
    --delimiter character
       Print output separated by delimiter character. Turns off pretty-print option.

    --facets [FIELD | *]
       Return all possible values of the facet that apply to the search results. If the argument is *,
       return the field names available for querying.

    --fields FIELD,FIELD,...
       Limit output to the listed fields.

    --format narrow | wide
       Output format. Default is narrow. Note: Some fields may have multiple values associated with an
       object. For example, datasets may contain multiple variables. In that case the
       wide format will only show one of the values for that field.

       narrow (default): Each line is (id, field, value)
       wide: Each line is (id, field1_value, field2_value, ...)

    -h, --help
       Help string.

    --limit limit
       Limit the number of records returned. Note: The limit may be larger than the internal per-query
       limit, currently 1000. If so, multiple queries are issued and the results combined.
       The default is to read all records. Also note this option does not apply to facet option
       queries (--facets).

    -o path
       Save output to a file.

    -p, --pretty-print
       Align results and print field headers. See --delimiter.

    -q FIELD=VALUE
    --facet-query FIELD=VALUE[,FIELD=VALUE...]
       Faceted query. Return all records matching the queries. This option may be used more than once.
       See Notes for more details.

    --service-url URL
       Query the service at URL. If the esgcet publisher package is present, the value of parameter
       'solr_search_service_url' is used, otherwise the default is %s.

    -t TEXT
    --free-text TEXT
       Free text query.

    --type f|d
       List files (f) or datasets (d). Default is dataset.

    -v, --verbose
       Echo processing information.

Notes:

    - For fields that are different, the queries are combined with the AND operator. For fields that are the same,
      the queries are combined with OR. For example:

      experiment=historical,experiment=piControl,model=CanCM4

      searches for all records where (experiment is historical OR experiment is piControl) AND (model is CanCM4)

Examples:

    - Find all CMIP5 datasets for model INM-CM4. List the model, experiment, and time frequency

      esgquery_index -q project=CMIP5,model=INM-CM4 --fields model,experiment,time_frequency -p

    - Find all CMIP5 monthly decadal2001 files for model CanCM4, variable specific humidity

      esgquery_index -q project=CMIP5,experiment=decadal2001,model=CanCM4,time_frequency=mon,cf_standard_name=specific_humidity --fields id --type f -p

    - Find all metadata associated with a specific dataset  

      esgquery_index -q id='cmip5.output1.INM.inmcm4.historical.mon.atmos.Amon.r1i1p1.v20111201|pcmdi9.llnl.gov'

    - Determine if a file with a specific tracking_id is in the latest version of a dataset

      esgquery_index --type f -t 'tracking_id:5d70965b-18c5-4bc1-9e88-ae4cc6b147e5' -q latest=true --fields latest

    - Find all files in a given dataset

      esgquery_index --type f -q dataset_id='cmip5.output1.CCCma.CanCM4.decadal2001.mon.atmos.Amon.r9i2p1.v20111022|dap.cccma.uvic.ca' --fields id -p

    - Get the checksum of a file

      esgquery_index -v --type f -q id='cmip5.output1.CCCma.CanCM4.decadal2001.mon.atmos.Amon.r9i2p1.v20111022.cl_Amon_CanCM4_decadal2001_r9i2p1_200201-201112.nc|dap.cccma.uvic.ca' --fields checksum

    - Get the datasets stored locally on a non-default node

      esgquery_index --service-url http://esg-datanode.jpl.nasa.gov/esg-search/search -q distrib=false --fields id -p

    - Find all data nodes with any version of a dataset

      esgquery_index -q master_id=cmip5.output1.INM.inmcm4.rcp85.fx.land.fx.r0i0p0 --fields id,data_node -p

    - Find the data node having the master copy of a dataset

      esgquery_index -q master_id=cmip5.output1.INM.inmcm4.rcp85.fx.land.fx.r0i0p0,replica=false --fields version -p

    - Find the URLs of files containing variable fgco2 for experiment rcp85

      esgquery_index -v -q variable=fgco2,experiment=rcp85 --fields url --type f -p

    - Find all versions of a dataset

      esgquery_index -q master_id='cmip5.output1.NOAA-GFDL.GFDL-CM3.historicalNat.mon.atmos.Amon.r3i1p1' --fields id,version,latest -p

    - List all CMIP5 experiments

      esgquery_index --facets experiment -q project=CMIP5 -p

    - Find the CMIP5 models for which data has been published since 3/19/2012

      esgquery_index -q from=2012-03-19T00:00:00Z,project=CMIP5 --facets=model -p

    - List all facet fields

      esgquery_index --facets='*' -p

"""%(DEFAULT_QUERY_SERVICE,)

def leng(item):
    if item is not None:
        return len(item)
    else:
        return 4

def getItemCount(header, tuples):
    itemCount = [len(item) for item in header]
    for item in tuples:
        try:
            itemCount = map(lambda x,y: max(x,y), itemCount, [leng(t) for t in item])
        except:
            print item
            raise
    return itemCount

def printQueryResult(header, tuples, out=sys.stdout, printHeaders=True):
    """Print a query result.

    header
      A list of property names.

    tuples
      A list of result tuples. Each tuple has the same length as the header, and each item in the tuple corresponds to the
      respective header item. For example, if the header is ['field1', 'field2'] then ``tuples`` might have the value [(dataset1_field1, dataset1_field2), (dataset2_field1, dataset2_field2), ...]

    out
      Output file object.

    printHeaders
      Boolean flag. If True, print header and trailer lines.
      
    """
    itemCount = getItemCount(header, tuples)
    width = reduce(lambda x,y: x+y, itemCount)+3*len(itemCount)-1
    breakline = '+'+width*'-'+'+'
    if printHeaders:
        format = '| '+reduce(lambda x,y: x+' | '+y, ["%%-%ds"%item for item in itemCount])+' |'
    else:
        format = reduce(lambda x,y: x+' | '+y, ["%%-%ds"%item for item in itemCount])
    if printHeaders:
        print >>out, breakline
        print >>out, format%tuple(header)
        print >>out, breakline
    count = 0
    for item in tuples:
        count += 1
        try:
            print >>out, format%item
        except:
            print >>out, 'Barf!'
            print >>out, item
            sys.exit(0)
    if printHeaders:
        print >>out, breakline
        print >>out, '%d results found'%count

try:
    from esgcet.query import printResult
except:
    printResult = printQueryResult
    
# Formulate a SOLR query, returning the http query string.
#
# facets: list of (string,value) tuples
# fields: list of strings
# freetext: string arg to free text query, or None
# objtype: DATASET, FILE, or MODEL
# service: search service URL
# facetValues: list of strings
def formulateQuery(facets, fields, format, freetext, objtype, service, offset, limit, facetValues=None):
    queryList = ["type=%s"%typeName[objtype]]
    if facetValues is not None:
        facetString = string.join(facetValues,',')
        queryList.append("facets=%s"%facetString)
    if freetext is not None:
        queryList.append("query=%s"%freetext)
    if len(fields)>0:
        if 'id' not in fields:
            fields.append('id')
        fieldString = string.join(fields,',')
        queryList.append("fields=%s"%fieldString)
    if len(facets)>0:
        for field,value in facets:
            queryList.append("%s=%s"%(field,value))
    queryList.append("offset=%d"%offset)
    queryList.append("limit=%d"%limit)
    queryString = string.join(queryList,'&')
    query = "%s?%s"%(service,queryString)
    return query

def readChunk(service, query):
    try:
        tree = etree.parse(query)
    except IOError as xe:
        print "IOError exception:"
        print xe
        try:
            u = urlopen(query)
        except HTTPError as he:
            print "HTTPError exception on",query
            errorHtml = he.read()
            m = re.search(r'message.*?<u>(.*?)</u>', errorHtml)
            if m is not None:
                print 'Error:', m.group(1)
            sys.exit(1)
        # can't go on, we don't have 'tree'
        print "Nobody ever wrote code to handle this case, exiting"
        print "query=",query
        sys.exit(1)
    except Exception as e:
        print "From query",query,"encountered exception",e
        raise e
        sys.exit(1)
    return tree

# Parse the response header for available facet values
# Returns (([valueList],), header), numFound as for parseTrailer
def parseHeader(tree):
    root = tree.getroot()
    headerElem = root[0]
    for item in headerElem.iter('arr'):
        if item.get('name')=='facet.field':
            valueList = [(sub.text,) for sub in item]
            header = ['field']
            break
    else:
        raise RuntimeError("No facet.field list found")
    result = root[1]
    numFound = int(result.get('numFound'))
    return ([valueList], header), numFound

# Parse a SOLR XML response
# tree is an lxml ElementTree
# If includeId is true, the response records include (id,'id',id) where applicable.
# Returns (results, number_found, number_documents) where
# results is a list [(objid, field, value), (objid, field, value), ...], 
# number_found is the total number of results independent of chunk size,
# and number_documents is the number of results parsed by this call.
def parseResponse(tree, includeId):
    root = tree.getroot()
    result = root[1]
    numFound = int(result.get('numFound'))
    start = int(result.get('start'))
    newResults = []
    numDocs = 0
    for doc in result:
        objid = ''
        interResults = []               # [(field, value), (field, value), ...]
        for elem in doc:
            name = elem.get('name')
            if name=='id':
                objid = elem.text
                if includeId:
                    interResults.append(('id', objid))
            elif elem.tag=='arr':
                for sub in elem:
                    if sub.tag=='str':
                        interResults.append((name, sub.text))
            else:
                interResults.append((name, elem.text))
        results = [(objid, field, value) for field, value in interResults]
        newResults.extend(results)
        numDocs += 1
    return newResults, numFound, numDocs

# Parse the trailer for facet values and counts.
# Returns ((valueLists, header), numFound where
#   valueLists = [list1, list2, ... listn]
#   listn is a list [(facetOption, facetCount), (facetOption, facetCount), ...]
#   header = [facetName1, facetName2, ...] of same length as valueLists
#   numFound is the total number of records that would be returned
def parseTrailer(tree, facetValues, includeId):
    root = tree.getroot()
    result = root[1]
    numFound = int(result.get('numFound'))
    numResults = 0                      # No actual data records returned
    valueLists = []
    header = []
    trailer = root[2]
    if trailer.get('name') != 'facet_counts':
        raise RuntimeError("Could not find facet counts in response")
    facetFields = trailer[1]
    if facetFields.get('name')!='facet_fields':
        raise RuntimeError("Could not find facet fields in response")
    for item in facetFields:
        field = item.get('name')
        subResults = [(sub.get('name'), sub.text) for sub in item]
        valueLists.append(subResults)
        header.append(item.get('name'))
    return (valueLists, header), numFound

# Print a list of results.
# results = [(id,field,value), (id,field,value), ...]
def outputResults(results, format, header=None, prettyPrint=False, printHeaders=True, delimiter=None, out=sys.stdout):
    if format=='narrow':
        if prettyPrint:
            header = ['id', 'field', 'value']
            printResult(header, results, printHeaders=printHeaders, out=out)
        else:
            if delimiter is not None:
                for item in results:
                    print >>out, delimiter.join(item)
            else:
                for item in results:
                    print >>out, item
    elif format=='wide':
        # valuedict: (id, field) => value
        objset = set()
        fieldset = set()
        valueDict = {}
        for objid,field,value in results:
            objset.add(objid)
            fieldset.add(field)
            valueDict[(objid, field)] = value

        objlist = list(objset)
        objlist.sort()
        if 'id' in fieldset:
            fieldset.remove('id')
        fieldlist = list(fieldset)
        fieldlist.sort()
        wideResults = []
        for obj in objlist:
            wideResults.append(tuple([obj]+[valueDict.get((obj,field), '') for field in fieldlist]))
        if prettyPrint:
            printResult(['id']+fieldlist, wideResults, printHeaders=printHeaders, out=out)
        else:
            if delimiter is not None:
                for item in wideResults:
                    print >>out, delimiter.join(item)
            else:
                for item in wideResults:
                    print >>out, item
    else:
        print 'Format not yet implemented:', format
        sys.exit(1)

# Print facet results.
# results = [(v1, ..., vn), (v1, ..., vn), ...]
# header = [f1, f2, ..., fn]
def outputFacetResults(results, header, prettyPrint=False, printHeaders=True, delimiter=None, out=sys.stdout):
    if prettyPrint:
        printResult(header, results, printHeaders=printHeaders, out=out)
    else:
        if delimiter is not None:
            for item in results:
                print delimiter.join(item)
        else:
            for item in results:
                print item

def preoutput(argv):
    # This is what was formerly all of main() but the output section.
    global DEFAULT_QUERY_SERVICE

    try:
        args, lastargs = getopt.getopt(argv, "d:ho:pq:t:v", ['count', 'delimiter=', 'facet-query=', 'facets=', 'fields=', 'format=', 'free-text=', 'help', 'limit=', 'pretty-print', 'service-url=', 'type=', 'verbose'])
    except getopt.error:
        print sys.exc_value
        print usage
        sys.exit(0)

    # Get the search URL from the publisher configuration if possible
    try:
        from esgcet.config import loadConfig

        config = loadConfig(None)
        DEFAULT_QUERY_SERVICE = config.get("DEFAULT", "solr_search_service_url", default=DEFAULT_QUERY_SERVICE)
    except:
        pass

    allFacets = False                   # facets=*
    countOnly = False
    delim = None
    facets = []
    facetValues = None
    fields = []
    format = DEFAULT_FORMAT
    freetext = None
    includeId = False
    objtype = DATASET
    offset = 0
    outpath = sys.stdout
    outpathIsStdout = True
    prettyPrint = False
    service = DEFAULT_QUERY_SERVICE
    userLimit = MAX_RECORDS
    verbose = False
    for flag, arg in args:
        if flag=='--count':
            countOnly = True
        elif flag in ['-d', '--delimiter']:
            delim = arg
            prettyPrint = False
        elif flag=='--facets':
            facetList = arg.split(',')
            facetValues = [item.strip() for item in facetList]
            allFacets = (facetValues[0]=='*')
        elif flag=='--fields':
            fieldList = arg.split(',')
            fields = [item.strip() for item in fieldList]
        elif flag=='--format':
            if arg not in ['narrow', 'wide']:
                raise RuntimeError("Invalid format: %s"%arg)
            format = arg
        elif flag in ['-h', '--help']:
            print usage
            sys.exit(0)
        elif flag=='--limit':
            try:
                userLimit = int(arg)
            except:
                raise RuntimeError("Invalid limit: %s"%arg)
        elif flag=='-o':
            outpath = open(arg, 'w')
            outpathIsStdout = False
        elif flag in ['-p', '--pretty-print']:
            prettyPrint = True
        elif flag in ['-q', '--facet-query']:
            queries = arg.split(',')
            for q in queries:
                f,v = q.split('=')
                facets.append((f.strip(), v.strip()))
        elif flag=='--service-url':
            service = arg
        elif flag in ['-t', '--free-text']:
	    freetext = arg
	elif flag=='--type':
            try:
                objtype = typeCode[arg]
            except:
                raise RuntimeError("Invalid return type: %s"%arg)
        elif flag in ['-v', '--verbose']:
            verbose = True

    # If returning id only, use wide format
    if fields==['id']:
        format = 'wide'
        includeId = True

    # For facet value queries, use wide format.
    if facetValues is not None:
        format = 'wide'

    # While remaining data:
    fullResults = []
    numFound = 0
    moredata = True
    nread = 0
    nleft = userLimit
    offset = 0
    chunksize = DEFAULT_CHUNKSIZE
    while moredata:

        # Formulate a query
        if not (countOnly or facetValues is not None):
            limit = min(nleft, chunksize)
        else:
            limit = 0
        query = formulateQuery(facets, fields, format, freetext, objtype, service, offset, limit, facetValues=facetValues)
        if verbose:
            print >>sys.stderr, 'Query: ', query

        # Read a chunk
        chunk = readChunk(service, query)

        # Parse the response. For facet value searches, parse the response trailer
        if facetValues is None:
            results, numFound, numResults = parseResponse(chunk, includeId)
            fullResults.extend(results)
        else:
            numResults = 0
            if allFacets:
                fullResults, numFound = parseHeader(chunk)
            else:
                fullResults, numFound = parseTrailer(chunk, facetValues, includeId)

        # More data if some results were found and the number of records read < total
        nread += numResults
        nleft -= limit
        moredata = (numResults>0) and (nread<min(numFound, userLimit))
        offset += limit

##    print "jfp fullResults as list of (id,field,value) ="
##    pprint.pprint(fullResults)
##    results_ids = set([a for a,b,c in fullResults])
##    fullResults_dicts = [(a, { b:c for a,b,c in fullResults if a1==a }) for a1 in results_ids]
##    print "jfp fullResults as list of (id,dict_of_field:value) ="
##    pprint.pprint( fullResults_dicts )
    # TO DO: return values which aren't used here, really should be computed in main
    return (fullResults,countOnly,facetValues,allFacets,prettyPrint,delim,outpath,numFound,\
            outpathIsStdout,format)

def main(argv):
    (fullResults,countOnly,facetValues,allFacets,prettyPrint,delim,outpath,numFound,\
     outpathIsStdout,format) = preoutput(argv)

    # Output the results
    if not (countOnly or facetValues is not None):
        outputResults(fullResults, format, prettyPrint=prettyPrint, printHeaders=True,\
                      delimiter=delim, out=outpath)
    elif facetValues is not None:
        for valueList, head in zip(fullResults[0], fullResults[1]):
            if allFacets:
                header = (head,)
            else:
                header=(head,'count')
            outputFacetResults(valueList, header, prettyPrint=prettyPrint, printHeaders=True,\
                               delimiter=delim, out=outpath)
    else:
        print numFound

    if not outpathIsStdout:
        outpath.close()

if __name__=='__main__':
    main(sys.argv[1:])
