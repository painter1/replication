#!/usr/apps/esg/cdat6.0a/bin/python

import sys, lxml, pprint
import esgquery_index

def try2_pubpath2version(pubpath):
    """Input is a path of data as published at PCMDI, e.g.
    /cmip5/data/cmip5/output1/INM/inmcm4/1pctCO2/mon/atmos/Amon/r1i1p1/zg/1/zg_Amon_inmcm4_1pctCO2_r1i1p1_210001-210912.nc
    Hardly anything about the path can look different from this!  The format is, in full generality,
    /root/data/project/product/institute/model/experiment/time_frequency/realm/table/ensemble/variable/file_version/filename
    where "data" is exactly that string, and other names have the usual meanings.
    Output is a version string.
    Make sure you have p2p.py in your PYTHONPATH:
    export PYTHONPATH=$PYTHONPATH:$HOME/src/esgf-contrib/estani/esgf-replication-61f7311/replication/model/
    and
export PYTHONPATH=$PYTHONPATH:/export/home/painter/pytools:/export/home/painter/src/esgf-contrib/estani/replicas/
    """

    import p2p
    import esgquery_index

    sppath = pubpath.split('/')
    if sppath[2]!='data':
        print "don't recognize path as for published data"
        return None
    url1 = 'http://pcmdi9.llnl.gov'
    if sppath[1]=='cmip5':
        url2 = '/thredds/fileServer/cmip5_data/'
    elif sppath[1]=='css02-cmip5':
        url2 = '/thredds/fileServer/cmip5_css02_data/'
    else:
        print "don't recognize path as for published data"
        return None
    url3 = '/'.join(sppath[3:])
    url4 = '\|application/netcdf\|HTTPServer'
    urlfull = url1+url2+url3+url4
    # A typical urlfull is
    # 'http://pcmdi9.llnl.gov/thredds/fileServer/cmip5_data/cmip5/output1/INM/inmcm4/1pctCO2/mon/atmos/Amon/r1i1p1/zg/1/zg_Amon_inmcm4_1pctCO2_r1i1p1_210001-210912.nc\|application/netcdf\|HTTPServer'
    try:
        query_argv = ['--type', 'f', '-q', 'url='+urlfull, '--fields', 'url,version', '-p']
        #print "query_argv1=",query_argv
        (fullResults,countOnly,facetValues,allFacets,prettyPrint,delim,outpath,numFound,\
         outpathIsStdout,format) = esgquery_index.preoutput(query_argv)
    except lxml.etree.XMLSyntaxError as e:
        print "in pubpath2version(",pubpath,")"
        print "and query",query_argv
        print "exception",e
        return None
    if len(fullResults)==0:
        # try the other server:
        url1 = 'http://pcmdi7.llnl.gov'
        urlfull = url1+url2+url3+url4
        try:
            query_argv = ['--type', 'f', '-q', 'url='+urlfull, '--fields', 'url,version', '-p']
            #print "query_argv2=",query_argv
            (fullResults,countOnly,facetValues,allFacets,prettyPrint,delim,outpath,numFound,\
             outpathIsStdout,format) = esgquery_index.preoutput(query_argv)
        except lxml.etree.XMLSyntaxError as e:
            print "in pubpath2version(",pubpath,")"
            print "and query",query_argv
            print "exception",e
            return None
    if len(fullResults)==0:
        # try the other server:
        url1 = 'http://pcmdi7.llnl.gov'
        url2 = '/thredds/fileServer//cmip5_css02/data/'
        url4 = '|application/netcdf|HTTPServer'
        urlfull = url1+url2+url3+url4
        print "urlfull=",urlfull
        try:
            query_argv = ['--type', 'f', '-q', 'url='+urlfull, '--fields', 'url,version', '-p']
            print "query_argv3=",query_argv
            (fullResults,countOnly,facetValues,allFacets,prettyPrint,delim,outpath,numFound,\
             outpathIsStdout,format) = esgquery_index.preoutput(query_argv)
        except lxml.etree.XMLSyntaxError as e:
            print "in pubpath2version(",pubpath,")"
            print "and query",query_argv
            print "exception",e
            return None

    results_ids = set([a for a,b,c in fullResults])
    fullResults_dicts = [(a, { b:c for a,b,c in fullResults if a1==a }) for a1 in results_ids]
    # print "fullResults_dicts=",fullResults_dicts
    if len(results_ids)==0:
        print "No dataset found"
        return None
    elif len(results_ids)>1:
        print "Too many datasets!  Version cannot be determined. ids=",results_ids
        return None
    else:
        return list(results_ids)[0].split('.')[9]
        
def try3_pubpath2version(pubpath):
    """Input is a path of data as published at PCMDI, e.g.
    /cmip5/data/cmip5/output1/INM/inmcm4/1pctCO2/mon/atmos/Amon/r1i1p1/zg/1/zg_Amon_inmcm4_1pctCO2_r1i1p1_210001-210912.nc
    Output is a version string.
    Make sure you have p2p.py in your PYTHONPATH:
    export PYTHONPATH=$PYTHONPATH:$HOME/src/esgf-contrib/estani/esgf-replication-61f7311/replication/model/
    and
export PYTHONPATH=$PYTHONPATH:/export/home/painter/pytools:/export/home/painter/src/esgf-contrib/estani/replicas/
    """

    import p2p
    import esgquery_index

    sppath = pubpath.split('/')
    if sppath[2]!='data':
        print "don't recognize path as for published data"
        return None
    abs_path = '/'.join(sppath[3:])
    dataset = '.'.join(sppath[3:12])
    pcmdiservers=['pcmdi7.llnl.gov','pcmdi9.llnl.gov']
    for server in pcmdiservers:
        query_argv = ['--type', 'f', '-q', 'drs_id='+dataset+',data_node='+server, '--fields', 'url', '-p']
        (fullResults,countOnly,facetValues,allFacets,prettyPrint,delim,outpath,numFound,\
         outpathIsStdout,format) = esgquery_index.preoutput(query_argv)
        idurl = [(a,c) for a,b,c in fullResults if b=='url' and c.find(abs_path)>-1 ]
        if len(idurl)>0:
            break
    versions = ([a.split('.')[9] for (a,c) in idurl])
    if len(versions)==0:
        print "No match found"
        return None
    elif len(versions)>1:
        print "Too many matches!  Version cannot be determined. idurl=",idurl
        return None
    else:
        return versions[0]


def get_idurls_from_dataset( dataset ):
    """Input is a dataset name, e.g. 'cmip5.output1.NCC.NorESM1-M.historical.mon.atmos.Amon.r1i1p1'.
    This will return a list of (id,url) pairs for files in the dataset; if located at PCMDI.
    """
    idurls = []
    pcmdiservers=['pcmdi7.llnl.gov','pcmdi9.llnl.gov']
    for server in pcmdiservers:
        query_argv = ['--type', 'f', '-q', 'drs_id='+dataset+',data_node='+server, '--fields', 'url', '-p']
        try:
            (fullResults,countOnly,facetValues,allFacets,prettyPrint,delim,outpath,numFound,\
             outpathIsStdout,format) = esgquery_index.preoutput(query_argv)
            idurls.extend( [(a,c) for a,b,c in fullResults if b=='url' ] )
        except lxml.etree.XMLSyntaxError as e:
            print "in get_idurls_from_dataset(",dataset,")"
            print "with query",query_argv
            print "caught exception",e
            return None
    return idurls

def get_idurls_from_pubpath( pubpath ):
    """Input is a path of data as published at PCMDI - the same as the input to pubpath2version.
    This will return a list of (id,url) pairs for files in the dataset; if located at PCMDI.
    """
    sppath = pubpath.split('/')
    if sppath[2]!='data':
        print "don't recognize path as for published data"
        return None
    abs_path = '/'.join(sppath[3:])
    dataset = '.'.join(sppath[3:12])
    return get_idurls_from_dataset(dataset)

def pubpath2version(pubpath,idurls=None):
    """Input is a path of data as published at PCMDI, e.g.
    /cmip5/data/cmip5/output1/INM/inmcm4/1pctCO2/mon/atmos/Amon/r1i1p1/zg/1/zg_Amon_inmcm4_1pctCO2_r1i1p1_210001-210912.nc
    Output is a version string, or None if no version is found.
    If you want to get version numbers for multiple files from the same dataset, it will be much faster
    if first you call get_idurls_from_pubpath or get_idurls_from_dataset and the provide the result
    as the second argument of this function.
    """

    sppath = pubpath.split('/')
    if sppath[2]!='data':
        print "don't recognize path as for published data"
        return None
    abs_path = '/'.join(sppath[3:])
    dataset = '.'.join(sppath[3:12])
    if idurls==None:
        idurls = get_idurls_from_dataset(dataset)

    idu = [(a,c) for (a,c) in idurls if c.find(abs_path)>-1 ]

    versions = set([a.split('.')[9] for (a,c) in idu])
    if len(versions)==0:
        print "No match found for",abs_path
        return None
    elif len(versions)>1:
        print "Too many matches!  Version cannot be determined. idurls=",idurls
        print "abs_path=",abs_path,"  idu=",idu
        return None
    else:
        return list(versions)[0]


##    TYPE = p2p.Utils.to_obj({'DATASET':'Dataset','FILE':'File'})  # copied from the P2P class definition
##    typd=TYPE.DATASET
##    p = p2p.P2P()
##    drs_id = '.'.join(pubpath.split('/')[3:-3])
##    query = p._P2P__constraints_to_str({'drs_id':drs_id}, type=typd)
##    rsr = p.raw_search(query)
##    rsrdocs = rsr['response']['docs']
##    # ...rsrdocs will contain the url of an xml file which can be parsed, and searched for
##    # the original path.  But this job is done already by esgquery_index, so it's better to call
##    # esgquery_index than to parse the xml here.  Without parsing the xml, the best we can do is:
##    if len(rsrdocs)==0:
##        print "No dataset found"
##        return None
##    if len(rsrdocs)>1:
##        import pprint
##        print "Too many datasets!  Version cannot be determined. rsrdocs="
##        for rd in rsrdocs:
##            print rd['url'],rd['version']
##        return None
##    else:
##        return rsrdocs[0]['version']

if __name__ == '__main__':
    if len( sys.argv ) > 1:
        pubpath = sys.argv[1]
        version = pubpath2version( pubpath )
        print "The version is", version
    else:
        print "please provide a filename"


