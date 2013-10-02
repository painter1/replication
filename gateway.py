#!/usr/local/cdat/bin/python

from esgcet.publish.hessianlib import Hessian, ProtocolError, RemoteCallException
import xml.dom.minidom
import logging 
glog = logging.getLogger('gateway')
import re
import urllib2
import sys, getopt, drs, os.path
from pyesgf.search.connection import SearchConnection
from timeout import *  # usage: decorate a function with "@timed_out(4)" for a 4-second timeout
import socket

glog.addHandler( logging.FileHandler('gateway.log') )

#we expect to get a cmip5 DRS conform structure(heuristic wird output always respect, cmip5 not)
url_pat = re.compile("([^:]+)://([^/:]+)(:([0-9]+))?(/+.*)?(/[^/]*/output.?/.*)/([^/]+$)")
#if it's not cmip5 thisis the best we can do: first level is root, the next is part of the path
url_pat_alt = re.compile("([^:]+)://([^/:]+)(:([0-9]+))?(/+.*?)?(/[^/]*/.*)/([^/]+$)")
version_uri_pat = re.compile("^.*\.v?([0-9]*).xml")
version_name_pat = re.compile("^.*version=v?([0-9]+)")


# adapted from
# http://skyl.org/log/post/skyl/2010/04/remove-insignificant-whitespace-from-xml-string-with-python/
from StringIO import StringIO
from lxml import etree
def fix_readable_xml(dirty_xml):
    # Recently (as of Sept 2012) NCAR changed its xml files to be human-readable.
    # Unfortunately, the resulting whitespace (newlines and indentation) is incorrect
    # and breaks most automatic xml parsers.  This function will mostly undo the problem.
    parser = etree.XMLParser(remove_blank_text=True)
    tree = etree.parse(StringIO(dirty_xml), parser)
    return etree.tostring(tree.getroot())

# Given a collection name, what institutes are in it?  The collection is provided to listDatasets*()
# as its parent argument.  The collection names make sense only in the gateway system.  This list is
# to make sense of them in any CMIP5 context, so that the P2P functions can use collection information.
# With this table we can convert a collection name to a list of institute names.  This always makes sense,
# in particular in the P2P system.  Thus we can use that to restrict the range of a search, for a
# tremendous payoff in running time.  The disadvantage is that if a new institute gets into a collection,
# it would be missed until I notice error messages about it and edit this dict.
# The keyword 'ALL' applies if the institute could be anything.
coll_inst = {
    'cmip5' : ['MOHC'],
    'cmip5.output1.CAWCR.ACCESS1-0' : ['CAWCR'],
    'cmip5.output1.CSIRO-BOM.ACCESS1-0' : ['CSIRO-B)M'],
    'cmip5.output1.CSIRO-QCCCE.CSIRO-Mk3-6-0' : ['CSIRO-QCCCE'],
    'cmip5_ichec' : ['ICHEC'],
    'cmip5_ipsl' : ['IPSL'],
    'ornl.cmip5.data' : ['ALL'],
    'pcmdi.BCC' : ['BCC'],
    'pcmdi.CCCMA' : ['CCCma','CCCMA'],
    'pcmdi.CMCC' : ['CMCC'],
    'pcmdi.CNRM' : ['CNRM-CERFACS'],
    'pcmdi.DIAS' : ['MRI', 'MIROC', 'NICAM' ],
    'pcmdi.GFDL' : ['NOAA-GFDL'],
    'pcmdi.IAP' : ['LASG-CESS', 'LASG-IAP', 'PCMDI'],
    'pcmdi.IPSL' : ['IPSL'],
    'pcmdi.LUCID' : ['LUCID'],
    'pcmdi.NCC' : ['NCC'],
    'pcmdi.NCCS' : ['COLA-CFS', 'NASA-GISS', 'NASA-GMAO'],
    'pcmdi.PCMDI' : ['BNU', 'FIO', 'INM', 'IPSL', 'MOHC', 'NIMR-KMA'],  # mb sdb 'ALL'
    'pcmdi.PNNL' : ['PNNL'],
    'ucar.cgd.ccsm4.cmip5.output' : ['NCAR', 'NSF-DOE-NCAR'],
    'wdcc.MPI-M' : ['MPI-M'],
    'wdcc.replicas' : ['CCCma', 'CNRM-CERFACS', 'CSIRO-QCCCE', 'INM', 'IPSL', 'MOHC', 'NCAR', 'NCC']
    # ...mb sdb 'ALL'
    }

def listCollections_P2P(index_node_url):
   """formerly was P2P_index_node.listCollections
      The P2P system doesn't have "collections" as in the gateway system.
      So this returna a single collection which covers all collections known at the index node."""
   if index_node_url==None: return []
   index_node=index_node_url.split('/')[2]
   # ...e.g. pcmdi9.llnl.gov if url=http://pcmdi9.llnl.gov/esg-search/search
   collection_of_all = {'state': 'published', 'source_catalog_uri': '', 'id': index_node+'.ALL',\
                        'name': index_node+' All Publications'}
   collections = [collection_of_all]
   return collections

class Gateway(object):
    """Encapsulates Gateway access, and Peer-to-Peer server access"""
    
    # some constants
    esg_ns = 'http://www.earthsystemgrid.org/'
    esg_ns2 = 'http://sgf.ucar.edu/schemas/datasetList'

    _att = lambda node, att: node.getAttribute(att)

    def __attToDict(self, node):
        res = {}
        for i in range(node.attributes.length):
            att = node.attributes.item(i)
            #workaround
            if att.name == "MD5":
                res['checksum_type'] = 'md5'
                res['checksum_value'] = att.value
            else:
                res[att.name] = att.value
        return res
        

    def __init__(self, url, hessian_service='remote/hessian/guest/remoteMetadataService'):
        if url==None:
            self._url = url
        else:
            self._url = url.rstrip('/')
            self._service = Hessian('/'.join([self._url, hessian_service]))

    @timed_out(480)    # 8-minute timeout
    def __contactGateway(self, method, *args, **kwargs):
        """Encapsulate the Gateway call, so we can change it more easily"""
        #default is hessian
        if 'restFull' in kwargs:
            surl = '/'.join([self._url, method])
            try:
                url = urllib2.urlopen(surl,None,60)
            except urllib2.HTTPError as e:
                # not helpful with NCI problem: print e.read()
                print "In __contactGateway opening",surl,", exception",e
                raise e
            except Exception as e:
                print "In __contactGateway opening",surl,", exception",e
                raise e
            if 'restHeaders' in kwargs: url.headers.dict.update(kwargs['restHeaders'])
            return url.read()
        else:
            try:
                if method=="getDatasetFiles":
                    glog.debug( "in contactGateway %s %s" % ( self._url, args ) )
                    glog.debug( "about to call getattr(%s,%s)(%s)",self._service,method,*args )
                result = getattr(self._service, method)(*args)
                # glog.debug( "from contactGateway, returning %s", result )
                # normally: return getattr(self._service, method)(*args)
                return result
            except RemoteCallException as e:
                glog.error('message "'+e.message['message']+'" from url '+str(self._url))
                return None
            except socket.error as e:
                print "__contactGateway hit socket error",e," contacting",self._url
                glog.error("error exception %s contacting %s", e, self._url)
                return None
            except TimedOutExc as e:
                print "__contactGateway timed out contacting", self._url
                glog.error("timed out contacting %s", self._url)
                return None

    def __getRest(self, service, headers=None):
        url = urllib2.urlopen('/'.join([self._url, service]))
        if headers is not None: url.headers.dict.update(headers)
        return url.read()

    def getMetadata(self, parent, xml_only=False):
        """List dataset metadata from gateway."""
        xml_ans = self.__contactGateway('getDatasetMetadata', parent)
        if self._url.find('earthsystemgrid.org')>-1 or self._url.find('ncar.gov')>=1 or\
           self._url.find('ucar.edu')>-1 :
            xml_ans = fix_readable_xml( xml_ans )
        if xml_only: return xml_ans
        if xml_ans is None: return None

        result = None

        # parse answer into dom object
        dom = xml.dom.minidom.parseString(xml_ans)
        #The requested dataset is also returned, go to it before looking into its children.
        for ds in dom.firstChild.getElementsByTagNameNS(Gateway.esg_ns, 'dataset'):
            #DRS conform:
            mo = version_uri_pat.match(ds.getAttribute('source_catalog_uri'))
            #ESG publication name conform
            if not mo: version_name_pat.match(ds.getAttribute('name'))
            if mo: version = mo.group(1)
            else: version = None
            result = { 'name' : ds.getAttribute('name'), 'id' : ds.getAttribute('id'), 'state' : ds.getAttribute('state')
            ,'catalog' : ds.getAttribute('source_catalog_uri'), 'version' : version}

        return result


    def listCollections(self, xml_only=False):
        if self._url==None: return []
        xml_ans = self.__contactGateway('dataset.esgxml', restFull=True)
        if self._url.find('earthsystemgrid.org')>-1 or self._url.find('ncar.gov')>=1 or\
           self._url.find('ucar.edu')>-1 :
            xml_ans = fix_readable_xml( xml_ans )
        if xml_only: return xml_ans
        if xml_ans is None: return None
        
        dom = xml.dom.minidom.parseString(xml_ans)
        collections = []
	nodes = dom.getElementsByTagNameNS(Gateway.esg_ns, 'dataset');
	if not nodes: nodes = dom.getElementsByTagNameNS(Gateway.esg_ns2, 'dataset')
        for ds in nodes:
            collections.append(self.__attToDict(ds))

        return collections
        
    def listDatasets(self, parent, xml_only=False):
        """List all Datasets depending on the given parent."""
        if self._url==None: return []
        xml_ans = self.__contactGateway('getDatasetHierarchy', parent)
        if self._url.find('earthsystemgrid.org')>-1 or self._url.find('ncar.gov')>=1 or\
           self._url.find('ucar.edu')>-1 :
            xml_ans = fix_readable_xml( xml_ans )
        if xml_only: return xml_ans
        if xml_ans is None: return None

        result = []

        # parse answer into dom object
        dom = xml.dom.minidom.parseString(xml_ans)
        #The requested dataset is also returned, go to it before looking into its children.
        domcc = dom.firstChild.firstChild
        # jfp Sept 2012: ...This domcc works for normal gateways.  But NCAR's XML files now
        # have extra whitespace, which adds readability and also adds superfluous elements
        # once parsed:  The whitespace is parsed as text nodes each of whose data contents
        #is a string of whitespace.  The attribute isWhitespaceInElementContent cannot be
        # relied on (in tests, it's false even when it should be true).
        # So, here's a quick-and-dirty workaround:
        # probably this section is no longer needed,now that fix_readable_xml is called
        #if self._url.find('earthsystemgrid.org')>-1 or self._url.find('ncar.gov')>=1 or\
        #   self._url.find('ucar.edu')>-1 :
        #    domc = dom.firstChild
        #    if hasattr( domc.childNodes[0],'getElementsByTagNameNS'):
        #        domcc = domc.firstChild
        #    else:
        #        domcc = domc.childNodes[1]
        for ds in domcc.getElementsByTagNameNS(Gateway.esg_ns, 'dataset'):
            #DRS conform:
            mo = version_uri_pat.match(ds.getAttribute('source_catalog_uri'))
            #ESG publication name conform
            if not mo: version_name_pat.match(ds.getAttribute('name'))
            if mo: version = mo.group(1)
            else: version = None
            result.append({ 'name' : ds.getAttribute('name'), 'id' : ds.getAttribute('id'), 'state' : ds.getAttribute('state'), 'version' : version,
                        'catalog':ds.getAttribute('source_catalog_uri'), 'parent': parent})

        return result
        
    def listFiles(self, dataset, xml_only=False):
        """List files from dataset along with their metadata"""
        
        if self._url==None: return []
        xml_ans = self.__contactGateway('getDatasetFiles', dataset)
        if self._url.find('earthsystemgrid.org')>-1 or self._url.find('ncar.gov')>=1 or\
           self._url.find('ucar.edu')>-1 :
            xml_ans = fix_readable_xml( xml_ans )
        if xml_only: return xml_ans
        if xml_ans is None: return None


        # parse answer into dom object
        dom = xml.dom.minidom.parseString(xml_ans)
        endpoints_desc = {}
        for endpoints in dom.getElementsByTagNameNS(Gateway.esg_ns, 'data_access_capability'):
            endpoints_desc[endpoints.getAttribute('name')] = { 'type' : endpoints.getAttribute('type'), 'base_uri' : endpoints.getAttribute('base_uri')}
            
        print "jfp ",self._url," no. endpoints=",len(endpoints_desc)
        #if len(endpoints_desc)<=0:
            # dom.firstChild.removeChild(dom.firstChild.firstChild)
            # for endpoints in dom.getElementsByTagNameNS(Gateway.esg_ns, 'data_access_capability'):
            #     endpoints_desc[endpoints.getAttribute('name')] = { 'type' : endpoints.getAttribute('type'), 'base_uri' : endpoints.getAttribute('base_uri')}
            # print "jfp elements with tag data_access_capability:",\
            #      dom.getElementsByTagNameNS(Gateway.esg_ns, 'data_access_capability')
            # print "jfp xml_ans=",xml_ans
            # print "jfp dom=",dom.toprettyxml()
            # for node in dom.firstChild.childNodes:
            #   print "jfp dom node=",node.toprettyxml()
        files = []
        for file in dom.getElementsByTagNameNS(Gateway.esg_ns, 'file'):
            #get file metadata
            atts = self.__attToDict(file)
            atts['endpoints'] = []
            

            #get endpoint metadata (complete url with base name)
            path = None
            for ep in file.getElementsByTagNameNS(Gateway.esg_ns, 'file_access_point'):
                name = ep.getAttribute('data_access_capability')  #e.g. "HTTPServer"
                url = endpoints_desc[name]['base_uri'] + ep.getAttribute('uri')

                #we expect to get a cmip5 DRS conform structure
                mo = url_pat.match(url)
                if not mo:
                    #try once more with an alternative less precise regexp
                    mo= url_pat_alt.match(url)
                    if not mo:  raise Exception("Unexpected endpoint url {0}".format(url))
                (protocol, server, port, root, path, file_name) = mo.group(1,2,4,5,6,7)
                if path[0] == '/': path = path[1:]            
                atts['endpoints'].append({'name': name, 
                    'type' : endpoints_desc[name]['type'],
                    'protocol': protocol,
                    'server' : server,
                    'port' : port,
                    'root' : root,
                    'path' : path,
                    'url': url})
            #get the path from one of the endpoints, it should be all the same
            atts['path'] = path
            #get the checksum info if present
            tmp_chksum = None
            for ep in file.getElementsByTagNameNS(Gateway.esg_ns, 'checksum'):
                tmp_chksum = (ep.getAttribute('algorithm').lower(), ep.getAttribute('value')) 
                #if possible get the md5
                if tmp_chksum[0] == 'md5': break
            if tmp_chksum:
                atts['checksum_type'], atts['checksum_value'] = tmp_chksum

            files.append(atts)
            
        return files
        
class P2P_index_node(Gateway):

    def __init__(self, url, hessian_service='remote/hessian/guest/remoteMetadataService'):
        Gateway.__init__(self,url,hessian_service)
        self._conn = SearchConnection(self._url)

    def getMetadata(self, parent, xml_only=False):
        """List metadata, P2P version
         parent is the DRS name of a dataset, e.g. CMIP5.output1.IPSL.historical.mon.atmos.Amon.r1i1p1.
         This method does not support a parent which is the name of a collection, e.g. pcmdi.IPSL."""
        glog.debug("in P2P getMetadata, url=%s, parent=%s, xml_only=%s",self._url, parent, xml_only)
        if self._url==None: return []
        if xml_only: raise Exception("P2P_index_node.listDatasets cannot generate xml")

        re_name =  re.compile('http://([^/]*)')
        mo = re_name.match(self._url)
        p2pserver = mo.group(1)
        facets = parent.split('.')
        dataset = parent
        ctx = self._conn.new_context(id=dataset,latest=True,project=facets[0].upper(),\
                                     product=facets[1],institute=facets[2],\
                                     model=facets[3],experiment=facets[4],time_frequency=facets[5],\
                                     realm=facets[6],cmor_table=facets[7],ensemble=facets[8] )
        dataset_search_results = ctx.search(data_node=p2pserver)
        if len(dataset_search_results)==0:
            return None
        if len(dataset_search_results)>1:
            glog.warn("in P2P getMetadata, more than one dataset found matching %s, will use just one.",
                      parent)
        dsj = dataset_search_results[0].json
        #  >>> TO DO: what if keys not in dsj, probably need a defaultdict <<<<<
        for dsjurl in dsj['url']:  # this section is similar to that of listDatasets
            if dsjurl.find('.xml')>-1:
                cat = dsjurl
            name = dsj['title']
            id = dsj['master_id']
            state = 'published'
            catalog = cat[:cat.find('.xml')+4]
            version = dsj['id'].split('|')[0].split('.')[-1],

        result = { 'name':name, 'id':id, 'state':state, 'catalog':catalog, 'version':version }
        return  result

    def listCollections(self, xml_only=False):
        """like listCollections() but uses the peer-to-peer system and hence the collection
           name is less informative, e.g. 'pcmdi.ALL', or, more generally, 'index_node.ALL'"""
        # This is be plug-compatible.  That is, it will return a gateway-style collection name,
        # even though the P2P system uses the different concept of index_node + data_node.
        if xml_only: raise Exception("P2P_index_node.listCollections cannot generate xml")
        # a bare function (not a method) is needed for processGatewayOld, so we'll just call it here:
        return listCollections_P2P(self._url)
        
    def listDatasets(self, parent, xml_only=False):
        """List all Datasets depending on the given parent, using the P2P system."""
        
        if self._url==None: return []
        if xml_only: raise Exception("P2P_index_node.listDatasets cannot generate xml")
        print "jfp in P2P's listDatasets _url=",self._url,"  parent=",parent

        # Speed is a problem with the new_context() call.  Constrain it as much as possible.
        if parent in coll_inst.keys():    # collection came from the gateway system
            institutes = coll_inst[parent]
        else:    # parent collection not known, maybe came from the P2P system
            if parent.split('.')[-1]!='ALL':   # parent collection came from the gateway system but is unknown
                glog.error("no gateway inst for %s",parent)
            institutes=['ALL']
        for institute in institutes:
            if institute=='ALL':
                ctx = self._conn.new_context(project='CMIP5',product='output1')
            else:
                ctx = self._conn.new_context(project='CMIP5',product='output1',institute=institute)
            dataset_search_results = ctx.search()
            print "jfp len(dataset_search_results)=",len(dataset_search_results)
            results = []
            for ds in dataset_search_results:
                dsj = ds.json
                for dsjurl in dsj['url']:
                    if dsjurl.find('.xml')>-1:
                        cat = dsjurl
                        # example of cat.  There may be others in dsjurl, e.g. an application/las
                        # 'http://pcmdi9.llnl.gov/thredds/esgcet/8/cmip5.output1.IPSL.IPSL-CM5A-LR.1pctCO2.fx.atmos.fx.r0i0p0.v20120430.xml#cmip5.output1.IPSL.IPSL-CM5A-LR.1pctCO2.fx.atmos.fx.r0i0p0.v20120430|application/xml+thredds|Catalog'
                results.append({
                    'parent': parent,
                    'catalog' : cat[:cat.find('.xml')+4],
                    'id' :  dsj['master_id'],
                    'version':  dsj['id'].split('|')[0].split('.')[-1],
                    'state': 'published',
                    'name': dsj['title'],
                    })
        return results

    def listFiles(self, dataset, xml_only=False):
        """List files from dataset along with their metadata, using the P2P system."""
        
        if self._url==None: return []
        if xml_only: raise Exception("P2P's listFiles cannot generate xml")

        re_name =  re.compile('http://([^/]*)')
        mo = re_name.match(self._url)
        p2pserver = mo.group(1)
        # ...e.g. p2pserver='pcmdi9.llnl.gov' if  _url='http://pcmdi9.llnl.gov/esg-search/search'
        # conn = SearchConnection('http://pcmdi9.llnl.gov/esg-search/search')
        # p2pserver = 'pcmdi9.llnl.gov'
        print "jfp in P2P's listFiles, _url=",self._url,"  dataset=",dataset
        # Note: the id does not serve as a constraint in new_context; you have to specify each facet.
        facets=dataset.split('.')    # uppercase the project because it should be CMIP5, not cmip5...
        ctx = self._conn.new_context(id=dataset,latest=True,project=facets[0].upper(),\
                               product=facets[1],institute=facets[2],\
                               model=facets[3],experiment=facets[4],time_frequency=facets[5],\
                               realm=facets[6],cmor_table=facets[7],ensemble=facets[8] )

        files = []
        # Get the datasets.  For now, restrict the search to data on the selected P2P server.
        # Without "data_node=p2pserver", we'd get more results (replicas, e.g.) which should be merged.
        # This would be useful if we were to upgrade the replication scripts to automatically deal with a
        # troublesome server by trying an alternative.
        dataset_search_results = ctx.search(data_node=p2pserver)
        for ds in dataset_search_results:
            file_results = ds.files_context().search()
            for f in file_results:
                # Now translate the f.json dict to atts, also a dict.  The point is to have
                # the same name:value pairs as the gateway system would have generated.
                atts={}
                endpoints = f.json['url'] # list of strings like url+'|application/gridftp|GridFTP'
                atts['endpoints']=[]
                for ep in endpoints:
                    print "jfp ep=",ep
                    epl = ep.split('|')
                    name = epl[2]                 #e.g. "HTTPServer","GridFTP","OPeNDAP"
                    type = name                   #e.g. "HTTPServer","GridFTP","OPeNDAP"
                    # eplapp = epl[1].split('/')[1] # strips 'application' from, e.g. 'application/netcdf'
                    url = epl[0]
                    protocol = url[:url.find('://')]    #e.g. "http","gridftp"
                    #...e.g. "gsiftp://vetsman.ucar.edu:2811//datazone/cmip5_data/cmip5/output1/NSF-DOE-NCAR/CESM1-FASTCHEM/historical/mon/atmos/Amon/r1i1p1/v20120522/ch4/ch4_Amon_CESM1-FASTCHEM_historical_r1i1p1_185001-189912.nc"
                    urlsplit = url.split('/')
                    serverport = urlsplit[2].split(':') # e.g. ['vetsman.ucar.edu','2811']
                    server = serverport[0]              #e.g. "vetsman.ucar.edu"
                    if len(serverport)==1:
                        port = None                     # means unspecified
                    else:
                        port = serverport[1]            #e.g. "2811"
                    fullpath = '/'+'/'.join(urlsplit[3:])
                    fullpath = os.path.dirname(fullpath) # forget the filename
                    nroot = fullpath.find('/cmip5/output')+1
                    root = fullpath[:nroot]             #e.g. "/datazone/cmip5_data/"
                    path = fullpath[nroot:]             #e.g. "cmip5_data/cmip5/output1..."
                    atts['endpoints'].append({'name': name, 
                                              'type' : type,
                                              'protocol': protocol,
                                              'server' : server,
                                              'port' : port,
                                              'root' : root,
                                              'path' : path,
                                              'url': url})
                #get the path from one of the endpoints, it should be all the same
                atts['path'] = path
                if 'checksum' in f.json.keys():
                    atts['checksum_value'] = f.json['checksum']
                    atts['checksum_type'] = f.json['checksum_type']
                else:     # sometimes the server doesn't have a checksum
                    atts['checksum_value'] = 'DUMMY'
                    atts['checksum_type'] = 'md5'
                atts['name'] = f.json['title'] #e.g. 'ch4_Amon...-189912.nc'
                atts['size'] = f.json['size']  # file size in bytes
                files.append(atts)
            
        return files
        
        


GW = {
    'WDCC' : { 'url' : 'http://ipcc-ar5.dkrz.de', 'collections' : ['wdcc.replicas', 'wdcc.MPI-M']},
    'BADC' : { 'url' : 'http://cmip-gw.badc.rl.ac.uk', 'collections' : ['cmip5', 'cmip5_ipsl']},
    'JPL' : { 'url' : 'http://esg-gateway.jpl.nasa.gov', 'collections' : []},
    'PCMDI': { 'url' : 'http://pcmdi3.llnl.gov/esgcet', 'collections' : ['pcmdi.CNRM','pcmdi.PCMDI','pcmdi.NCC','pcmdi.CCCMA','pcmdi.BCC','pcmdi.NCCS', 'pcmdi.DIAS', 'pcmdi.GFDL']},
    'NCAR' : { 'url' : 'http://www.earthsystemgrid.org', 'collections' : ['ucar.cgd.ccsm4.cmip5.output']},
    'NERSC' : { 'url' : 'http://esg.nersc.gov/esgcet', 'collections' : ['nersc.cmip5']},
    'ORNL': { 'url' : 'http://esg2-gw.ccs.ornl.gov/esgcet', 'collections' : []},
    'NCI': { 'url' : 'http://esg.nci.org.au/esgcet', 'collections' : ['cmip5.output1.CSIRO-QCCCE.CSIRO-Mk3-6-0','cmip5.output2.CSIRO-QCCCE.CSIRO-Mk3-6-0']},
    'WDCC2' : { 'url' :'http://albedo2.dkrz.de/esgcet', 'collections' : ['wdcc2.test'] } 
}
P2PS = {
    'PCMDI': { 'url' : 'http://pcmdi9.llnl.gov/esg-search/search', 'collections' : GW['PCMDI']['collections'] },
    'WDCC' : { 'url' : 'http://euclipse1.dkrz.de/esg-search/search', 'collections' : GW['WDCC']['collections'] },
    'WDCC2' : { 'url' : 'http://esgf-data.dkrz.de/esg-search/search', 'collections' : GW['WDCC']['collections'] },
    'BADC' : { 'url' : 'http://esgf-index1.ceda.ac.uk/esg-search/search', 'collections' : GW['BADC']['collections'] },
    'JPL' : { 'url' : 'http://esg-datanode.jpl.nasa.gov/esg-search/search', 'collections' : GW['JPL']['collections'] },
    'NASA-GSFC' : { 'url' : 'http://esgf.nccs.nasa.gov/esg-search/search', 'collections' : [] },
    # url not known 'NCAR' : { 'url' : 'http://www.earthsystemgrid.org******', 'collections' : GW['NCAR']['collections'] },
    # url not known 'NERSC' : { 'url' : 'http://esg.nersc.gov/esgcet******', 'collections' : GW['NERSC']['collections'] },
    'ORNL': { 'url' : 'http://esg.ccs.ornl.gov/esg-search/search', 'collections' : GW['ORNL']['collections'] },
    'NCI': { 'url' : 'http://esg2.nci.org.au/esg-search/search', 'collections' : GW['NCI']['collections'] },
    'CMCC': { 'url' : 'http://adm07.cmcc.it/esg-search/search', 'collections' : [] }, # not responsive 2012.10.03
    'IPSL': { 'url' : 'http://esgf-node.ipsl.fr/esg-search/search', 'collections' : [] },
    }
name = re.compile('http://([^/]*)')
for g in GW: 
    GW[g]['name'] = g
    GW[g]['server'] = name.match(GW[g]['url']).group(1)
for g in P2PS: 
    P2PS[g]['name'] = g
    P2PS[g]['server'] = name.match(P2PS[g]['url']).group(1)

usage="""gateway.py [opt]
Opt:
    -h, --help  : show this help
    -g          : define Gateway (name) {0}
    -s          : define P2P server (name) {0}
    --gateway-url: define Hessian API endpoit (either this or -g is required)
    -c          : list all top level collections (only from Gateway 1.3.1+)
    --parent    : define a parent dataset for action
    -d          : retrieve all datasets from a given parent
    -D          : retrieve all datasets from a given parent and its version (dataset_id#version format)
    -A          : retrieve all datasets from all knwon top level collections (parents)
    -f          : list files from parent dataset
    -m          : retrieve metadata from parent dataset
    
    -x          : retrieve response xml
    -o          : retrieve response dictionary

    -v          : verbose mode
    -q          : quiet mode
    (default)   : print results to stdout
""".format(GW.keys(),P2PS.keys())
def main(argv=None):
    if argv is None: argv = sys.argv[1:]
    #jfp    logging.basicConfig(level=logging.ERROR, format='%(levelname)s: %(message)s')

    try:
        args, lastargs = getopt.getopt(argv, "hg:fdDcxovAm", ['help', 'parent=', 'apply='])
    except getopt.error:
        print sys.exc_info()[:3]
        return 1

    #init values
    P2PURL = P2PName = gatewayURL = gatewayName = parent_dataset = to_apply = None
    all_datasets = list_files = list_datasets = list_datasets_version = list_collections = get_metadata = retrieve_xml = retrieve_object = verbose = False

    #parse arguments
    for flag, arg in args:
        if flag=='-g':
            if arg.find('.') != -1:
                #probably a server name, see if we know it
                for g in GW:
                    if GW[g]['server'] == arg:
                        arg = g
                        break
            gatewayName = arg
            if arg in GW: gatewayURL = GW[arg]['url']
            else: 
                print "Unknown Gateway {0}".format(arg)
                glog.error( "Unknown Gateway {0}".format(arg) )
                return 2
        if flag=='-s':
            if arg.find('.') != -1:
                #probably a server name, see if we know it
                for s in P2PS:
                    if P2PS[s]['server'] == arg:
                        arg = s
                        break
            P2PName = arg
            if arg in P2PS: P2PURL = P2PS[arg]['url']
            else: 
                print "Unknown P2P Server {0}".format(arg)
                glog.error( "Unknown P2PServer {0}".format(arg) )
                return 2
        elif flag=='--gateway-url': gatewayURL = arg
        elif flag=='--parent':      parent_dataset = arg
        elif flag=='--apply':       to_apply = arg
        elif flag=='-A':            all_datasets = True
        elif flag=='-f':            list_files = True
        elif flag=='-d':            list_datasets = True
        elif flag=='-D':            list_datasets_version = True
        elif flag=='-c':            list_collections = True
        elif flag=='-m':            get_metadata = True

        elif flag=='-x':            retrieve_xml = True
        elif flag=='-o':            retrieve_object = True

#        elif flag=='-d':            glog.setLevel(logging.DEBUG)
#jfp        elif flag=='-v':            glog.setLevel(logging.INFO)
#jfp        elif flag=='-q':            glog.setLevel(logging.NONE)
        elif flag=='-h' or flag=='--help':
            print usage
            return 0 
        
    
        
    glog.setLevel(logging.DEBUG)  #jfp
    glog.debug( "jfp gateway.main(), just parsed arguments, P2PURL=%s, gatewayURL=%s, args=%s",\
                P2PURL,gatewayURL,argv )
    glog.info( "jfp gateway.main(), just parsed arguments, P2PURL=%s, gatewayURL=%s, args=%s",\
                P2PURL,gatewayURL,argv )

    #check that we have URLs.  If no P2P url, try to get it from the gateway
    if not gatewayURL:
        glog.info( "Missing Gateway Url" )
    if not P2PURL:  # try to get it from the gateway name
        for s in P2PS:
            if P2PS[s]['name'] == gatewayName:
                P2PName = gatewayName
                P2PURL = P2PS[P2PName]['url']
                break
    if not P2PURL:
        glog.info( "Missing P2P url, Gateway url is", gatewayURL )
    if not gatewayURL and not P2PURL:
        return 2

    #start processing request
    g = Gateway(gatewayURL)
    p2ps = P2P_index_node(P2PURL)
    #default attribute id for extracting values from object
    getAtt = lambda var: var['id']
    results = None

    if list_collections:
        results = g.listCollections(xml_only=retrieve_xml)
        resultsP2P = p2ps.listCollections()
        glog.debug( "jfp Gateway.listCollections %s: %s", gatewayURL, results )
        glog.debug( "jfp P2P_index_node.listCollections %s: %s", P2PURL, resultsP2P )

    if list_datasets or list_datasets_version:
        if not parent_dataset:
            print "Missing parent dataset, use --parent to set."
            return 2
        if list_datasets_version: getAtt = lambda var: '#'.join([var['id'], var['version']])
        results = g.listDatasets(parent_dataset, xml_only=retrieve_xml)
        if results==None: results=[]
        #print "jfp Gateway.listDatasets ",len(results),"results."
        #glog.debug( "jfp Gateway.listDatasets %s results for %s",len(results),parent_dataset)
        #if  len(results)>0:  #jfp testing
        #    print "jfp results[0]=",results[0]
        #resultsP2P = p2ps.listDatasets(parent_dataset, xml_only=retrieve_xml)
        #if resultsP2P==None: resultsP2P=[]
        #print "jfp P2P_index_nodelistDatasets ",len(resultsP2P),"results."
        #glog.debug( "jfp P2P_index_node.listDatasets %s results for %s",len(resultsP2P),parent_dataset)
        #if len(resultsP2P)>0:  #jfp testing
        #    print "jfp resultsP2P[0]=",resultsP2P[0]

    if list_files:
        if not parent_dataset:
            print "Missing parent dataset, use --parent to set."
            return 2
        results = g.listFiles(parent_dataset, xml_only=retrieve_xml)
        #resultsP2P = p2ps.listFiles(parent_dataset,xml_only=retrieve_xml) #jfp testing
        ##print "jfp Gateway.listFiles results   =",results
        ##print "jfp P2P_index_node.listFiles results=",resultsP2P
        getAtt = lambda var: '/'.join([var['path'], var['name']])

    if all_datasets:
        #try to get all collections
        collections = []
        try:
            collections = [c['id'] for c in g.listCollections()]
        except:
            glog.warn("Couldn't retrieve top-level collections. Falling back to hard-coded list")

        if not collections and gatewayName in GW: col = GW[gatewayName]['collections']
        results = []
        for col in collections:
            results.extend(g.listDatasets(col))

    if get_metadata:
        #this is no list, it's just metadata from given parent
        if not parent_dataset: 
            print "Missing parent dataset, use --parent to set."
            return 2
        results = g.getMetadata(parent_dataset, xml_only=retrieve_xml)
        #if results and results['name'].find('historical.mon.atmos')>-1:
        #glog.debug("jfp g.getMetadata results=%s",results)
        #resultsP2P = p2ps.getMetadata(parent_dataset, xml_only=retrieve_xml)
        #if resultsP2P and resultsP2P['name'].find('historical.mon.atmos')>-1:
        #glog.debug("jfp p2ps.getMetadata results=%s from url=%s",resultsP2P,P2PURL)
        if not ( retrieve_xml| retrieve_object): return results['catalog']

    if retrieve_object: return results
    if results: return __toStdout(results, not (retrieve_xml | retrieve_object), getAtt, to_apply, verbose=verbose)
    

    return 0

def __toStdout(result, iterate=False, get_attr=None, apply=None, verbose=None):
    if not result:
        if verbose: print "Nothing found"
        return 0

    if apply:
        eval(apply)
        return 

    if iterate:
        for r in result: 
            if get_attr: print get_attr(r)
            else: print r
    else:
        print result
    return 0

def getKnownGateways():
    return GW.keys()

def getGatewayInfo(gateway=None):
    if not gateway: 
        import copy
        return copy.deepcopy(GW)
    if gateway in GW: return GW[gateway].copy()
    else: return None

def getCurrentDatasets(gateways=None, filter_datasets=None, callback_result=None, continue_on_errors=False):
    """Returns a tupple (dataset (dict), files [(dict)], Gateway) for all dataset published at all known
        top level collections from all known gateways.
        filter_datasets : call_back for filtering datasets before being further processed
        """
    from drs import DRS
    if gateways:
        if isinstance(gateways, str): gateways = [gateways]
        known_gateways = gateways
    else: known_gateways = ['WDCC', 'BADC', 'PCMDI', 'NCI']

    result = set()

    for gateway in known_gateways:
        glog.debug('Analyzing gateway: %s', gateway)
        #get collections
        # -- This is turned off as it's not simple to get which are CMIP5 and which not!! --
#        cmd = '-g {0} -c'.format(gateway)
#        glog.debug('Running cmd: %s', cmd)
#        try:
#            collections = main(cmd.split(' '))
#        except:
#            glog.warn("Couldn't retrieve top-level collections. Falling back to hard-coded list")
#            
#        if not collections: collections = GW[gateway]['collections']
        collections = GW[gateway]['collections']
        
        for collection in collections:
            glog.debug('Collection: %s', collection)
            cmd = '-g {0} -od --parent {1}'.format(gateway, collection)
            glog.debug('Running cmd: %s', cmd)
            datasets = main(cmd.split(' '))
            all_drs = {}

            #get the ids only (and only if marked as published
            datasets = filter(lambda d: d['state']=='published', datasets)

            #we might filter or sort them out.            
            if filter_datasets: 
                old = len(datasets)
                datasets = filter_datasets(datasets, GW[gateway])
                glog.debug('Datasets filter to %s, from %s', len(datasets), old)
            
            for dataset in datasets:
                cmd = '-g {0} -of --parent {1}'.format(gateway, dataset['id'])
                glog.debug('Running cmd: %s', cmd)
                try:
                    #don't stop if something doesn't work
                    files = main(cmd.split(' '))
                    #skip if empty
                    if not files: continue
                    
                    #drs = DRS(path=files[0]['path'], id=dataset)
                    entry = (dataset, files, GW[gateway])
                    if callback_result:
                        callback_result(*entry)
                    else:
                        result.add(entry)
                    
                except:
                    glog.error('skipping: %s - exception: %s', dataset, sys.exc_info()[:2])
                    if continue_on_errors: continue
                    else: raise 
        

    return list(result)

def createKML():
    # *** This is broken as of now. ****
    raise Exception('Method is broken')

    import kml as kml_lib
    kml = kml_lib.cmip5()

    set_gateways = set(['BADC', 'PCMDI'])
    set_institutes = set()
    set_servers = set()
    set_inst_to_server = set()
    set_server_to_gateway = set()
    
    for gateway in set_gateways:
        glog.debug('Analyzing gateway: %s', gateway)
        for collection in GW[gateway]['collections']:
            glog.debug('Collection: %s', collection)
            cmd = '-g {0} -od --parent {1}'.format(gateway, collection)
            glog.debug('Running cmd: %s', cmd)
            res = main(cmd.split(' '))
            all_drs = {}
            from drs import DRS
            for r in res:
                drs = DRS(id=r['id'])
                if drs.institute not in all_drs:
                    glog.debug('New Institute: %s', drs.institute)
                    set_institutes.add(drs.institute)
                    cmd = '-g {0} -of --parent {1}'.format(gateway, drs.getId())
                    glog.debug('Running cmd: %s', cmd)
                    res = main(cmd.split(' '))
                    #representative file
                    servers = []
                    all_drs[drs.institute] = {'file': res[0], 'drs':drs, 'server':servers}
                    #get al known servers for this file
                    for endpoint in res[0]['endpoints']:
                        if endpoint['server'] not in servers:
                            servers.append(endpoint)
                            set_servers.add(endpoint['server'])
                            set_inst_to_server.add((drs.institute, endpoint['server']))
                            set_server_to_gateway.add((endpoint['server'], gateway))


    print set_gateways, set_institutes, set_servers, set_inst_to_server, set_server_to_gateway
    from geo_utils import GeoDB
    import re
    pat = re.compile('^[^/]*//([^/]*)(/.*)?$')
    g = GeoDB('sqlite:///geo.db')
    t = lambda **dic: dic

#    for gateway in set_gateways:
#        url = pat.match(GW[gateway]['url']).group(1)
#        geo = GeoIPDAO(name=url)
#        geo = g.addAll([geo])[0]
#        kml['placemarks'].append(t(name='ESG-' + gateway,description=url,style='gateway',longitude=geo.lon, latitude=geo.lat))
#
#    for server in set_servers:
#        url = server
#        geo = GeoIPDAO(name=url)
#        geo = g.addAll([geo])[0]
#        kml['placemarks'].append(t(name='DN@' + server,description=url,style='node',longitude=geo.lon, latitude=geo.lat))
#
#    for server, gateway in set_server_to_gateway:
#        url_server = server
#        geo_server = g.addAll([GeoIPDAO(name=url_server)])[0]
#        url_gw = pat.match(GW[gateway]['url']).group(1)
#        geo_gw = g.addAll([GeoIPDAO(name=url_gw)])[0]
#        kml['lines'].append(t(name='DN@{0} -> ESG-{1}'.format(server, gateway),style='nodeToGateway',points=[t(longitude=geo_server.lon, latitude=geo_server.lat), t(longitude=geo_gw.lon, latitude=geo_gw.lat)]))

    print kml.getXML().toprettyxml('','')


if __name__ == '__main__':
    #configure logging
    #jfp was logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    #jfp was glog.setLevel(logging.WARN)
    glog.setLevel(logging.DEBUG)

#    createKML()
#    sys.exit(0)

    ret_val = main()
    if isinstance(ret_val, int) and ret_val != 0: print usage
    sys.exit(ret_val)


    #__getCollections()
    #datasets = filter(lambda ds: ds['id'].startswith('cmip5.output1') , __getDatasets())
    #for ds in datasets: print ds['id']

