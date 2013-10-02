#!/usr/local/cdat/bin/python
import urllib2
import xml.dom.minidom
import re

dataset_pat = re.compile('^(.*)\.v([0-9]*)$')
def parseNode(node, services=[]):
    """Extract information from the dataset xml node (not necessary "our" concept of dataset
    a file will be here too!) and store it in a dictionary"""
    result = {}

    if node.hasAttribute('urlPath'):
        #there's a default access... who designed this schema?!...
        if 'access' not in  result: result['access'] = []
        service_name = node.getElementsByTagName('serviceName')[0].firstChild.nodeValue
        if service_name in services: service = services[service_name]
        else: service = {'url_base' : '', 'type': service_name}
        access_data = {'url': service['url_base'] + node.getAttribute('urlPath'),
                            'type': service['type'],
                            'format': ''} #<-- Unknown for default service (?!!)
        result['access'].append(access_data)

    prop = node.firstChild
    while prop:
        #add all properties
        if prop.nodeType == node.ELEMENT_NODE and prop.tagName == 'property':
            result[prop.getAttribute('name')] = prop.getAttribute('value')
        #add file access
        elif prop.nodeType == node.ELEMENT_NODE and prop.tagName == 'access':
            if 'access' not in  result: result['access'] = []
            service_name= prop.getAttribute('serviceName')
            if service_name in services:
                #resolve properly
                service = services[service_name]
            else:
                #if we can't resolve then do the best we can... (I don't think this is a valid TDS)
                service = {'url_base' : '', 'type': service_name}

            access_data = {'url': service['url_base'] + prop.getAttribute('urlPath'), 
                            'type': service['type'],
                            'format':prop.getAttribute('dataFormat')}
            result['access'].append(access_data)
        #add here other metadata if required
        prop = prop.nextSibling
    
    return result

def parseCatalogLinknode(node):
    """Extract data from a link in the main catalog about datset published there
        returns: = (drs id, version number, absolute url)"""
    mo = dataset_pat.match(node.getAttribute('xlink:title'))
    if mo:
        id, version = mo.groups()
        version = int(version)
        return (id, version, node.getAttribute('xlink:href'))
    else:
        print "Can't parse %s" % node.getAttribute('xlink:title')
        return (None, None, None)

def findCatalog(root_cat, dataset_name, dataset_version):
    """Find the catalog to the given datasete and version from the root catalog
        It expects a normal ESGF TDS catalog"""
    c = urllib2.urlopen(root_cat)
    if c.getcode() != 200: return None
    dom = xml.dom.minidom.parseString(c.read())
    catalog = dom.firstChild

    for node in catalog.getElementsByTagName('catalogRef'):
        dataset, version, url = parseCatalogLinknode(node)
        if dataset == dataset_name and version == dataset_version: 
            return '%s/%s' % (root_cat[:root_cat.rindex('/')], url)

def findRootCatalog(catalog):
    parts = catalog.split('/')
    if len(parts) == 7 and parts[3] == 'thredds' and parts[4] == 'esgcet':
        return '/'.join(parts[:5] + ["catalog.xml"])
    raise Exception("Can't infer root catalog. Unknown format.")

def getDatasetMetadata(catalog_url):    
    """Harvest a main catalog and returs a dictionary with information about the datasets catalogs
        returns:= dictionary[drs id string][version number] = absolute url"""
    try:
        xml_str = urllib2.urlopen(catalog_url).read()
    except:
        # TODO: use exception, write log
        #print 'error', catalog_url
        return None
    dom = xml.dom.minidom.parseString(xml_str)
    catalog = dom.firstChild

    #parse services and resolve to absolute url
    server_root = catalog_url[:catalog_url.index('/',10)]
    services = {}
    for service in catalog.getElementsByTagName('service'):
        type = service.getAttribute('serviceType')

        #don't consider compound services
        if type.lower() == 'compound': continue

        url_base = service.getAttribute('base')
        if url_base[0] == '/': url_base = server_root + url_base  
        services[service.getAttribute('name')] = {'type': type, 'url_base': url_base}
    dataset = None
    
    files = []
    aggregations = []
    for node in catalog.getElementsByTagName('dataset'):
        ds = parseNode(node, services)
        if node.parentNode == catalog:
            #this is the main node
            dataset = ds
        elif 'file_id' in ds: files.append(ds)
        elif 'aggregation_id' in ds: aggregations.append(ds)
        
    return {'dataset' : dataset, 'files' : files, 'aggregations' : aggregations}

def getAllCatalogs(main_catalog_url):
    """Harvest a main catalog and returs a dictionary with information about the datasets catalogs
        returns:= dictionary[drs id string][version number] = absolute url"""
 
    catalog_base = main_catalog_url[:main_catalog_url.rindex('/')]
    
    xml_str = urllib2.urlopen(main_catalog_url).read()
    dom = xml.dom.minidom.parseString(xml_str)
    catalog = dom.firstChild
    datasets = {}
    
    for node in catalog.getElementsByTagName('catalogRef'):
        dataset, version, url = parseCatalogLinknode(node)
        if dataset not in datasets: datasets[dataset] = {}
        
        datasets[dataset][version] = '%s/%s' % (catalog_base, url)
    
    return datasets
