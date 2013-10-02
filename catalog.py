#!/usr/local/cdat/bin/python
import urllib2
import xml.dom.minidom

def parseNode(node):
    """Extract information from the dataset xml node (not necessary "our" concept of dataset
    a file will be here too!) and store it in a dictionary"""
    result = {}
    prop = node.firstChild
    while prop:
        if prop.nodeType == node.ELEMENT_NODE and prop.tagName == 'property':
            result[prop.getAttribute('name')] = prop.getAttribute('value')
        #add here other metadata if required
        prop = prop.nextSibling
    
    return result

def parseCatalogLinknode(node):
    """Extract data from a link in the main catalog about datset published there
        returns: = (drs id, version number, absolute url)"""
    id, version = node.getAttribute('xlink:title').split('.v')
    version = int(version)
    return (id, version, node.getAttribute('xlink:href'))

def getDatasetMetadata(catalog):    
    """Harvest a main catalog and returs a dictionary with information about the datasets catalogs
        returns:= dictionary[drs id string][version number] = absolute url"""
    xml_str = urllib2.urlopen(catalog).read()
    dom = xml.dom.minidom.parseString(xml_str)
    catalog = dom.firstChild
    #services = catalog.getElementsByTagName('service')
    dataset = None
    
    files = []
    aggregations = []
    for node in catalog.getElementsByTagName('dataset'):
        ds = parseNode(node)
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
