#!/usr/local/cdat/bin/python
"""Handles the efficient harvesting for all cmip5 datasets world-wide"""

import sqlalchemy
import os,sys
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, Boolean, DateTime, sql, ForeignKeyConstraint, orm
import time, datetime
import logging

from replication.model import p2p

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__) 

Base = declarative_base()

##WORK AROUNDS---
# Set _database to "postgres", "mysql", or "sqllite" to avoid reading the config file twice
_database = None
if _database is None:
    import metaconfig
    _conf = metaconfig.get_config('cmip5_status')
    _dburl = _conf.get('db','dburl')
    _database = _dburl.split('://')[0]

# For Postgres:
if _database.startswith("postgres"):
    try:
        from sqlalchemy.dialects.postgresql import BIGINT as MyBigInteger
    except:
        from sqlalchemy.databases.postgres import PGBigInteger as MyBigInteger
# For MySQL:
elif _database=="mysql":
    from sqlalchemy.databases.mysql import MSBigInteger as MyBigInteger, MSDouble as MyDouble
# For SQLlite:
elif _database=="sqllite":
    MyBigInteger = types.Integer
else:
    raise Exception("No database defined in ~/.metaconfig.conf or otherwise.")

#For timestamps in PCMDI since atomfeed is not working.
dummy_date = datetime.datetime(2011,10,10,7,7,57)
##---


class DAO(object):
    """Some basic functinality for Persistent Objects"""
    def __str__(self):
        """Short representation, only primary keys"""
        return "<{0} id:{1}>".format(self.__class__.__name__, "".join(orm.object_mapper(self).primary_key_from_instance(self)))

    def __repr__(self):
        """Complete representation of this object."""
        return "<{0} ({1})>".format(self.__class__.__name__, \
            ",".join(map(lambda k: k+"="+str(self.__dict__[k]), filter(lambda name: name[0] != '_', self.__dict__))))

class Dataset(Base, DAO):
    """Store Datasets for a persistance view"""
    __tablename__ = 'datasets'
    __table_args__ = {'schema':'p2p_cmip5'}

    FIELDS_STR = 'data_node,index_node,master_id,version,url,timestamp,size,number_of_files,replica'
    FIELDS = FIELDS_STR.split(',')

    catalog = Column(String, primary_key=True)  #TDS catalog link
    id = Column(String, nullable=False, index=True)         #DRS id
    version = Column(Integer, nullable=False)   #version number
    access_http = Column(Boolean, default=True)     #if there's http access configured
    access_opendap = Column(Boolean, default=False) #if there's opendap configured
    access_gridftp = Column(Boolean, default=False) #if there's gridftp access configured
    
    data_node = Column(String, nullable=False, index=True)  #data node holding the data 
                                                            #(should be same as in catalog!)
    index_node = Column(String, nullable=False)             #index_node where this is located
    
    replica = Column(Boolean)

    size = Column(MyBigInteger)
    filecount = Column(Integer)
    
    creationtime = Column(DateTime, nullable=False, index=True)
    last_check = Column(DateTime, nullable=False, index=True, default=sqlalchemy.func.current_timestamp())


    @staticmethod
    def instance(p2p_response):
        """Generates a Dataset object out of a P2P Search response. Or required data must be available!"""

        #use default values for those missing
        p2p_result = {'number_of_files':None,'size':None}
        p2p_result.update(p2p_response)

        if not all(k in p2p_result for k in Dataset.FIELDS):
            raise Exception("Cannot create object from the given attributes")

        #extract catalog info
        catalog = None
        if 'url' in p2p_result:
            candidates = [catalog for catalog in p2p_result['url'] if catalog[-7:] == 'Catalog']
            if len(candidates) == 1:
                catalog = candidates[0].split('.xml')[0] + '.xml'
        return Dataset(catalog=catalog, id=p2p_result['master_id'], version=int(p2p_result['version']),
                data_node=p2p_result['data_node'], index_node=p2p_result['index_node'], 
                replica=p2p_result['replica'], size=p2p_result['size'], 
                filecount=p2p_result['number_of_files'], creationtime=p2p_result['timestamp'])

    @staticmethod
    def insert(p2p_results):
        """Insert all results from the path p2p_result (or list) into the current session 
(Does not commit them!)"""
        if isinstance(p2p_results, dict): p2p_results = [p2p_results]
        for dataset in p2p_results:
            try:
                ds = Dataset.instance(dataset)
                if ds:
                    getSession().add(ds)
            except:
                log.error('Skipping dataset: ' + ('id' in dataset and dataset['id'] or 'dataset'))

    @staticmethod
    def update(p2p_results):
        """Updates (all inserts) all results from the path p2p_result (or list) into the current session
(Does not commit them!)"""
        if isinstance(p2p_results, dict): p2p_results = [p2p_results]
        for dataset in p2p_results:
            try:
                ds = Dataset.instance(dataset)
                if ds:
                    _ = getSession().merge(ds)
            except:
                log.error('Skipping dataset: ' + ('id' in dataset and dataset['id'] or 'dataset'))


_db = None
def getSession():
    global _db
    if not _db:
        e = sqlalchemy.create_engine(_dburl )
        Base.metadata.create_all(e)
        _db = orm.sessionmaker(bind=e, autoflush=False, autocommit=False,expire_on_commit=False)()
    return _db

def Q():
    return getSession().query(Dataset)



def __trim_dict(dict, *key_list):
    """just trim the dictionary from given keys (if present)"""
    for key in key_list:
        if key in dict: 
            del dict[key]

#jfp was node='esgf-data.dkrz.de'
def processP2P(node='pcmdi9.llnl.gov', batch_size=3000, **constraints):
    """Harvest the new P2P nodes"""
    
    print "jfp in processP2P, node=",node
    p = p2p.P2P(node=node, defaults={'distrib':True, 'project':'CMIP5'})
    nodes = p.get_facets('data_node', **constraints)['data_node']
    #we don't need a data_node constraint anymore as we keep on working from these results
    #also trim other constraints that we use and make no sense here (limit makes little sence since
    #we can't order them at this time
    if constraints: 
        constraints = constraints.copy()
        __trim_dict(constraints, 'data_node', 'limit', 'fields', 'distrib')
    
    sql_str = "SELECT data_node, index_node, count(*) as count, max(creationtime) as latest from p2p_cmip5.datasets group by data_node, index_node order by data_node;"
    db_info = {}
    #for data_node, index_node, total, last_entry in getSession().connection().execute(sql_str):
    for row in getSession().connection().execute(sql_str):
        db_info[row['data_node']] = row
    
    #now check the remote info and see what needs/can be updated
    for data_node, count in nodes.items():
        index_node = None
        log.info("Checking %s", data_node)
        if data_node in db_info:
            #check if it has been changed
            _ , index_node, total, last_entry = db_info[data_node]
            if count != total:
                log.info("Different number of datasets detected. Remote: %s, local: %s", count, total)
                if total > count:
                    log.debug("We have more. Some dataset where removed")
            else:
                log.debug("No changes detected.")
                continue
        else:
            #this is a new data_node we didn't know about (we need to find the index_node!
            log.info("Unknown data node.")
            
            result = p.get_datasets(fields='index_node', data_node=data_node, limit=1, **constraints)
            if result:
                index_node = result[0]['index_node']
            else:
                log.error("Skipping. Cannot find the index node holding %s data.", data_node)
                continue
        
        #if here, then we need to process data from this node.
        log.debug("Ingesting new data")
        p.node = index_node
        try:
            if data_node in db_info:
                getSession().query(Dataset).filter_by(data_node=data_node).delete()
            Dataset.insert(p.datasets(batch_size=batch_size, fields=Dataset.FIELDS_STR, data_node=data_node, distrib=False, **constraints))
            log.debug("%s changes, %s new entries", len(getSession().dirty), len(getSession().new))
            getSession().commit()
            getSession().expunge_all()  #we don't ned them anymore
            log.info("All datasets from %s are up to date", data_node)
        except:
            try:
                #let's try at least a distributed search... index node might be behind a firewall
                p.node = node
                Dataset.insert(p.datasets(batch_size=batch_size, fields=Dataset.FIELDS_STR, data_node=data_node, distrib=False, **constraints))
                log.debug("%s changes, %s new entries", len(getSession().dirty), len(getSession().new))
                getSession().commit()
                getSession().expunge_all()  #we don't ned them anymore
                log.info("All datasets from %s are up to date", data_node)
            except:
                pass
            getSession().rollback()
            log.warn("Can't access index %s", index_node,exc_info=True)
    
    #report data nodes we knew about but are not there anymore.
    missing = set(db_info) - set(nodes)
    if missing:
        log.info("Missing data nodes (%s): %s", len(missing), ', '.join(missing))
    
    
    
#############################
### ****    W I K I     *****
#############################

table_styles = {0:
        {0: '<rowstyle="background-color:#fafafa">',
        1: '<rowstyle="background-color:#f0f0f0">'},
    1:  {0: '<rowstyle="background-color:#fff9e9">',
        1: '<rowstyle="background-color:#f4efdf">'}}
def _getWikiHeader():
    curr_date = datetime.datetime.utcnow().strftime("%A, %d. %B %Y %I:%M%p")
    warning = """{{{#!wiki caution
This data reflects the current state of the archive extracted by an automated system. It is helpful to spot problems and gain a general view of it. <<BR>>
'''Don't use the modeling center names on this page for citation.''' <<BR>>
For a more general but accurate view of what is available or will be shortly see: http://cmip-pcmdi.llnl.gov/cmip5/availability.html
}}}"""

    return "%s\n\n= CMIP5 Archive Status =\n'''Last Update''': ''%s'' (UTC)\n\n" % (warning, curr_date)

def _getWikiFooter():
    return """

----
''This page is automatically generated. Don't edit, all changes will get lost after next update.~-(Version 0.1)-~''

"""

def _getWikiSummary():
    """Prepare the summary wiki section"""
    sql_summary = """
    SELECT COUNT(DISTINCT id) as datasets, COUNT(DISTINCT data_node) as node_count,
        COUNT(DISTINCT index_node) as index_count, COUNT(DISTINCT split_part(id,'.',3)) as institutes,
        COUNT(DISTINCT split_part(id, '.',5)) as experiments, COUNT(DISTINCT split_part(id, '.',4)) as models,
        to_char(sum(size)/1024/1024/1024/1024, 'FM999G999G999D99') as size, 
        to_char(sum(filecount), 'FM999G999G999') as filecount
        FROM %s
        WHERE replica is not true and id like 'cmip5.%%%%'""" % Dataset.__table__.fullname

    #get the data from the DB
    summary = getSession().connection().engine.execute(sql_summary).fetchone()

    #return the prepared string
    return """== CMIP5 Federated Archive ==

        ||<-2: rowstyle="background-color:#eee">'''Summary'''||
        ||<)>''Modeling centers''||<:>%s||
        ||<)>''Models''||<:>%s||
        ||<)>''Experiments''||<:>%s||
        ||<)>''Data nodes''||<:>%s||
        ||<)>''P2P Index''||<:>%s||
        ||<)>''Datasets''||<:>%s||
        ||<)>''Size''||<:>%s TB||
        ||<)>''Files''||<:>%s||\n\n''Latest version only; no replicas.''\n\n""" % (
            summary['institutes'], summary ['models'],  
            summary['experiments'], summary['node_count'], summary['index_count'],
            summary['datasets'], summary['size'], summary['filecount'] )


def _getWikiModels():
    """Prepare the mode listings for the wiki"""
    sql_row = """
    SELECT split_part(d.id, '.', 3) as institute, split_part(d.id, '.', 4) as model,
        count(Distinct split_part(d.id, '.', 5)) as experiments,
        count(Distinct d.id) as datasets,  to_char(sum(filecount),'FM999G999G999') as filecount,
        to_char(sum(size)/1024/1024/1024, 'FM999G999G990D99') as size, max(creationtime) as modtime, 
        bool_or(access_http) as http,
        bool_or(access_gridftp) as gridftp, bool_or(access_opendap) as opendap
        from (
            SELECT id, max(version) as version
                from %s where replica is not true GROUP BY id
        ) as uniq
            join %s as d on (uniq.id = d.id AND uniq.version = d.version)
        group by institute, model order by model, institute;""" % (Dataset.__table__.fullname,
                                                                Dataset.__table__.fullname)

    wiki_str = "== Models & Modeling Centers ==\n\n\
||<rowstyle=\"background-color:#ddd\">'''Model'''||'''Modeling Center'''||\
'''# Experiments'''||'''# Datasets'''||'''Last Modification'''||\n"
    
    row_nr=0
    style_nr=0
    last_institute=None
    for row in getSession().connection().engine.execute(sql_row):        
        
        #update style
        if last_institute != row['institute']: style_nr += 1
        row_nr += 1
        last_institute = row['institute']
        
        wiki_str += "||%s%s||%s||<)>%s||<)>%s||<:>%s||\n" % (
            table_styles[style_nr%len(table_styles)][row_nr%len(table_styles[style_nr%len(table_styles)])],
            row['model'],row['institute'],
            row['experiments'],row['datasets'],row['modtime'])

    wiki_str += "\n''Latest version only; no replicas.''\n\n"
    return wiki_str

def _getWikiDatanodes():
    """Prepare datanode lsiting"""
    sql_row="""
    SELECT data_node as datanode, split_part(d.id, '.', 4) as model,
        count(Distinct d.id) as datasets, COUNT(CASE WHEN replica THEN 1 ELSE null END) as replicas,
        to_char(sum(size)/1024/1024/1024, 'FM999G999G990D99') as size, 
        to_char(sum(filecount),'FM999G999G999') as filecount, max(creationtime) as modtime, bool_or(access_http) as http, 
        bool_or(access_gridftp) as gridftp, bool_or(access_opendap) as opendap 
    from (
        SELECT id, max(version) as version
            from %s  group by id
    ) as uniq 
        join %s as d on (uniq.id = d.id AND uniq.version = d.version)
    group by datanode, model;""" % (Dataset.__table__.fullname,
                                                                Dataset.__table__.fullname)


    wiki_str = "== Data nodes ==\n\n\
{{{#!wiki important\nThe '''''Data nodes''''' listed below are not meant to be accessed directly by users.  To browse and download CMIP5 data.}}}\n\n\
||<rowstyle=\"background-color:#ddd\">'''Data node'''||'''Model'''||\
'''# Datasets'''||'''Replica'''||'''Size (GB)'''||'''# Files'''||'''Last Modification'''||\n"
    is_replica = ' {{http://openiconlibrary.sourceforge.net/gallery2/open_icon_library-full/icons/png/22x22/actions/media-optical-copy-2.png}} '
    error = ' {X} '
    row_nr = 0
    style_nr = 0
    last_node = None
    row_count=0
    node_string=""

    for row in getSession().connection().engine.execute(sql_row):
        #update style
        if last_node != row['datanode']: 
            style_nr += 1
            if row_count > 0:
                cells = node_string.split('||')
                tmp = cells[1].split('>')
                cells[1] = tmp[0] + ' rowspan=%s>[[http://%s/thredds/esgcet/catalog.html|%s]]||' \
                                    % (row_count, last_node, last_node) + '||'.join(tmp[1:])
                
                wiki_str += '||'.join(cells)
                row_count = 0
                node_string = ""


        row_nr += 1     #total row number for style to improve reading
        row_count += 1  #node row count for spanning
        last_node = row['datanode']
        if row['replicas'] == row['datasets']: replica = is_replica
        elif  row['replicas'] > 0: replica = error
        else: replica = ""

        node_string += "||%s%s||%s||<:>%s||<)>%s||<)>%s||%s||\n" % (table_styles[style_nr%len(table_styles)][row_nr%len(table_styles[style_nr%len(table_styles)])],row['model'],row['datasets'],replica,row['size'],row['filecount'],row['modtime'] )


    #process last entry
    if row_count > 0:
        cells = node_string.split('||')
        tmp = cells[1].split('>')
        cells[1] = tmp[0] + ' rowspan=%s>[[http://%s/thredds/esgcet/catalog.html|%s]]||' \
                            % (row_count, last_node, last_node) + '||'.join(tmp[1:])

        wiki_str += '||'.join(cells)
        row_count = 0
        node_string = ""

    wiki_str += "''Latest version only; includes replicas.''"
    return wiki_str

def updateWiki(dry_run=False, verbose=False):
    #db = getSession()
    
    wiki = _getWikiHeader()
    wiki +=  _getWikiSummary()
    wiki += _getWikiModels()
    wiki += _getWikiDatanodes()
    wiki += _getWikiFooter()
    
    if dry_run:
        if verbose: print wiki
    else: __update_wiki(wiki)

def __update_wiki(content, url='http://esgf.org/wiki/Cmip5Status/P2PArchiveView'):
    """Updates a given moinmoin wiki page. Permissions must be properly set
(All: write)"""
    success = True
    extraInfo = None

    import urllib, re
    try:
        #get a ticket
        response = urllib.urlopen(url + '?action=edit').read()
        ticket =  re.search('<input type="hidden" name="ticket" value="([^"]+)">', response).group(1)

        #update content
        data='action=edit&savetext=%23acl+All%3Aread%2Cwrite%0D%0A{0}&editor=text&button_save=Save+Changes&comment=Auto-generated&category=&ticket={1}'.format(urllib.quote_plus(content),ticket)

        response = urllib.urlopen(url=url, data=data).read()
    except:
        success = False
        import sys
        extraInfo = str(sys.exc_info()[:3])


    #assure everything is as expected
    if  success and re.search('Thank you for your changes', response): return True
    else:
        #send notification of error

        import smtplib
        from email.mime.text import MIMEText
        msg = MIMEText("There was a problem while updating the wiki page\n\nExtra:{0}\n\Message:\n{1}".format(extraInfo,response))
        msg['Subject']='Error while updating the wiki page'
        msg['From']='wikiupdate@cmip2.dkrz.de'
        msg['To']='gonzalez@dkrz.de'
        s = smtplib.SMTP('localhost')
        try:
            s.sendmail(msg['From'], msg['To'], msg.as_string())
        except:
            pass


        return False
    
def updateDB(gateways=None,work_around=True, debug=False, fast=False, collections=None):
    """Harvest and ingest the content of the gateways into the global DB"""
    broken_atom_feed = set(['PCMDI','NCI', 'BADC'])
    if not gateways: gateways = ['PCMDI','WDCC', 'ORNL', 'NCI', 'BADC', 'NCAR' ]

    #work around for broken atom feed
    if work_around:
        work_around = []
        for g in gateways:
            if g in broken_atom_feed:
                gateways.remove(g)
                work_around.append(g)
    else: work_around = []


    for gw in gateways:
        try:
            processGateway(gw,fast=fast)
            updateCollections(gw)
        except:
            print "Aborting harvesting %s, but committing what we got." % gw
            print sys.exc_info()[:2]
            if debug: raise
        finally:
            try:
                getSession().commit()
            except:
                #we don't care about this one, it might also benn broken because
                #of the previous one, but it's worth the try
                getSession().rollback()

    for gw in work_around:
        #work around!!
        if work_around: fast = True  
        print "Harvesting %s in the old style (fast:%s, collections:%s)" % (gw, fast, collections)
        try:
            processGatewayOld(gw, fast=fast, collections=collections)
            updateCollections(gw)
        except:
            print "Aborting harvesting %s, but committing what we got." % gw
            raise
        finally:
            try:
                getSession().commit()
            except:
                #we don't care about this one, it might also benn broken because
                #of the previous one, but it's worth the try
                pass

    

#sed -n '/\*!!!\*$/,/^[ \t]*$/ '"{s/^[^']*'\([^']*\)'[^']*\('\([^']*\)'\)\?[^#]*#\(.*\)$/\1\t\3\t: \4/p}" $file

def auto_doc(message=None):
    import re, sys, os
    file = sys.argv[0]
    
    re_start = re.compile('.*\*!!!\*$')
    re_end = re.compile('^[ \t]*$')
    re_entries= re.compile("^[^']*'([^']*)'[^']*(?:'([^']*)')?[^#]*#(.*)$")
    parsing=False
    results = []
    for line in open(file, 'r'):
        if parsing:
            items = re_entries.match(line)
            if items:
                flag, flag_opt, mesg = items.groups()
                if flag_opt: flag = '%s, %s' % (flag, flag_opt)
                results.append('  %-20s : %s' % (flag, mesg))
            if re_end.match(line): break
        elif re_start.match(line): parsing = True

    if message: message = ': ' + message
    else: message = ''
    if results: print '%s [opt]%s\nopt:\n%s' % (os.path.basename(file), message, '\n'.join(results))
    else: print '%s %s' % (os.path.basename(file), message)

def usage():
    auto_doc() 

def main(argv=None):
    import getopt

    if argv is None: argv = sys.argv[1:]
    try:
        args, lastargs = getopt.getopt(argv, "ndh", ['help','debug','dry-run','harvest','update-wiki'])
    except getopt.error:
        print sys.exc_info()[:3]
        return 1

    debug = dry_run =  False
    harvest = wiki = False

    #parse arguments *!!!*
    for flag, arg in args:
        if flag=='--harvest': harvest = True    #updates the DB backend
        elif flag=='--update-wiki': wiki = True   #updates the setup wiki page
        elif flag=='-n' or flag=='--dry-run': dry_run = True  #Don't update the wiki, just show the wiki text
        elif flag=='-d' or flag=='--debug': debug = True  #extra debug output (breaks on any exception)
        elif flag=='-h' or flag=='--help':        #This help
            usage()
            return 0

    #backwards compatibility

    #This won't succedd if already set(do better next time :-/)
    if not (harvest or wiki):
        print "No command selected."
        return 1

    if harvest:
        #jfp: checking multiple nodes should be completely unnecessary, but I'll leave that
        # here until I'm finished testing...
        # nodes = ['esgf-data.dkrz.de','pcmdi9.llnl.gov','esgf.nccs.nasa.gov']
        nodes = ['pcmdi9.llnl.gov']
        for node in nodes:
            processP2P(node=node)
    if wiki: 
        if dry_run: updateWiki(verbose=True, dry_run=True)
        else:       updateWiki()

    return 0


if __name__ == '__main__':
    import sys
    result=main(None)
    if result != 0: usage()
    sys.exit(result)
