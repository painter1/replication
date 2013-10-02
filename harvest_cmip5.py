#!/usr/local/cdat/bin/python
"""Handles the efficient harvesting for all cmip5 datasets world-wide"""

import sqlalchemy
import os,sys
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, Boolean, DateTime, sql, ForeignKeyConstraint, orm
import time, datetime
Base = declarative_base()

print "jfp imports done, will set up logging and database ",time.ctime()
import logging
hlog = logging.getLogger('harvest')
hlog.addHandler( logging.FileHandler('harvest.log') )

from esgcet.config import loadConfig
config = loadConfig(None)

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
    __tablename__ = 'datasets'
    __table_args__ = (ForeignKeyConstraint(['parent_id','parent_gateway'], ['global.collections.id', 'global.collections.gateway']),{'schema':'global'})
    
    catalog = Column(String, primary_key=True)
    id = Column(String, nullable=False)
    version = Column(Integer, nullable=False)
    access_http = Column(Boolean, default=True)
    access_opendap = Column(Boolean, default=False)
    access_gridftp = Column(Boolean, default=False)
    state = Column(String)
    
    parent_id = Column(String)
    parent_gateway = Column(String)
    master_gateway = Column(String)
    
    size = Column(MyBigInteger)
    filecount = Column(Integer)
    
    creationtime = Column(DateTime, default=sqlalchemy.func.current_timestamp())
    modtime = Column(DateTime, nullable=False)

class Collection(Base, DAO):
    __tablename__ = 'collections'
    __table_args__ = {'schema':'global'}
    
    id = Column(String, primary_key=True)
    gateway = Column(String, primary_key=True)
    creationtime = Column(DateTime, default=sqlalchemy.func.current_timestamp())
    state = Column(String)
    modtime = Column(DateTime, nullable=False)
    datasets = orm.relation(Dataset, backref=orm.backref('parent'), order_by = Dataset.id, cascade='all, delete', primaryjoin=sql.and_(id == Dataset.parent_id,gateway == Dataset.parent_gateway) )

_db = None
def getSession():
    global _db
    if not _db:
        e = sqlalchemy.create_engine(_dburl)
        Base.metadata.create_all(e)
        _db = orm.sessionmaker(bind=e, autoflush=True, autocommit=False,expire_on_commit=False)()
#jfp ... changed autoflush so I can see problems before the whole dataset has been processed
#jfp was:        _db = orm.sessionmaker(bind=e, autoflush=False, autocommit=False,expire_on_commit=False)()
    return _db


def getLatestDatasetsQuery(parent_gateway=None):
    """This is certainly suboptimal but more general, one way to imporve this is to
    apply the filter that will be used for this query to the subquery as well
    But it works fast enough for our purposes.
    Because replicas might not have the latest version, if we want to limit the search for a given gateway
    we must do so already in the sub query (and afterwards again)"""
    db = getSession()
    if parent_gateway:
        latest = db.query(Dataset.id, sqlalchemy.func.max(Dataset.version).label('version')).filter_by(parent_gateway=parent_gateway).group_by(Dataset.id).subquery()
        return db.query(Dataset).join((latest, sqlalchemy.and_(Dataset.version == latest.c.version,Dataset.id==latest.c.id)))
    else:
        #subquery with list of id and version having max version
        latest = db.query(Dataset.id, sqlalchemy.func.max(Dataset.version).label('version')).group_by(Dataset.id).subquery()
        #now join that with the complete dataset list and return
        return db.query(Dataset).join((latest, sqlalchemy.and_(Dataset.version == latest.c.version,Dataset.id==latest.c.id)))

def _getCMIP5Id(gw_url):
    """Parses an html to get this. Yikes!"""
    import urllib2, re
    #get cmip5 id (because it's case sensitive!)
    hlog.debug("jfp opening page " + gw_url + '/browse/browseCollections.htm')
    page = urllib2.urlopen(gw_url + '/browse/browseCollections.htm').read()
    try:
        return re.search('.*\(([Cc][Mm][Ii][Pp]5)\).*', page).group(1)
    except:
        print 'No CMIP5 found for Gateway %s. Check manually.(1)' % gw_name
        hlog.warn( "No CMIP5 found for Gateway %s. Check manually.(1)" % gw_name )
        return

def _getCMIP5Collections(gw_url, cmip5_id):
    """Parses an html to get this. Yikes!"""
    import urllib2, re
    page = urllib2.urlopen(gw_url + ('/project/%s.html' % cmip5_id)).read()
    return re.findall('.*/dataset/(.*)\.html.*', page)

def processGatewayOld(gw_name, fast=True):
    """Old method for harvesting gateways"""
    import urllib2, re, xml, gateway
    db = getSession()
    gw_url = gateway.getGatewayInfo()[gw_name]['url']
    gw_server = gateway.getGatewayInfo()[gw_name]['server']
    #skip these
    skip_top_level = []

    try:
        cmip5_id = _getCMIP5Id(gw_url)
    except:
        print 'No CMIP5 found for Gateway %s. Check manually.(2)' % gw_name
        hlog.warn( 'No CMIP5 found for Gateway %s. Check manually.(2)' % gw_name )
        return
    
    #get already known collections
    db_col = {}
    for col in db.query(Collection).filter(Collection.gateway==gw_server).all():
        #within the gateway these are unique
        db_col[col.id] = col

    #now get known datasets
    db_ds = {}
    for ds in getLatestDatasetsQuery(gw_server).filter(Dataset.parent_gateway==gw_server).all():
        db_ds['%s#%s' % (ds.id, ds.version)] = ds

    counter = 0
    for col in _getCMIP5Collections(gw_url, cmip5_id):
        hlog.info( "Processing Collection %s on %s" %(col,time.ctime()) )
        if col in skip_top_level:
            print "Skipping"
            hlog.info( "Skipping, time is %s" % (time.ctime()) )
            continue

        if not col in db_col:
            #new collection!
            hlog.info("New collection %s on %s" % (col,time.ctime()))
            md = gateway.main(('-g %s --parent %s -mo' % (gw_name, col)).split())
            if md==None: continue
            #use a fictional date for the update so we know later on which
            #should be latered
            db.add(Collection(id=col, gateway=gw_server,state=md['state'],
                modtime=dummy_date))

        existing_ds = {}
        datasets = gateway.main(('-g %s --parent %s -do' % (gw_name,col)).split())
        if datasets==None: datasets=[]
        for dataset in datasets:
            ds_key = '%s#%s' % (dataset['id'], dataset['version'])

            #store for later
            existing_ds[ds_key] = True

            if ds_key in db_ds:
                old_ds = db_ds[ds_key]
                #should we update? (for now don't...)
                #if int(dataset['version']) == old_ds.version:
                    #same version... we might want to check... but in the common case this won't be necessary
                    #and is extremely expensive for this old way of getting things
                #    continue
            else:
                old_ds = None

            #Avoid reparsing already parsed datasets. The might change! e.g. they can be retracted.
            #They should be parsed once in a while
            if fast and old_ds: continue

            print "Processing dataset", ds_key, " on ", time.ctime()
	    hlog.info( "Processing dataset %s on %s" %(ds_key,time.ctime()) )
            metadata = gateway.main(('-g %s --parent %s -mo' % (gw_name, dataset['id'])).split())
            if not metadata:
                continue
            #version work around 
            if metadata['state'] == 'retracted':
                print "retracted dataset"
                hlog.info( "retracted dataset" )
                #this got retracted!
                if old_ds and old_ds.state != metadata['state']:
                    #state changed!
                    old_ds.state = metadata['state']
                continue
            if not metadata['catalog'] or not metadata['version']:
                print "Can't parse this, no catalog or version!!", metadata
                hlog.info( "Can't parse this, no catalog or version!! %s" %s (metadata) )
                continue


            #this is new!
            files = gateway.main(('-g %s --parent %s -fo' % (gw_name,dataset['id'])).split())
            if files==None: files=[]
            filecount = len(files)
            if filecount > 0:
                size = sum([int(f['size']) for f in files])
                #we assume this is per dataset defined, and not per file
                # use some file in the middle for this
                ep = files[filecount/2]['endpoints']
                if ep:
                    types = [e['type'] for e in ep]
                else:
                    types = []
            else:
                #empty dataset?! There are some...
                size = 0
                types = []
            if old_ds:
                #we will need to update the existing one
                old_ds.access_http=('HTTPServer' in types)
                old_ds.access_gridftp=('GridFTP' in types)
                old_ds.access_opendap=('OPeNDAP'in types)
                 
            else:
                db.add(Dataset(id=dataset['id'], version=int(metadata['version']), catalog=metadata['catalog'], 
                state=metadata['state'], filecount=filecount, size=size, access_http=('HTTPServer' in types),
                access_gridftp=('GridFTP' in types), access_opendap=('OPeNDAP' in types), 
                modtime=dummy_date, parent_gateway=gw_server, parent_id=col))
            if counter > 20:
                db.commit()
                counter = 0
            else:
                counter += 1
            # db.commit() #jfp temporary extra commit, to aid debugging


        if col in db_col:
            print col, len(db_col[col].datasets), len(existing_ds)
            hlog.info( "collection,lengths: %s, %s, %s on %s" %\
                       ( col, len(db_col[col].datasets), len(existing_ds), time.ctime() ) )
            for dataset in db_col[col].datasets:
                ds_key = '%s#%s' % (dataset.id, dataset.version)
                if not ds_key in existing_ds:
                    print "dataset %s was deleted" % ds_key
                    hlog.info( "dataset %s was deleted" % ds_key )
                    db.delete(dataset)
                    #if dataset.state == 'published':
                        #dataset.state = 'retracted'

    #commit the rest of the changes
    db.commit()


def processGateway(gw_name):
    import urllib2, re, xml, gateway
    db = getSession()
    gw_url = gateway.getGatewayInfo()[gw_name]['url']
    gw_server = gateway.getGatewayInfo()[gw_name]['server']
    
    hlog.debug("jfp gw_url=%s, gw_server=%s",gw_url,gw_server)
    try:
        cmip5_id = _getCMIP5Id(gw_url)
    except:
        if gw_name == 'NCI':
            hlog.warn("_getCMIP5Id failed; but recognize gateway and will use 'cmip5'")
            cmip5_id = 'cmip5'
        else:
            print 'No CMIP5 found for Gateway %s. Check manually.(3)' % gw_name
            return
    hlog.debug("jfp cmip5_id=%s",cmip5_id)
     
    #get all toplevel collections from gateway
    gw_top = {}
    hlog.debug("jfp in processGateway, will call gateway.main with args %s",
               ('-g %s -co' % gw_name).split())
    collections = gateway.main(('-g %s -co' % gw_name).split())
    if collections==None: collections=[]
    if collections==2:   # quick fix; gateway.main should throw an exception instead
        collections=[]
    for tlc in collections:
        gw_top[tlc['id']] = tlc
    
    #get already known collections
    db_col = {}
    for col in db.query(Collection).filter(Collection.gateway==gw_server).all():
        #within the gateway these are unique
        db_col[col.id] = col
    hlog.debug("jfp db_col=%s",db_col)
    
    db_ds = {}
    for ds in getLatestDatasetsQuery(gw_server).filter(Dataset.parent_gateway==gw_server).all():
        db_ds[ds.id] = ds
    
    #now get all CMIP5 datasets
    page_url = '%s/project/%s/dataset/feed.atom' % (gw_url, cmip5_id)
    hlog.debug("jfp in processGateway, about to open %s",page_url)
    # jfp 2012.09.11 This url is unavailable at NCI; the harvest will always fail.
    #    if page_url.find('nci.org.au')>-1:  # jfp unreliable site, try the url twice (doesn't help)
    #        try:
    #            print "first attempt..."
    #            page = urllib2.urlopen(page_url,None,120).read()
    #        except Exception as e:
    #            print "...failed: ",e
    #        print "second attempt..."
    try:
        page = urllib2.urlopen(page_url).read()
    except Exception as e:
        print "exception opening %s in processGateway: %s" % (page_url,e)
        raise e
    dom = xml.dom.minidom.parseString(page)
    counter = 0 #commit after a bunch
    existing_ds = {}
    hlog.debug("jfp %s dom entries",len(dom.getElementsByTagName('entry')))
    for entry in dom.getElementsByTagName('entry'):
        id = entry.getElementsByTagName('title')[0].childNodes[0].data
        timestamp = entry.getElementsByTagName('updated')[0].childNodes[0].data
        last_update = datetime.datetime(*time.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")[:6])
        #top level and normal are mixed!
        if id in gw_top: 
            #this is a top level for cmip5!
            print "Top level found", id 
            if id in db_col:
                #update
                col = db_col[id]
                if last_update > col.modtime:
                    #we know this collection was modified! (not that we car now...)
                    print "Collection modifed! was %s now is %s" % (col.modtime, last_update)
                    col.modtime = last_update
            else:
                #add new collection
                metadata = gateway.main(('-g %s --parent %s -mo' % (gw_name, id)).split())
                if metatdata==None: continue
                db.add(Collection(gateway=gw_server,id=id,modtime=last_update,state=metadata['state']))
            continue

        #remember this dataset for later
        existing_ds[id] = True

        if id in db_ds:
            #we know this normal dataset! Check if it has changed
            if  db_ds[id].modtime == last_update:
                #old news...
                hlog.debug("Unchanged dataset %s, modtime=%s",id,last_update)
                continue
            print "Changed dataset found", id, db_ds[id].modtime, last_update 
            hlog.info( "Changed dataset found %s %s %s", id, db_ds[id].modtime, last_update )
            #something got changed!
            old_ds = db_ds[id]
            old_ds.modtime = last_update
        else:
            print "New dataset found", id, " on ", time.ctime()
            hlog.info( "New dataset found %s on %s" %(id,time.ctime()) )
            old_ds = None
        #new dataset version or something changed!
        metadata = gateway.main(('-g %s --parent %s -mo' % (gw_name, id)).split())
        if metadata==None or metadata==2: continue
        hlog.debug("version %s",metadata['version'])
         
        #version work around
        if metadata['state'] == 'retracted':
            print "retracted dataset"
            #this got retracted!
            if old_ds and old_ds.state != metadata['state']:
                #state changed!
                old_ds.state = metadata['state']
            continue
        if not metadata['catalog'] or not metadata['version']:
            print "Can't parse this, no catalog or version!!", metadata
            continue
            
        files = gateway.main(('-g %s --parent %s -fo' % (gw_name, id)).split())
        if files==None: files=[]
        filecount = len(files)
        if filecount > 0:
            size = sum([int(f['size']) for f in files])
            #we assume this is per dataset defined, and not per file
            ep = files[filecount/2]['endpoints']
            if ep:
                types = [e['type'] for e in ep]
            else:
                types = []
        else:
            #empty dataset?! There are some...
            size = 0
            types = []

        if old_ds and int(metadata['version']) == old_ds.version:
            print "Same version was updated!!"

            to_check_update = [('access_http', 'HTTPServer' in types),
                ('access_gridftp', 'GridFTP' in types), ('access_opendap', 'OPeNDAP'in types),
                ('filecount', filecount), ('size', size)]
            for var, value in to_check_update:
                report = ""
                old_value = old_ds.__dict__[var]
                if old_value != value:
                    #report and update
                    report += "Changed %s from %s to %s, " % (var, old_value, value)
                    old_ds.__dict__[var] = value
            continue    #Use old_ds instead of creating a new one.
        elif old_ds:
            #new version
            print "New version found %s, last one was %s; on %s" %\
               (metadata['version'], old_ds.version, time.ctime())
            hlog.info( "New version found %s, last one was %s; on %s" %\
               (metadata['version'], old_ds.version, time.ctime()) )
        
        #Definitely a new version of either an existing dataset or a new one.
        try:  #jfp added try/except
            db.add(Dataset(id=id, version=int(metadata['version']), catalog=metadata['catalog'],\
                           state=metadata['state'], filecount=filecount, size=size, access_http=\
                           ('HTTPServer' in types), access_gridftp=('GridFTP' in types),\
                           access_opendap=('OPeNDAP' in types), modtime=last_update, parent_gateway=gw_server))
            db.flush()  # jfp will slow things down, but we'll catch problems right away
        except sqlalchemy.exc.IntegrityError:   #jfp added try/except
            print "exception adding dataset id=",id," version=",metadata['version']," state=",metadata['state']
            print "catalog=",metadata['catalog']," modtime=",last_update," parent_gateway=",gw_server
            print "access_http=",('HTTPServer' in types)," access_gridftp=",('GridFTP' in types)
            print sys.exc_info()[:2]
            db.rollback()  # jfp mandatory after a failed flush!
            # raise  #jfp Now should be able to continue with other datasets.
        if counter > 20:
            #db.commit()
            counter = 0
        else:
            counter += 1
    #db.commit()

    #Now we must find missing ones, so we delete them properly
    for col in db_col.values():
        for dataset in col.datasets:
            if not dataset.id in existing_ds:
                if dataset.state == 'published':
                    dataset.state = 'retracted'
                print "dataset %s was removed by %s" % (dataset.id,time.ctime())
                hlog.info( "dataset %s was removed by %s" %(dataset.id,time.ctime()) )
    #db.commit()
    # print "jfp finished with loop over db_col.values()"
    datasets = {}
    for col in db.query(Collection).filter(Collection.state=='published').filter(Collection.gateway==gw_server):
        gdatasets = gateway.main(('-g %s --parent %s -do' % (col.gateway, col.id)).split())
        if gdatasets==None: gdatasets=[]
        for dataset in gdatasets:
            datasets[dataset['id']] = col.id
    
    for d in db.new:
        if d.id in datasets:
            d.parent_id = datasets[d.id]
        else:
            print "problem with", d
    db.commit()
    print "jfp committed"

def updateCollections(gw_name):
    import gateway
    gw_server = gateway.getGatewayInfo()[gw_name]['server']
    
    db = getSession()
    datasets = {}
    for col in db.query(Collection).filter(Collection.state=='published').filter(Collection.gateway==gw_server):
        gdatasets = gateway.main(('-g %s --parent %s -do' % (col.gateway, col.id)).split())
        if gdatasets==None: gdatasets=[]
        for dataset in gdatasets:
            datasets[dataset['id']] = col.id
    
    #search for orphans of this gateway
    for dataset in db.query(Dataset).filter(Dataset.parent_gateway==gw_server).filter(Dataset.parent_id==None):
        if dataset.id in datasets:
            dataset.parent_id = datasets[dataset.id]
        else:
            #perhaps the top level collection was retracted!
            print "Dataset has no parent", dataset.id
    
    db.commit()

def updateMasterGateway():
    #assume institutes publish at only one data node, everything else is a replica, 
    #so the master_gateway never changes for an institute... 
    #this also imply the first time it has to be done by hand!
    
# jfp was:
#    sql = "UPDATE global.datasets as g SET master_gateway = tmp.master_gateway FROM (SELECT DISTINCT split_part(id, '.',3) as institute, master_gateway from global.datasets where master_gateway is not null) as tmp WHERE g.master_gateway is null and split_part(g.id,'.',3)=tmp.institute;"
# jfp eliminate "as g" because it exhibits a bug in Postgres:
    sql = "UPDATE global.datasets       SET master_gateway = tmp.master_gateway FROM (SELECT DISTINCT split_part(id, '.',3) as institute, master_gateway from global.datasets where master_gateway is not null) as tmp WHERE global.datasets.master_gateway is null and split_part(global.datasets.id,'.',3)=tmp.institute;"

    #trigger this update
    proxy = getSession().connection().engine.execute(sql)
    print "updated %s datasets to their corresponding master_gateway" % proxy.rowcount

def _getWikiHeader():
    curr_date = datetime.datetime.utcnow().strftime("%A, %d. %B %Y %I:%M%p")
    return "= CMIP5 Archive Status =\n'''Last Update''': ''{0}'' (UTC)\n".format(curr_date)

def _getWikiFooter():
    return """

----
''This page is automatically generated. Don't edit, all changes will get lost after next update.~-(Version 0.2)-~''

"""

def _getWikiSummary():
    """Prepare the summary wiki section"""
    sql_summary = """
    SELECT COUNT(DISTINCT id) as datasets, COUNT(DISTINCT split_part(catalog, '/',3)) as nodes,
        COUNT(DISTINCT parent_gateway) as gateways, COUNT(DISTINCT split_part(id,'.',3)) as institutes,
        COUNT(DISTINCT split_part(id, '.',5)) as experiments, COUNT(DISTINCT split_part(id, '.',4)) as models,
        to_char(sum(size)/1024/1024/1024/1024, 'FM999G999G999D99') as size, 
        to_char(sum(filecount), 'FM999G999G999') as filecount
        FROM global.datasets
        WHERE state = 'published'"""

    #get the data from the DB
    summary = getSession().connection().engine.execute(sql_summary).fetchone()

    #return the prepared string
    return """== CMIP5 Federated Archive ==

        ||<-2: rowstyle="background-color:#eee">'''Summary'''||
        ||''Modeling centers''||<:>%s||
        ||''Models''||<:>%s||
        ||''Experiments''||<:>%s||
        ||''Data nodes''||<:>%s||
        ||''Gateways''||<:>%s||
        ||''Datasets''||<:>%s||
        ||''Size''||<:>%s TB||
        ||''Files''||<:>%s||\n\n""" % (summary['institutes'], summary ['models'],  
            summary['experiments'], summary['nodes'], summary['gateways'],
            summary['datasets'], summary['size'], summary['filecount'] )


def _getWikiModels():
    """Prepare the mode listings for the wiki"""
    sql_row = """
    SELECT split_part(d.id, '.', 3) as institute, split_part(d.id, '.', 4) as model,
        count(Distinct d.id) as datasets,  to_char(sum(filecount),'FM999G999G999') as filecount,
        to_char(sum(size)/1024/1024/1024, 'FM999G999G990D99') as size, max(modtime) as modtime, 
        bool_or(access_http) as http,
        bool_or(access_gridftp) as gridftp, bool_or(access_opendap) as opendap
        from (
            SELECT id, max(version) as version
                from global.datasets where state = 'published' group by id
        ) as uniq
            join global.datasets as d on (uniq.id = d.id AND uniq.version = d.version)
        group by institute, model order by model, institute;"""

    wiki_str = "== Modeling Centers and Models ==\n\n\
||<rowstyle=\"background-color:#ddd\">'''Modeling Center'''||'''Model'''||\
'''# Datasets'''||'''Size (GB)'''||'''# Files'''||'''Last Modification'''||'''HTTP'''||'''GridFTP'''||'''OPeNDAP'''||\n"
    ok = ' (./) '
    missing = ' {X} '
    for row in getSession().connection().engine.execute(sql_row):
        modtime = row['modtime']
        if modtime == dummy_date:
            modtime = " /!\ ''Not reported'' "
        if row['http']: http=ok
        else: http=missing
        if row['gridftp']: gridftp=ok
        else: gridftp=missing
        if row['opendap']: opendap=ok
        else: opendap=missing

        wiki_str += "||%s||%s||<:>%s||<:>%s||<:>%s||%s||%s||%s||%s||\n" % (row['institute'],row['model'],row['datasets'],row['size'],row['filecount'],modtime,http,gridftp,opendap)

    wiki_str += "\n''Latest version only; no replicas.''"
    return wiki_str

def _getWikiDatanodes():
    """Prepare datanode lsiting"""
    sql_row="""
    SELECT split_part(d.catalog, '/', 3) as datanode, split_part(d.id, '.', 4) as model,
        count(Distinct d.id) as datasets, to_char(sum(size)/1024/1024/1024, 'FM999G999G990D99') as size, 
        to_char(sum(filecount),'FM999G999G999') as filecount, max(modtime) as modtime, bool_or(access_http) as http, 
        bool_or(access_gridftp) as gridftp, bool_or(access_opendap) as opendap 
    from (
        SELECT id, max(version) as version
            from global.datasets where state = 'published' group by id
    ) as uniq 
        join global.datasets as d on (uniq.id = d.id AND uniq.version = d.version)
    group by datanode, model;"""

    wiki_str = "== Data nodes ==\n\n\
{{{#!wiki caution\nThe '''''Data nodes''''' listed below are not meant to be accessed directly by users.  To browse and download CMIP5 data, users should go to one of the following main CMIP5 Data Portals (or gateways):\n\n\
PCMDI: http://pcmdi3.llnl.gov/esgcet/home.htm <<BR>>\n\
BADC: http://cmip-gw.badc.rl.ac.uk/home.htm <<BR>>\n\
DKRZ: http://ipcc-ar5.dkrz.de/home.htm }}}\n\n\
||<rowstyle=\"background-color:#ddd\">'''Data node'''||'''Model'''||\
'''# Datasets'''||'''Size (GB)'''||'''# Files'''||'''Last Modification'''||'''HTTP'''||'''GridFTP'''||'''OPeNDAP'''||\n"
    ok = ' (./) '
    missing = ' {X} '
    for row in getSession().connection().engine.execute(sql_row):
        modtime = row['modtime']
        if modtime == dummy_date:
            modtime = " /!\ ''Not reported'' "
        if row['http']: http=ok
        else: http=missing
        if row['gridftp']: gridftp=ok
        else: gridftp=missing
        if row['opendap']: opendap=ok
        else: opendap=missing

        wiki_str += "||%s||%s||<:>%s||<:>%s||<:>%s||%s||%s||%s||%s||\n" % (row['datanode'],row['model'],row['datasets'],row['size'],row['filecount'],modtime,http,gridftp,opendap)

    wiki_str += "''Latest version only; includes replicas.''"
    return wiki_str

def updateWiki():
    db = getSession()
    
    wiki = _getWikiHeader()
    wiki +=  _getWikiSummary()
    #print _getWikiModels()
    wiki += _getWikiDatanodes()
    wiki += _getWikiFooter()
    
    __update_wiki(wiki)

def __update_wiki(content, url='http://esgf.org/wiki/Cmip5Status/ArchiveView'):
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
    
def updateDB():
    gateways = []  # convenient when I temporarily comment-out the next line
    gateways = ['WDCC', 'ORNL', 'BADC', 'NCAR' ]
    # jfp 2012.09.11:  NCI deleted, guaranteed to fail, missing web page ...dataset/feed.atom
    # old_method_gw = [] #jfp temporary
    old_method_gw = ['PCMDI']
    print "jfp gateways=",old_method_gw,gateways

    for gw in old_method_gw:      # PCMDI first, because reliable
	hlog.info( "Harvesting %s the old style on %s" % (gw,time.ctime()) )
        try:
            processGatewayOld(gw)
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
    for gw in gateways:
        print "Harvesting", gw, " on ", time.ctime()
        hlog.info( "Harvesting %s  on %s" % (gw, time.ctime()) )
        try:
            processGateway(gw)
            print "jfp processed", gw
            updateCollections(gw)
            print "jfp updateCollections done for",gw
        except Exception as e:
            import traceback
            print "Aborting harvesting %s, but committing what we got; exception is %s" % (gw,e)
            print sys.exc_info()[:2]
            print traceback.format_exc()
        finally:
            try:
                getSession().commit()
            except:
                #we don't care about this one, it might also benn broken because
                #of the previous one, but it's worth the try
                pass

    

usage="""harvest_cmip5.py [opt] harvest|update-wiki
harvest: updates the DB backend
update-wiki: updates the setup wiki page
"""
    
def main(argv=None):
    import getopt

    if argv is None: argv = sys.argv[1:]
    try:
        args, lastargs = getopt.getopt(argv, "h", ['help'])
    except getopt.error:
        print sys.exc_info()[:3]
        return 1

    #parse arguments
    for flag, arg in args:
        if flag=='-h' or flag=='--help':
            print usage
            return 0

    #This won't succedd if already set(do better next time :-/)
    if len(lastargs) == 0: return 1

    for lastarg in lastargs:
        if lastarg=='harvest':
            updateDB()
        elif lastarg=='update-wiki':
            updateWiki()
        else: return 1

    #asure the master Gateway is properly populated    
    updateMasterGateway()

    return 0


if __name__ == '__main__':
    #configure logging
#    log.basicConfig(level=log.INFO)
#    log.basicConfig(level=log.DEBUG)
    logging.basicConfig(level=logging.DEBUG)

    import sys
    result=main(None)
    if result != 0: print usage
    sys.exit(result)
