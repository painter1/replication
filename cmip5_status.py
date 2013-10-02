#!/usr/local/cdat/bin/python
from sqlalchemy import Column, Integer, String, DateTime
import sqlalchemy
from utils_db import DAO, DB, Base
import logging as log

class DatasetDAO(Base, DAO):
    __tablename__ = 'cmip5_datasets'

    dataset_id = Column(String, primary_key=True)
    version = Column(Integer, primary_key=True)
    datanode = Column(String, primary_key=True)
    endpoint_type = Column(String, primary_key=True)
    gateway = Column(String, primary_key=True)

    size = Column(Integer)
    files_count = Column(Integer)
    catalog=Column(String)
    parent=Column(String)
    
    creationtime = Column(DateTime, default=sqlalchemy.func.current_timestamp())
    modtime = Column(DateTime, default=sqlalchemy.func.current_timestamp())
    
    #not stored in DB
    __drs = None

    def __init(self, **kwargs):
        Base.__init__(self, **kwargs)

    def __init__(self, **kwargs):
        self.__init(**kwargs)

    def __init__(self, dataset, endpoint, gateway, **kwargs):
        self.__init(dataset_id=dataset['id'], version=dataset['version'], datanode=endpoint['server'], endpoint_type=endpoint['type'], 
            gateway=gateway['server'], catalog=dataset['catalog'], parent=dataset['parent'], **kwargs)

    def markAsUpdated(self):
        self.modtime = sqlalchemy.func.current_timestamp()
    
    def getDRS(self):
        if not self.__drs:
            self.__drs = DRS(id=self.dataset_id)
        return self.__drs.copy()


class Cmip5StatusDB(DB):
    def addAll(self, items, overwrite=False):
        if not items: return []
        self.open()

        if overwrite: 
            log.debug('merging %s items', len(items))
            return self._merge_all(items)
            

        results = self._session.query(items[0].__class__).filter(items[0].__class__.dataset_id.in_([ i.dataset_id for i in items])) #\
#                                                        .filter(items[0].__class__.version.in_([ i.version for i in items])).all()

        new = []
        existing = []

        in_session = self._add_all(items, db_objects=results, new=new, existing=existing )

        return new + existing

def __getTestData():
    return [EntryDAO(DATASET_ID='cmip5.output1.IPSL.IPSL-CM5A-LR.aqua4K.mon.atmos.cfMon.r1i1p1', version=20110429, datanode='cmip2.dkrz.de', gateway='albedo2.dkrz.de')]



def __update(db):
    import gateway
    db.open()

    
    gateways = gateway.getGatewayInfo()
    
    known_active = ['WDCC', 'PCMDI', 'BADC', 'NCI', 'NCAR']

    #only reparse datasets once in a while
    import datetime
    timestamp = "modtime > '{0}'".format(datetime.datetime.now() - datetime.timedelta(days=30))
    global log

    def handle_datasets(datasets, gateway):
        #only process if not empty
        if not datasets: return datasets

        total = db._session.query(DatasetDAO).filter(DatasetDAO.parent==datasets[0]['parent']).count()
        if len(datasets) < total:
            #Some datasets are not available anymore, we should scan everything again
            log.error('Dataset were deleted for parent %s, please delete from db.', datasets[0]['parent'])

        known = set([ds[0] for ds in db._session.query(DatasetDAO.dataset_id).filter(DatasetDAO.gateway==gateway['server']).filter(timestamp)])
        new_datasets = []
        
        #get datasets not already checked within a given period
        for dataset in datasets:
            if dataset['id'] not in known: new_datasets.append(dataset)

        log.info('Processing %s new datasets (skipping %s).', len(new_datasets), len(datasets) - len(new_datasets))
        return new_datasets
            
    global __to_ingest
    __to_ingest = []

    def handle_result(dataset, files, gateway):  
        global __to_ingest
        #store only MB
        dataset_size = sum([int(f['size']) for f in files])>>20
        files_count= len(files) 

        #we assume all files in dataset have the same access points
        for endpoint in files[0]['endpoints']:                 
            dao = DatasetDAO( dataset, endpoint, gateway, size=dataset_size, files_count=files_count)
            dao.markAsUpdated()
            __to_ingest.append(dao)

        #update if batch's full        
        if len(__to_ingest) > 25:
            db.addAll(__to_ingest, overwrite=True)
            __to_ingest = []


    for gw_name in gateways:
        #only parse gateways that are known to be active...
        if gw_name not in known_active: continue

        gw_data = gateways[gw_name]
        log.info('Processing %s', gw_name)


        __to_ingest=[]
        
        try: 
            gateway.getCurrentDatasets(gw_name, filter_datasets=handle_datasets, callback_result=handle_result, continue_on_errors=True)
    
            #make sure we don't leave any last ones
            db.addAll(__to_ingest, overwrite=True)
        except:
            import sys
            log.error('There was an error contacting gateway %s: %s',gw_name, str(sys.exc_info()[:3]))
            raise

    db._session.commit()

def getCurrentStatus(db):

    #This will only work in postgres...
    sql_str = """SELECT split_part(dataset_id, '.', 3) as institute, split_part(dataset_id, '.', 4) as model, 
        count(*) as dataset_count, sum(size)/1024 as size_gb, sum(files_count) as filecount, datanode, endpoint_type, gateway
            from public.cmip5_datasets group by model,institute,datanode,endpoint_type,gateway 
            order by institute, model, datanode, gateway, endpoint_type;"""

    db.open()
    
    return db._engine.connect().execute(sql_str).fetchall()
    
def __updateWiki(content, url='http://esgf.org/wiki/Cmip5Status/ArchiveView'):
    """Updates a given moinmoin wiki page. Permissions must be properly set (All: write)"""
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

def _status2Wiki(db):
    """Reformat the result in a moinmoin wiki syntax"""
    import datetime
    db.open()

    curr_date = datetime.datetime.utcnow().strftime("%A, %d. %B %Y %I:%M%p")

    header="= CMIP5 Archive Status =\n'''Last Update''': ''{0}'' (UTC)\n".format(curr_date)

    #build summary
    total_institutes, total_models, total_datanodes, total_gateways =\
        db._engine.execute("SELECT count(DISTINCT split_part(dataset_id, '.', 3)) as institutes, count(DISTINCT split_part(dataset_id, '.', 4)) as models,\
count(DISTINCT datanode)as datanodes, count(DISTINCT gateway) as gateways from %s" % DatasetDAO.__tablename__).fetchone()

    total_files, total_size, total_count = db._engine.execute('select sum(tmp.files_count), sum(tmp.size),count(*) from '\
+ '(SELECT dataset_id, version, max(files_count) as files_count, max(size) as size from %s group by dataset_id, version) as tmp'\
% DatasetDAO.__tablename__).fetchone()
     
    summary="||<-2: rowstyle=\"background-color:#eee\">'''Summary'''||\n||''Modeling centers''||<:>{0}||\n\
||''Models''||<:>{1}||\n||''Data nodes''||<:>{2}||\n||''Gateways''||<:>{3}||\n||''Datasets''||<:>{4}||\n\
||''Size''||<:>{5:.2f} TB||\n||''Files''||<:>{6}||\n\n".format(\
    total_institutes, total_models, total_datanodes, total_gateways, total_count, float(total_size)/1024/1024, total_files)


    #build row data
    row_head="||<rowstyle=\"background-color:#ddd\">'''Modeling Center'''||'''Model'''||\
'''# Datasets'''||'''Size (GB)'''||'''# Files'''||'''Data node'''||'''Access type'''||'''Publishing Gateway'''||\n"

    rows = []
    for row in getCurrentStatus(db):
        institute, model, count, size, filecount, datanode, type, gateway = row
        #create row
        rows.append("||%s||%s||<:>%s||<:>%s||<:>%s||%s||%s||%s||" % (institute, model, count, size, filecount, datanode, type, gateway))

    #other interesting data ---
    #percentage of archive size per datanode
    #'SELECT datanode, ROUND(sum(size)/%s) as percentage from (SELECT dataset_id, version,datanode, max(size) as size from %s group by dataset_id,version, datanode) as tmp group by datanode' % (total_size, DatasetDAO.__tablename__)
    #percentage of archive size by model
    #'SELECT split_part(dataset_id, '.', 4) as model, ROUND(sum(size)/%s) as percentage from (SELECT dataset_id, version,datanode, max(size) as size from %s group by dataset_id,version, datanode) as tmp group by model' % (total_size, DatasetDAO.__tablename__)

    footer = "\n\n----\n''This page is automatically generated. Don't edit, all changes will get lost after next update.''"

    return header + summary + row_head + "\n".join(rows) + footer
       
def updateCmip5ArchiveWiki(db):
    content = _status2Wiki(db)
    return __updateWiki(content, url='http://esgf.org/wiki/Cmip5Status/ArchiveView')

def _status2Kml(db):
    #prepare kml
    import kml as kml_lib
    kml = kml_lib.cmip5()



    #sets
    institutes = set()
    datanodes = set()
    gateways = set()
    inst2node = set()
    node2gate = set()

    #add all relations
    for institute, model, count, size, filecount, datanode, type, gateway in getCurrentStatus(db):
        institutes.add(institute)
        datanodes.add(datanode)
        gateways.add(gateway)
        inst2node.add((institute,datanode))
        node2gate.add((datanode,gateway))

    #get geo objects
    import geo_utils
    #*** We asume geo table is in the same DB   ***
    db_geo = geo_utils.GeoDB(str(db._engine.url))    
    db_geo.open()
    point = {}
#    for item in institutes.union(datanodes).union(gateways):
#        point[item] = geo_utils.GeoDAO(name=item)
    t = lambda **dic: dic
    for geo_item in db_geo.addAll([geo_utils.GeoDAO(name=item) for item in institutes.union(datanodes).union(gateways)]):
        point[geo_item.name] = geo_item
        if not geo_item.lat:
            log.info('No geo coordinates for %s', geo_item)
            continue
        #put placemark in place
        if geo_item in gateways: style = 'gateway'
        elif geo_item in datanodes: style = 'datanode'
        elif geo_item in institutes: style = 'institute'
        #Add placemarks
        kml['placemarks'].append(t(name=geo_item.name,description=style,style=style,longitude=geo_item.lon, latitude=geo_item.lat))
    
    #now add relations
    for institute,datanode in inst2node:
        if not point[institute].lat or not point[datanode].lat: continue
            
        kml['lines'].append(t(name='{0}->{1}'.format(institute, datanode),style='institute2datanode',\
points=[t(longitude=point[institute].lon, latitude=point[institute].lat), t(longitude=point[datanode].lon, latitude=point[datanode].lat)]))
    
    for datanode, gateway in node2gate:
        if not point[gateway].lat or not point[datanode].lat: continue

        kml['lines'].append(t(name='{0}->{1}'.format(datanode, gateway),style='datanode2gateway',\
points=[t(longitude=point[datanode].lon, latitude=point[datanode].lat), t(longitude=point[gateway].lon, latitude=point[gateway].lat)]))

    return  kml.getXML().toprettyxml('','')


usage="""cmip5_status.py [opt] update-db|update-wiki|create-kml
update-db: updates the DB backend
update-wiki: updates the setup wiki page

Opt:
 -h,--help              This help
 -P,--password <pass>   DB password
 -W                     Ask for password
 -k <file>              output to kml file
 -v                     verbose
 -q                     quite
 -d                     debug"""
    
def main(argv=None):
    import getopt

    if argv is None: argv = sys.argv[1:]
    try:
        args, lastargs = getopt.getopt(argv, "hP:Wqvdk:", ['help', 'password='])
    except getopt.error:
        print sys.exc_info()[:3]
        return 1

    #parse arguments
    password = kml_file = create_kml = None
    for flag, arg in args:
        if flag=='-P' or flag=='--password': password = arg
        elif flag=='-k': kml_file = arg
        elif flag=='-q': log.basicConfig(level=log.NONE)
        elif flag=='-v': log.basicConfig(level=log.INFO)
        elif flag=='-d': log.basicConfig(level=log.DEBUG)
        elif flag=='-W': 
            import getpass
            password = getpass.getpass('please give the DB password: ')
        elif flag=='-h' or flag=='--help':
            print usage
            return 0

    #This won't succedd if already set(do better next time :-/)
    log.basicConfig(level=log.WARN)

    log.debug('Argv: %s', argv)

    if password:
        #db = Cmip5StatusDB('postgres://plone:%s@plone.dkrz.de/thredds'.format(password))
        db = Cmip5StatusDB('postgres://cmip5:%s@localhost:6543/thredds'.format(password))
        del password
    else:
        db = Cmip5StatusDB('postgres://cmip5@localhost:6543/thredds')

    if len(lastargs) == 0: return 1

    for lastarg in lastargs:
        if lastarg=='update-db':
            __update(db)
        elif lastarg=='update-wiki':
            if updateCmip5ArchiveWiki(db):
                log.info("Success!")
        elif lastarg=='create-kml':
            kml = _status2Kml(db)
            if not kml_file:
                print kml
        else: return 1
    
    return 0


if __name__ == '__main__':
    #configure logging
#    log.basicConfig(level=log.INFO)
#    log.basicConfig(level=log.DEBUG)

    import sys
    result=main(None)
    if result != 0: print usage
    sys.exit(result)
