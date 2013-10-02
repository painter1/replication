#!/usr/local/cdat/bin/python

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy import Column, Integer, String, create_engine, orm, ForeignKey, ForeignKeyConstraint
import logging 
log = logging.getLogger('replica_db')
from utils_db import DAO
from drs import DRS

Base = declarative_base()

class ReplicaFileDAO(Base, DAO):
    """Defines the Data Access Object (DAO) of the file being replicated.
        This is mostly information on the "expected" file (destination, checksum, size, etc)"""
    __tablename__ = 'files'

    
    path = Column(String, nullable=False, primary_key=True)
    name = Column(String, nullable=False, primary_key=True)
    size = Column(Integer)
    checksum_type = Column(String(5))
    checksum_value = Column(String(32))

    #these are defined backed in ReplicaEndpointDAO and represents the endpoints
    # from where this file might be gathered
    #endpoints = [ReplicaEndpointsDAO...]

    #some private variables
    __mapFileStr = None
    __drs = None

    def __init__(self):
        Base.__init__(self)
        DAO.__init__(self)
    
    def __init__(self, file_dict):
        DAO.__init__(self, file_dict)

        #defaults/optional values
        if 'local_size' in file_dict:
            self.local_size = file_dict['local_size']
        else:
            self.local_size = 0

        #now parse the endpoints
        self.endpoints = []
        for ep in file_dict['endpoints']:
            self.endpoints.append(ReplicaEndpointDAO(ep))


    def getDRS(self):
        if not self.__drs: self.__drs = DRS(filename=self.name, directory=self.path.strip('/'))
        return self.__drs
   
    def getMapFileEntry(self, root):
        if not self.__mapFileStr:
            list = []
            list.append(self.getDRS().getId())
            full_path = '/'.join([root, self.path, self.name]).replace('//','/')
            list.append(full_path)
            import os.path
        
            try :
                list.append(str(os.path.getsize(full_path)))
            except: pass
            try:
                list.append('mod_time=' + str(os.path.getmtime(full_path)))
            except: pass

            if self.checksum_value:
                list.append('checksum=' + self.checksum_value)
                list.append('checksum_type=' + self.checksum_type.upper())
            self.__mapFileStr = '|'.join(list)
        return self.__mapFileStr

class ReplicaEndpointDAO(Base, DAO):
    """Endpoint from which a replica can be gathered.
        Represents a source for the file to be gathered. Normally there will be multiple
        sources for each file."""
    __tablename__ = 'endpoints'
    __table_args__ = (ForeignKeyConstraint(['path','file_name'], ['files.path', 'files.name']), {})

    #Use the url as key for this relation
    url = Column(String, primary_key=True)
    type = Column(String(15))
    name = Column(String(15))
    protocol = Column(String(10))
    server = Column(String(20))
    port = Column(Integer)
    root = Column(String)
    path = Column(String, ForeignKey('files.path'))

    # link to the file table
    #file_path = Column(String, ForeignKey('files.path'))
    file_name = Column(String, ForeignKey('files.name'))

    #link to the file object
    file = orm.relation(ReplicaFileDAO, backref=orm.backref('endpoints', order_by = url), primaryjoin='ReplicaEndpointDAO.path == ReplicaFileDAO.path and ReplicaEndpointDAO.file_name == ReplicaFileDAO.name')

    def __init__(self):
        Base.__init__(self)
        DAO.__init__(self)

    def __init__(self, endpoint_dict):
        DAO.__init__(self, endpoint_dict)


class ReplicaDB(object):

    def __init__(self, db_url):
        self._db_url = db_url
        
        self._engine = create_engine(self._db_url)
        self._session = None

    def open(self):
        """Assure we have access to the DB and that it's properly created"""
        if not self._session:
            Base.metadata.create_all(self._engine)
            self._session = orm.scoped_session(orm.sessionmaker(self._engine, autoflush=False, autocommit=False, expire_on_commit=False))
            #self._session = orm.sessionmaker(bind=self._engine, autoflush=False, autocommit=False, expire_on_commit=False)()

    def __addAllTry(self, files):
        pass

    def __getDuplicates(self, fileDAOs):
        pass
        
    def add_all(self, files, inserted=None, rejected=None, update=False, overwrite=False, replace=False):
        """Insert all the info from the files list into the DB.
            return := list of all DAO objects representing the files dictionaries. return = inserted + rejected (see below)
            files := list of dictionaries describing the files and their endpoints
            **args:
                inserted := empty list where only the trully inserted objects will appear
                rejected := empty list where the rejected objects will appear (i.e. because they were already in the DB)
        """
        self.open()

        if inserted is None: inserted = []
        if rejected is None: rejected = []
        toUpdate = []

        if replace:
            for f in files:
                inserted.append(self._session.merge(ReplicaFileDAO(f)))
            #inserted.append(ReplicaFileDAO(f, session=self._session))

            log.debug('Now commiting')
            self._session.commit()

            return inserted 

        #get all the results we are interested in
        results = self._session.query(ReplicaFileDAO).filter(ReplicaFileDAO.name.in_([ f['name'] for f in files])).filter(ReplicaFileDAO.path.in_([ f['path'] for f in files])).all()

        #we need a hashed set to make this run faster
        db = {}
        for f in results: db[f]=f

        for f in files:
            f_dao = ReplicaFileDAO(f)
            if f_dao in db:
                if update: self.__updateFile(db[f_dao], f_dao, overwrite)
                rejected.append(f_dao)
            else:
                inserted.append(f_dao)
        self._session.add_all(inserted)
        self._session.commit()

        return inserted + rejected

    def __updateFile(self, old_file, new_file, overwrite):
        if not overwrite:
            #perform some checks and abort if we found something strange
            if old_file.size != new_file.size: 
                log.error('Size mismatch. old:%s, new:%s for %s%s', old_file.size, new_file.size, new_file.path, new_file.name)
                return False
            if old_file.checksum_value and old_file.checksum_value != new_file.checksum_value:
                log.error('Checksum mismatch. old:%s, new:%s for %s%s', old_file.checksum_value, new_file.checksum_value, new_file.path, new_file.name)
                return False

            #A new checksum is always wanted if we had none...
            if not old_file.checksum_value:
                old_file.checksum_value = new_file.checksum_value
                old_file.checksum_type = new_file.checksum_type
        else:
            #don't care, overwrite everything (we might want to check this in case we want to return if we indeed change something...)
            old_file.size = new_file.size
            old_file.checksum_value = new_file.checksum_value
            old_file.checksum_type = new_file.checksum_type

        #now add missing endpoints (this is always good... should we check for the old ones?)    
        new_endpoints = [ ep for ep in new_file.endpoints if ep not in old_file.endpoints]
        if new_endpoints: log.debug('new endpoints found: %s', new_endpoints)
    
        old_file.endpoints.extend(new_endpoints)

        return True 
            

    def get(self, **filter):
        self.open()
        return self._session.query(ReplicaFileDAO).filter_by(**filter)
         
    def getQuery(self):
        self.open()
        return self._session.query(ReplicaFileDAO)
        
    def commit(self):
        self.open()
        self._session.commit()

    def close(self):
        if self._session:
            self._session.close()

def test():
    
    log.debug('Testing replica_db')
    from gateway import Gateway
    g = Gateway('http://albedo2.dkrz.de/esgcet')
    files = g.listFiles('cmip5.output1.MPI-M.MPI-ESM-LR.historical.mon.seaIce.OImon.r1i1p1')

    import re
    db = ReplicaDB('sqlite:///:memory:')
    #db = ReplicaDB('sqlite:///replica.db')
    print "File metadata:"
    print files[0]
    inserted = []
    rejected = []
    files_o = db.add_all(files, inserted=inserted, rejected=rejected, update=True) #, overwrite=True)
    log.debug('Ins: %s, rej: %s', len(inserted), len(rejected) )

    print "File"
    print files_o[0]

    print "Query something..."
    f_dao = None
    for row in db.get():
        if not f_dao: f_dao = row
        print row.path[:20], "...", row.name, len(row.endpoints), [ ep.name for ep in row.endpoints]
    db.close()


    print f_dao.getDRS().getId(), f_dao.getDRS().getDirectory(), f_dao.getDRS().getFilename()
    print f_dao.getMapFileEntry('/My_root_dir/')
    return locals()

def testMap():
    db = ReplicaDB('sqlite:///ipsl.db')
    root = '/gpfs_750/transfer/replication_cmip5/cmip5/data'
    for row in db.get():
        print row.getMapFileEntry(root)
def testIPSL():
    import gateway   

    #connect to db
    db = ReplicaDB('sqlite:///ipsl2.db')

    #get datasets for replication
    datasets = ["cmip5.output1.IPSL.IPSL-CM5A-LR.aqua4K.day.atmos.cfDay.r1i1p1","cmip5.output1.IPSL.IPSL-CM5A-LR.aqua4K.day.atmos.day.r1i1p1","cmip5.output1.IPSL.IPSL-CM5A-LR.aqua4K.fx.atmos.fx.r0i0p0","cmip5.output1.IPSL.IPSL-CM5A-LR.aqua4K.fx.land.fx.r0i0p0","cmip5.output1.IPSL.IPSL-CM5A-LR.aqua4K.fx.ocean.fx.r0i0p0","cmip5.output1.IPSL.IPSL-CM5A-LR.aqua4K.3hr.atmos.3hr.r1i1p1","cmip5.output1.IPSL.IPSL-CM5A-LR.aqua4K.6hr.atmos.6hrLev.r1i1p1","cmip5.output1.IPSL.IPSL-CM5A-LR.aqua4K.6hr.atmos.6hrPlev.r1i1p1"]

    for ds in datasets:
        cmd = '-g BADC -of --parent {0}'.format(ds)
        files = gateway.main(cmd.split(' '))
        print len(files)
        print files[0]
        db.add_all(files)

def main(argv=None):
    
    if argv is None: argv = sys.argv[1:]
    import getopt
    try:
        args, lastargs = getopt.getopt(argv, "g:D:e:dvqh", ['help', 'gateway-url=', 'parent='])
    except getopt.error:
        print sys.exc_info()[:3]
        return 1

    #init values
    db_name = 'replica.db'
    gatewayURL = gatewayName = regex = None
    parent_set = False
    gw_args = "-o"
    #parse arguments
    for flag, arg in args:
        if flag=='-g':              gw_args = '%s -g %s' % (gw_args, arg)
        elif flag=='--gateway-url': gw_args = '%s --gateway-url %s' % (gw_args, arg)
        elif flag=='--parent': 
            gw_args = '%s --parent %s' % (gw_args, arg)
            parent_set = True
        elif flag=='-D':            db_name = arg
        elif flag=='-e':            
            import re
            regex = re.compile(arg)

#        elif flag=='-x':            retrieve_xml = True
#        elif flag=='-o':            retrieve_object = True

        elif flag=='-d':            log.setLevel(logging.DEBUG)
        elif flag=='-v':            log.setLevel(logging.INFO)
        elif flag=='-q':            log.setLevel(logging.NONE)
        elif flag=='-h' or flag=='--help': return 1
        
    if not parent_set: 
        gw_args = '-A ' + gw_args
        log.warn('Top level collection not set. Trying all known top-level collections (set with --parent)')
    else: gw_args = '-d ' + gw_args

    
    #Get datasets
    log.info('Retrieving datasets from Gateway')
    log.debug('cmd: %s', gw_args)
    ds = gateway.main(gw_args.split(' '))

    if ds: log.debug('Total datasets: %s', len(ds))
    else: 
        log.error('No Dataset was found!')
        return 0
    
    if regex:
        ds = [d['id'] for d in ds if regex.search(d['id'])]

    #prepare DB
    log.debug('DB: %s', db_name)
    log.debug('Entries: %s', len(ds))
    db = ReplicaDB('sqlite:///%s' % db_name)
    db.open()
    for d in ds:
        cmd = '{0} -of --parent {1}'.format(gw_args[3:], d)
        log.debug('getting files with %s', cmd)
        files = gateway.main(cmd.split(' '))
        if files:
            log.info('Adding %s files for dataset %s', len(files), d)
            db.add_all(files)
        else:
            log.error('no file found!')


    return 0

if __name__=='__main__':
    #configure logging
    logging.basicConfig(level=logging.WARN)
    import sys, gateway
    GW = gateway.GW
    usage="""replica_db.py [opt]
Grabs information regarding datasets and store them on DB ready for replication.
Opt:
    -h, --help  : show this help
    -g          : define Gateway (name) {0}
    --gateway-url: define Hessian API endpoit (either this or -g is required)
    --parent    : define a parent dataset for action
    -D          : name DB (default: replica.db)
    -e          : regular expresion to select datasets
    
    -q          : quiet mode
    -v          : verbose mode
    -d          : debug mode
""".format(GW.keys())

    ret_val = main()
    if isinstance(ret_val, int) and ret_val != 0: print usage
    sys.exit(ret_val)
