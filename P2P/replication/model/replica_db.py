#!/usr/local/cdat/bin/python
"""Manages access to the replica DB"""
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, sql, ForeignKey, orm
import metaconfig
import os

import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

#jfp copied from replica_manager.py:
from esgcet.config import loadConfig, initLogging
config = loadConfig(None)
#Final destination of files (the archive).  Typically this comes from ~/.esgcet/esg.ini
archive_root0 = config.get('replication', 'archive_root0')  # on gdo2: /cmip5/data
archive_root1 = config.get('replication', 'archive_root1')  # on gdo2: /css01-cmip5/data
archive_root2 = config.get('replication', 'archive_root2')  # on gdo2: /css02-cmip5/data
#temporal destinations of files and other data while completing the datasets
replica_root0 = config.get('replication', 'replica_root0')   # on gdo2: /cmip5/scratch
replica_root1 = config.get('replication', 'replica_root1')   # on gdo2: /css01-cmip5/scratch
replica_root2 = config.get('replication', 'replica_root2')   # on gdo2: /css02-cmip5/scratch
# not used: map_dir= os.path.join(replica_root, 'map')
#jfp was files_dir= os.path.join(replica_root, 'files')
files_dir0 = replica_root0                               # on gdo2: /cmip5/scratch
files_dir1 = replica_root1                               # on gdo2: /css01-cmip5/scratch
files_dir2 = replica_root2                               # on gdo2: /css02-cmip5/scratch

def __get_config(section, key):
    try:
        return metaconfig.get_config('replication').get(section, key)
    except:
        log.error("Cannot find %s in section %s", key, section,exc_info=True)
        #jfp was log.error("Cannot find %s in section %s", key, section,exec_inf=True)
        raise

##WORK AROUNDS---
# Set _database to "postgres", "mysql", or "sqllite" to avoid reading the config file twice
_database = "postgres"
if _database is None:
    _conf = metaconfig.get_config('replication')
    _dburl = _conf.get('db','dburl')
    _database = __get_config('db', 'dburl').split('://')[0]

# For Postgres:
if _database=="postgres" or _database=="postgresql":
    try:
        from sqlalchemy.dialects.postgresql import BIGINT as MyBigInteger
        _database = "postgresql"
    except:
        from sqlalchemy.databases.postgres import PGBigInteger as MyBigInteger
# For MySQL:
elif _database=="mysql":
    from sqlalchemy.databases.mysql import MSBigInteger as MyBigInteger, MSDouble as MyDouble
# For SQLlite:
elif _database=="sqllite":
    MyBigInteger = sqlalchemy.types.Integer
else:
    raise Exception("No database defined in model/__init__.py")
##---

Base = declarative_base()


def enum(*sequential, **named):
    """Creates a struct/enum type"""
    enums = dict(zip(sequential, range(len(sequential))), **named)
    e = type('Enum', (), enums)    
    e._keys = enums.keys()
    e._values = enums.values()
    return e

STATUS = enum(UNINIT=0, INIT=10, DOWNLOADING=20, VERIFYING=30, 
        RETRIEVED=100, FINAL_DIR=110, CHOWNED=120, ESGCET_DB=130, 
        TDS=140, PUBLISHED=150, ERROR=-1)
"""Defines the status of datsets and/or files.

=============  =====  ==================================================================
   Name        Value           Description
=============  =====  ==================================================================
*UNINIT*         0    Creation is not finished.
*INIT*          10    Creation finished sucessfully.
*DOWNLOADING*   20    A download lisst was generated, probably downloading.
*VERIFYING*     30    Dataset is being verified
*RETRIEVED*    100    Dataset is complete and verified
*FINAL_DIR*    110    Dataset moved to final location
*CHOWNED*      120    Dataset ownership and permits changed to that of the final archive
*ESGCET_DB*    130    Dataset is ingested into the publisher DB
*TDS*          140    Catalog was generated
*PUBLISHED*    150    Dataset is succesfully published to a Gateway/Index
*ERROR*         -1    Verification failed
=============  =====  =================================================================="""


class DAO(object):
    """Basic class for providing string representation of DAO Objects"""

    def __str__(self):
        """Short representation, only primary keys"""
        return "<{0} id:{1}>".format(self.__class__.__name__, 
                "".join(orm.object_mapper(self).primary_key_from_instance(self)))

    def __repr__(self):
        """Complete representation of this object."""
        return "<{0} ({1})>".format(self.__class__.__name__, 
            ",".join(map(lambda k: k+"="+str(self.__dict__[k]), 
                        filter(lambda name: name[0] != '_', self.__dict__))))


class ReplicaAccess(Base, DAO):
    """This is the Object used for modelling acces to the file."""
    __tablename__ = 'file_access'
    __table_args__ = {'schema':'replica'}
    url = Column(String, primary_key=True)
    abs_path = Column(String, ForeignKey('replica.files.abs_path'))
    type = Column(String, nullable=False)

class ReplicaFile(Base, DAO):
    __tablename__ = 'files'
    __table_args__ = {'schema':'replica'}
    abs_path = Column(String, primary_key=True)
    dataset_name = Column(String,  ForeignKey('replica.datasets.name'))
    checksum = Column(String, nullable=False)       #nullable:=avoid replicating things we can't be sure of
    checksum_type =  Column(String, default='md5')
    size = Column(MyBigInteger, nullable=False) #Integer is not enough and sqlalchemy <6.x had no standard one.
    mtime = Column(Float)
    status = Column(Integer, default=STATUS.UNINIT)
    access = orm.relation(ReplicaAccess, backref=orm.backref('file'), order_by = ReplicaAccess.url, cascade='all, delete')
    def getFinalLocations(self):
        return [ os.path.join(archive_root0, self.abs_path),
                 os.path.join(archive_root1, self.abs_path),
                 os.path.join(archive_root2, self.abs_path)]
    def getFinalLocation(self):
        return self.getFinalLocations[2]

    def getDownloadLocations(self):
        return [ os.path.join(files_dir0, self.abs_path),
                 os.path.join(files_dir1, self.abs_path),
                 os.path.join(files_dir2, self.abs_path) ]
    def getDownloadLocation(self):
        return self.getDownloadLocations[2]

class ReplicaDataset(Base, DAO):
    """Manages the datasets being replicated"""
    __tablename__ = 'datasets'
    __table_args__ = {'schema':'replica'}
    name = Column(String, primary_key=True)
    version = Column(Integer, nullable=False) #only one version at a time!
    status = Column(Integer, default=STATUS.UNINIT)
    gateway = Column(String, nullable=False)
    parent = Column(String, nullable=False)
    catalog = Column(String, nullable=False)
    size = Column(MyBigInteger, nullable=False)
    filecount = Column(Integer, nullable=False)
    files = orm.relation(ReplicaFile, backref=orm.backref('dataset' ), order_by = ReplicaFile.abs_path, cascade='all, delete' )
    
    path = None #path to the root of this dataset (up to version in DRS structure)
    drs = None
    #I have to figure out how to use something like this in python/sqlalchemy...
    class DRS():
        __drs_elem = {'activity':0,'project':0,'product':1,'institute':2,'institution':2,'model':3,'experiment':4,
        'frequency':5,'realm':6,'cmor_table':7,'ensemble':8,'rip':8}
        
        def __init__(self, parent):
                self.__parent = parent
        
        def __getattr__(self, item):
            if self.__parent.name and item in ReplicaDataset.DRS.__drs_elem:
                return self.__parent.name.split('.')[ReplicaDataset.DRS.__drs_elem[item]]
            raise NameError("name '%s' is not defined" % item)

    def getDRS(self):
        if not self.drs:
            self.drs = ReplicaDataset.DRS(self)
        return self.drs

    def getPath(self):
        """Extract the path to this dataset"""
        if not self.path and self.files:
            #extrac from one file (first 10 components of DRS)
            self.path =  '/'.join(self.files[0].abs_path.split('/')[:10])
        return self.path

# notused:
#    def getFinalPath(self):
#        return os.path.join(archive_root, self.getPath())
#
#    def getDownloadPath(self):
#        return os.path.join(files_dir, self.getPath())

    def getMapfile(self, root):
        lines=[]
        for f in self.files:
            if not f.mtime:
                f.mtime = os.path.getmtime(os.path.join(root,f.abs_path))
            lines.append('|'.join([self.name, os.path.join(root,f.abs_path), str(f.size), 'mod_time=%f|checksum_type=%s|checksum=%s' % (f.mtime,f.checksum_type,f.checksum)]))
        return '\n'.join(lines) + '\n'
        

class ArchivedFile(Base,DAO):
    """Files already retrieved and published"""
    __tablename__ = 'archive'
    __table_args__ = {'schema':'replica'}
    abs_path = Column(String, primary_key=True)
    checksum = Column(String, nullable=False)       #nullable:=avoid replicating things we can't be sure of
    checksum_type =  Column(String, default='md5')
    size = Column(MyBigInteger, nullable=False) #Integer is not enough and sqlalchemy <6.x had no standard one.
    mtime = Column(Float, nullable=False)
    gateway = Column(String, nullable=False)
    catalog = Column(String, nullable=False)


# Special download list from well known repos
repos = { 
    'badc': { 'id' : 1, 'file_mod' : lambda file : 
            'gsiftp://capuchin.badc.rl.ac.uk:2812/badc/cmip5/data/' + file.abs_path},
    'dkrz': { 'id' : 2, 'file_mod' : lambda file : 
            'gsiftp://cmip2.dkrz.de:2812/wdcc' + file.abs_path},
    'pcmdi':{ 'id' : 3, 'file_mod' : lambda file : 
            'gsiftp://pcmdi7.llnl.gov:2812/' + [a.url[53:] for a in file.access if a.url[:13] == 'http://pcmdi7'][0]},
    'nci':  { 'id' : 4, 'file_mod' : lambda file : 
            'gsiftp://esgnode1.nci.org.au:2812//' + file.access[0].url[35:]},
    'ncar': { 'id' : 5, 'file_mod' : lambda file : 
            'gsiftp://vetsman.ucar.edu:2811//datazone/' + file.access[0].url[37:]}
}

class ReplicaRepo(Base,DAO):
    """Datasets present at well known repos"""
    __tablename__ = 'repos'
    __table_args__ = {'schema':'replica'}
    repo = Column(String, primary_key=True)
    dataset = Column(String, primary_key=True)
    version = Column(Integer, primary_key=True)
    mtime = Column(Float, nullable=False)
    
    __order = ['badc','pcmdi', 'ncar', 'nci']
    __cache = None
    """Cache holding the datsets being replicated and the repo entries order by priority"""

    @staticmethod
    def import_from_file(file, repo_name, replace=True):
        """Ingest into the DB datasets contained in a file as output from
globus.py
Ex: cmip5.output1.BCC.bcc-csm1-1.1pctCO2.day.atmos.day.r1i1p1.v20110101"""
        #check the file is there
        if not os.path.isfile(file):
            print "No such file"
            return

        #check the repo is known
        if repo_name not in repos:
            print "Unknown Repo"
            return

        skipped = 0
        mtime = os.path.getmtime(file) 
        db = getReplicaDB()

        #for now just remove and reingest, we can turn this later off.
        if replace:
            all = db.query(ReplicaRepo).filter_by(repo=repo_name)
            db.flush()
            print "Removing all existing (%d) ... " % all.count(),
            all.delete()
            print "done!"

        print "loading from %s ... " % file,
        for line in open(file, 'r'):
            try:
                split = line.rindex('.v')
                dataset, version = line[:split], line[split+2:]
                db.add(ReplicaRepo(repo=repo_name, dataset=dataset, version=int(version), mtime=mtime))
            except ValueError: 
                print "can't parse '%s'" % line
                skipped += 1
                
        total = len(db.new)
        db.commit()  
        print "%s added" % total
        if skipped > 0:
            print "%s lines where skipped" % skipped

    @staticmethod
    def get_repo_datasets():
        """Return dictionary with datasets in repos"""
        if ReplicaRepo.__cache is None:
            import bisect
            ReplicaRepo.__cache = {}                
            for dataset, repo in getReplicaDB().query(ReplicaDataset, ReplicaRepo).join(
                        ReplicaRepo, sql.and_(ReplicaDataset.name == ReplicaRepo.dataset, 
                                    ReplicaDataset.version == ReplicaRepo.version)).filter(
                        ReplicaDataset.status<=STATUS.DOWNLOADING):
                if dataset not in ReplicaRepo.__cache: 
                    ReplicaRepo.__cache[dataset] = [repo]
                else:
                    #insert in sorted order (overkill... we won't have more than 3 repos)
                    bisect.insort(ReplicaRepo.__cache[dataset], repo)
        return ReplicaRepo.__cache

    def __cmp__(self, other):
        if other.repo in ReplicaRepo.__order:
            return ReplicaRepo.__order.index(self.repo) - ReplicaRepo.__order.index(other.repo)
        #if not is larger (meaning lower priority)
        return 999

__rep_s=None
def getSession(shared=True):
    """Get a shared/new session"""
    global __rep_s
    if shared and __rep_s: return __rep_s

    #prepare replica_db
    db_string =  __get_config('db', 'dburl')
    tmp = db_string.split(':')
    if tmp[0].find(_database) == -1:
        #modiy for current dialect (sqlalchemy posgres dialect named changed 0.6 onwards)
        db_string = _database + ':' + ':'.join(tmp[1:])
    e = sqlalchemy.create_engine(db_string)
    Base.metadata.create_all(e)
    session = orm.sessionmaker(bind=e, autoflush=False, autocommit=False,expire_on_commit=False)()

    #if here, set the local session if it is shared
    if shared: __rep_s = session
    return session
