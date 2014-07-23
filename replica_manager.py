#!/usr/apps/esg/cdat6.0a/bin/python

#!/usr/local/cdat/bin/python
"""This file is specially tailored for WDCC. Adapt and use at your own risk."""

import sqlalchemy
from esgcet.publish import parseDatasetVersionId
from esgcet.config import loadConfig, initLogging
from esgcet.model import Dataset, DatasetVersion
from esgcet.query import printResult
from esgcet.messaging import warning
import os,sys,shutil, time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, sql, ForeignKey, orm
from sqlalchemy import exc
import logging
rmlog = logging.getLogger('replica_manager')
rmlog.addHandler( logging.FileHandler('replica_manager.log') )
# import cmipmd5  #jfp

##WORK AROUNDS---
# Set _database to "postgres", "mysql", or "sqllite" to avoid reading the config file twice
_database = "postgres"
if _database is None:
    from esgcet.config import loadConfig1
    import urlparse
    dburl = loadConfig1(None).get('replication', 'replica_db')
    urltuple = urlparse.urlparse(dburl)
    _database = urltuple[0]
# For Postgres:
if _database=="postgres":
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
    raise ESGPublishError("No database defined in model/__init__.py")
##---

Base = declarative_base()
#global values
rep_s = dataset_match = dataset_matchnot = None
report_types = ['error', 'error-files', 'pending', 'pending-files', 'retrieved', 'retrieved-files',\
                'todo', 'status-[status]']

usage="""replica_manager.py [opt] Action [Action]
Actions:
    --find-datasets:    search for datasets available for replicating and update DB with its data
    --download-list <type>:    create the download lists of the given type (bdm, list-<endpoint_type>)
    --verify:           Verify the checksums and that the dataset is complete
    --update:           Update the ckecksums with new information from the catalogs
    --report <type>:    provide some reports. Known types: """ + ','.join(report_types) + """
    --publish:          Not all users are allowed to do this!
    --archive:          store published replica and delete from DB
opt:
    --dataset   : SQL like similar clause for matching dataset names. Only these will be considered
                  for the action.
    --datasetnot : SQL 'not like' clause for matching dataset names. Most actions do not support this,
      but the verify action will not consider dataset names which match the value of datasetnot.
    --file      : use this file for output (download list, etc)
    -q          : quiet mode
    -v          : verbose mode
    -d          : debug mode
"""
def main(argv=None):
    import getopt

    if argv is None: argv = sys.argv[1:]

    try:
        args, lastargs = getopt.getopt(argv, "hdvq", ['help', 'find-datasets', 'download-list=', 
            'verify', 'discover', 'report=', 'dataset=', 'datasetnot=', 'file=', 'no-checksums',
            'update', 'move', 'chown', 'mapfile', 'multi-mapfiles',
            'publish', 'archive', 'clean', 'skip_hardlinks'])
    except getopt.error:
        rmlog.error( sys.exc_info()[:3] )
        return 1

    #init values
    find_datasets =  verify = discover =  update = move = chown = mapfile = multi_mapfiles =\
                    publish = archive = clean = False
    skip_hardlinks = False
    global dataset_match, dataset_matchnot
    file = download_type = report_type = None
    do_checksums = True

    #parse arguments
    for flag, arg in args:
        #actions
        if flag=='--find-datasets':     find_datasets = True
        elif flag=='--download-list':   download_type = arg
        elif flag=='--verify':          verify = True
        elif flag=='--discover':        discover = True
        elif flag=='--skip-hardlinks':  skip_hardlinks = True
        elif flag=='--clean':           clean = True
        elif flag=='--report':          report_type = arg
        elif flag=='--update':          update = True
        elif flag=='--move':            move = True
        elif flag=='--chown':           chown = True
        elif flag=='--mapfile':         mapfile = True
        elif flag=='--multi-mapfiles':         multi_mapfiles = True
        elif flag=='--publish':         publish = True
        elif flag=='--archive':         archive = True

        #options
        elif flag=='--dataset':         dataset_match = arg
        elif flag=='--datasetnot':      dataset_matchnot = arg
        elif flag=='--file':            file = arg
        elif flag=='--no-checksums':    do_checksums = False
#jfp        elif flag=='-d':            rmlog.setLevel(logging.DEBUG)
#jfp        elif flag=='-v':            rmlog.setLevel(logging.INFO)
#jfp        elif flag=='-q':            rmlog.setLevel(logging.NONE)
        elif flag=='-h' or flag=='--help':
            global usage
            rmlog.error( usage )
            return 0

    if download_type=='T' or download_type=='t' or download_type=='HTTP' or download_type=='http':
        download_type='list_HTTPServer'
    if not (find_datasets or download_type or verify or discover or clean or report_type or
            update or move or chown or mapfile or multi_mapfiles or publish or archive):
        rmlog.error( "You must select at least one action" )
        return 1

    #we have to scape something in sqlachemy apparently...
    if dataset_match: dataset_match.replace('%','%%')
    else: dataset_match = '%%'
    if dataset_matchnot: dataset_matchnot.replace('%','%%')

    if find_datasets: fill_replica_db()
    if download_type: create_download_lists(file=file, type=download_type)
    if verify: verify_datasets(skip_hardlinks,do_checksums)
    if discover: discover_datasets(skip_hardlinks,do_checksums)
    if clean: clean_errors()
    if report_type: 
        stat = show_report(type=report_type)
        if stat: return stat
    if update:  update_replica_db()
    if move: move_to_final_dir()
    if chown: change_ownership()
    if mapfile: create_mapfile(file)
    if multi_mapfiles: dumpMapfiles()
    if publish: publishReplica()
    if archive: archiveReplica()

    #if here everything ended ok
    return 0

config = loadConfig(None)

###########################################
#### PATHS #################################
#########################################

#Final destination of files (the archive).  Typically this comes from ~/.esgcet/esg.ini
archive_root0 = config.get('replication', 'archive_root0') # on gdo2: /cmip5/data
archive_root1 = config.get('replication', 'archive_root1') # on gdo2: /css01-cmip5/data
archive_root2 = config.get('replication', 'archive_root2') # on gdo2: /css02-cmip5/data
archive_root3 = config.get('replication', 'archive_root3') # on gdo2: /css02-cmip5/cmip5/data

#temporal destinations of files and other data while completing the datasets
replica_root0 = config.get('replication', 'replica_root0')   # on gdo2: /cmip5/scratch
replica_root1 = config.get('replication', 'replica_root1')   # on gdo2: /css01-cmip5/scratch
replica_root2 = config.get('replication', 'replica_root2')   # on gdo2: /css02-cmip5/scratch

# no longer used: map_dir= os.path.join(replica_root2, 'map')

#jfp was files_dir= os.path.join(replica_root2, 'files')
files_dir0 = replica_root0                               # on gdo2: /cmip5/scratch
files_dir1 = replica_root1                               # on gdo2: /css01-cmip5/scratch
files_dir2 = replica_root2                               # on gdo2: /css02-cmip5/scratch

#############################################

def enum(*sequential, **named):
    """Creates a struct/enum type"""
    enums = dict(zip(sequential, range(len(sequential))), **named)
    e = type('Enum', (), enums)    
    e._keys = enums.keys()
    e._values = enums.values()
    return e

STATUS = enum(UNINIT=0, INIT=10, DOWNLOADING=20, VERIFYING=30,\
              OBSOLETE=50, WITHDRAWN=51, UNAVAILABLE=52,\
              RETRIEVED=100, FINAL_DIR=110, CHOWNED=120, ESGCET_DB=130, TDS=140, PUBLISHED=150,\
              ERROR=-1, MULTIPLE_ERRORS=-10)

class DAO(object):

    def __str__(self):
        """Short representation, only primary keys"""
        return "<{0} id:{1}>".format(self.__class__.__name__, "".join\
                                     (orm.object_mapper(self).primary_key_from_instance(self)))

    def __repr__(self):
        """Complete representation of this object."""
        return "<{0} ({1})>".format(self.__class__.__name__, \
            ",".join(map(lambda k: k+"="+str(self.__dict__[k]),\
                         filter(lambda name: name[0] != '_', self.__dict__))))


class ReplicaAccess(Base, DAO):
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
    checksum = Column(String, nullable=False)   #nullable:=avoid replicating things we can't be sure of
    checksum_type =  Column(String, default='md5')
    size = Column(MyBigInteger, nullable=False) #Integer is not enough and sqlalchemy <6.x had no standard one.
    mtime = Column(Float)
    status = Column(Integer, default=STATUS.UNINIT)
    #jfp was access = orm.relation(ReplicaAccess, backref=orm.backref('file'), order_by = ReplicaAccess.url, cascade='all, delete')
    access = orm.relation(ReplicaAccess, backref=orm.backref('file'), order_by = ReplicaAccess.url )
    location = None # may get set during verification
    def getFinalLocations(self):
        return [ os.path.join(archive_root0, self.abs_path),
                 os.path.join(archive_root1, self.abs_path),
                 os.path.join(archive_root2, self.abs_path),
                 os.path.join(archive_root3, self.abs_path) ]

    def getDownloadLocation(self):
        if   self.abs_path.find("/BCC/")>0 or\
             self.abs_path.find("/CNRM-CERFACS/")>0 or\
             self.abs_path.find("/COLA-CFS/")>0 or\
             self.abs_path.find("/ICHEC/")>0 or\
             self.abs_path.find("/IPSL/")>0 or\
             self.abs_path.find("/LASG-CESS/")>0 or self.abs_path.find("/LASG-IAP/")>0 or\
             self.abs_path.find("/MIROC/")>0 or\
             self.abs_path.find("/MPI-M/")>0 or\
             self.abs_path.find("/MRI/")>0 or\
             self.abs_path.find("/NIMR-KMA/")>0 or\
             self.abs_path.find("/NOAA-GFDL/")>0 or\
             self.abs_path.find("/NOAA-NCEP/")>0 or\
             self.abs_path.find("/NSF-DOE-NCAR/")>0 :
            # Download to new CSS section
            return os.path.join(files_dir1, self.abs_path)
        else:
            # Download to original CSS section
            return os.path.join(files_dir2, self.abs_path)

class ReplicaDataset(Base, DAO):
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
    #jfp was files = orm.relation(ReplicaFile, backref=orm.backref('dataset' ), order_by = ReplicaFile.abs_path, cascade='all, delete' )
    files = orm.relation(ReplicaFile, backref=orm.backref('dataset' ), order_by = ReplicaFile.abs_path )
    
    path = None #path to the root of this dataset (up to version in DRS structure)

    def getPath(self):
        """Extract the path to this dataset"""
        if not self.path and self.files:
            #extrac from one file (first 10 components of DRS)
            self.path =  '/'.join(self.files[0].abs_path.split('/')[:10])
        return self.path

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

def getReplicaDB():
    global rep_s
    if not rep_s:
        #prepare rplica_db
        e = sqlalchemy.create_engine(config.get('replication', 'replica_db'))
        Base.metadata.create_all(e)
        #jfp was rep_s = orm.sessionmaker(bind=e, autoflush=False, autocommit=False,expire_on_commit=False)()
        rep_s = orm.sessionmaker(bind=e, autoflush=False, autocommit=False)()
    return rep_s
###########################################################
####### --- Preparing datasets available for download --- ####
################################################################

def push_dataset_aside( rd, previous_files, engine, Session ):
    # rd is a ReplicaDataset found from database rep_s.
    # This function makes a re-named copy of it and associated files, and then deletes it
    # and the associated files.  The purpose is to make room to put a newer version of the
    # dataset into the database replica.datasets.
    # previous_files is a checksum-based dictionary by which the files in the old rd can be found later.
    t0=time.time()
    global rep_s

    # rd is obsolete.  Make a copy with a different name and status...
    mod_rd = ReplicaDataset( name="old_"+str(rd.version)+'_'+rd.name, version=rd.version,\
                             status=STATUS.OBSOLETE, gateway=rd.gateway,\
                             parent=rd.parent, size=rd.size, filecount=rd.filecount,catalog=rd.catalog )
    rep_s.add(mod_rd)
    # The new copy of the dataset still has those files...
    # count = 0

    # The files pointing to rd should instead point to the copy, mod_rd.
    for file in rd.files:
        if file.dataset_name==rd.name:
#            mod_file = ReplicaFile( abs_path=file.abs_path, dataset_name=mod_rd.name,\
#                                    checksum=file.checksum, checksum_type=file.checksum_type,\
#                                    size=file.size, mtime=file.mtime, status=file.status )
#            rep_s.add( mod_file )
            # incredibly slow, do instead with engine.execute (below) ... rep_s.delete( file )
            file.dataset_name = mod_rd.name
            location = file.getDownloadLocation()
            # This could miss the case where a copy of the file had previously been downloaded
            # to a different directory (e.g. css01,css02,gdo2); but it's not essential to get
            # every case for previous_files; a few extra downloads won't hurt...
            if not os.path.exists(location):
                location = None
                for loc in file.getFinalLocation():
                    if os.path.exists(loc):
                        location = loc
                        break
            if location != None:
                previous_files[file.checksum] = (location, file.checksum, file.checksum_type)

        # if count<10: print mod_file.abs_path, mod_file.dataset_name, location
        # count+=1

    # Now that copies have been made of the existing (old) dataset and its files, we want
    # to delete the originals to make room for the new versions; and also delete those files'
    # file_access rows (there's no need for a copy of the file_access data - it's old, so if we
    # don't have it, we don't want it).
    # A cascading delete of the datase would do the job, and SQLAlchemy deletes are normally cascading.
    # However, occasionally a spurious lock makes the cascading delete hang.  Maybe it has to do with
    # the fact that Postgres is set up with non-cascading deletes.  The solution is to explicitly
    # delete the file and file_access data.
    # Here is straightfoward SQL to do that deletion.  It could be executed with
    # "engine.execute(sqltext(sqlcommand)) but that affects only the database, not the orm
    # representation which is generally how thses scripts work.  So we can't just run this SQL...
    # sqlcommandfa= "DELETE FROM replica.file_access WHERE abs_path IN ( SELECT abs_path FROM replica.files"+
    #           " WHERE dataset_name='"+rd.name+"');"
    # sqlcommandf = "DELETE FROM replica.files WHERE dataset_name='"+rd.name+"';"
    # Following are the SQLAlchemy query-delete calls which are equivalent to the above SQL, but
    # very much slower.

    # Delete the file_access referencing older version files: we don't want them.
    rep_s.flush()  # makes the new file.dataset_name visible to file_access rows
    # N.B. the with_labels() is probably unnecessary: it's a remnant of some messing around...
#    fquery  = rep_s.query(ReplicaFile         ).filter(ReplicaFile.dataset_name.like(rd.name)).with_labels()
#    fsquery = rep_s.query(ReplicaFile.abs_path).filter(ReplicaFile.dataset_name.like(rd.name)).with_labels()
    fsquery = rep_s.query(ReplicaFile.abs_path).filter(ReplicaFile.dataset_name.like(mod_rd.name)).with_labels()
    aquery = rep_s.query(ReplicaAccess).filter(ReplicaAccess.abs_path.in_(fsquery))
    # an = aquery.delete(synchronize_session=False)
    an = aquery.delete(synchronize_session='fetch')

#    # When the older version files were copied, we would have deleted the originals here...
    # fn = fquery.delete(synchronize_session=False)
#    fn = fquery.delete(synchronize_session='fetch')
#    rmlog.info("jfp %d files rows deleted; now about to delete older version dataset, time=%d"%(fn,td))
    rmlog.info("jfp %d file_access rows deleted; about to commit"%(an))
    # The commit is needed so that the dataset will know that there are no longer files pointing to it.
    # Maybe just a flush() would be good enough.
    rep_s.commit()

    # Finally, it's safe to delete the old dataset:
    rep_s.delete(rd)
    rep_s.commit()



def fill_replica_db(allow_empty_md5=True):
    """The replica DB will be initialized with data from files that need to be replicated"""
    import gateway
    global config, archive_root0,rmlog
    rep_s = getReplicaDB()

    known = set()

    #add all datasets in process
    for d in getReplicaDB().query(ReplicaDataset).filter(ReplicaDataset.name.like(dataset_match)):
        known.add('%s#%s' % (d.name, d.version))

    #add all dataset already published
    engine = sqlalchemy.create_engine(config.get('replication', 'esgcet_db'), echo=False, pool_recycle=3600)
    Session = orm.sessionmaker(bind=engine, autoflush=True, autocommit=False) 
    session = Session()
    for d in session.query(Dataset).filter(Dataset.name.like(dataset_match)):
        for v in d.versions:
            known.add('%s#%s' % (d.name, v.version))
    session.close()

    #prepare cmip5 conversion
    from drslib import cmip5, translate
    trans = cmip5.make_translator('cmip5')

    view_e = sqlalchemy.create_engine(config.get('replication', 'cmip5_view_db'), echo=False, pool_recycle=3600)
    #we still hit more than one if multiple replicas are published
    #jfp the clause "d.parent_gateway = d.master_gateway" prevents getting the same dataset twice (it could have been replicated)
    #jfp at the cost of eliminating most of the interesting datasets...
    #jfp for dataset in view_e.execute(sql.text("SELECT d.id, d.version, parent_gateway, size, filecount, parent_id, catalog from (SELECT id, max(version) as version from global.datasets group by id) as uniq join global.datasets as d on (uniq.id=d.id and uniq.version=d.version) WHERE d.id like :dataset"), dataset=dataset_match).fetchall():
      #jfp  # without the troublesome where clause, let's just see what we get:
      #jfp if (dataset[0]).find("NCAR.CCSM4.rcp26")>-1:
      #jfp     rmlog.debug( "1 ", dataset[0] )

#jfp: was    for dataset in view_e.execute(sql.text("SELECT d.id, d.version, parent_gateway, size, filecount, parent_id, catalog from (SELECT id, max(version) as version from global.datasets group by id) as uniq join global.datasets as d on (uniq.id=d.id and uniq.version=d.version) WHERE d.parent_gateway = d.master_gateway AND d.id like :dataset"), dataset=dataset_match).fetchall():
    for dataset in view_e.execute(sql.text("SELECT d.id, d.version, parent_gateway, size, filecount, parent_id, catalog from (SELECT id, max(version) as version from global.datasets group by id) as uniq join global.datasets as d on (uniq.id=d.id and uniq.version=d.version) WHERE                                              d.id like :dataset"), dataset=dataset_match).fetchall():
        #fast skip datasets already ingested or in the proces
        if '%s#%s' % (dataset['id'],dataset['version']) in known:
            dataset_name = dataset['id']
        try: 
            dataset_name = dataset['id']
            dataset_version = dataset['version']
            dataset_gateway = dataset['parent_gateway']
            dataset_size = dataset['size']
            dataset_filecount = dataset['filecount']
            dataset_parent = dataset['parent_id']
            dataset_catalog = dataset['catalog']
            catalog_chksums = None
            
            #to cancel commit if some error appear
            commit_changes = True

            print "jfp dataset_name=",dataset_name
            #we will store here all files from last version (if any)
            previous_files = {}

            if rep_s.query(ReplicaDataset).filter(ReplicaDataset.name==dataset_name).count() > 0:
                #at this point we don't care, it's already being processed
                #we might want to check the status later on
                #jfp added all code here but "continue"
                get_the_new_version = False
                qrd=rep_s.query(ReplicaDataset).filter(ReplicaDataset.name==dataset_name)
                for rd in qrd:
                    if rd.version<dataset_version:
                        rmlog.warn( ("dataset %s with version %d has been superseded\n"+10*' '+
                                     "by version %s.  We'll try to get it!" )\
                                    % ( dataset_name, rd.version, dataset_version ) )
                        push_dataset_aside( rd, previous_files, engine, Session )
                        get_the_new_version = True
                    elif rd.status == STATUS.WITHDRAWN or rd.status == STATUS.UNAVAILABLE:
                        # dataset was retracted/withdrawn or simply messed up, then republished
                        rd.status = STATUS.INIT
                if not get_the_new_version:
                    continue
            #jfp former else: clause...
            rmlog.info( "processing: %s \tfrom gateway: %s" % (dataset_name, dataset_gateway) )
            rep_ds = ReplicaDataset(name=dataset_name,version=dataset_version, gateway=dataset_gateway,parent=dataset_parent,
                size=dataset_size,filecount=dataset_filecount, catalog=dataset_catalog)
            rep_s.add(rep_ds)
            #persist that we have startet here.
            #rep_s.commit()
            #jfp ...former else: clause
                
            #check this is what we expect (e.g. no new version in between)
            dataset_remote = gateway.main(('-g %s --parent %s -mo' % (dataset_gateway, dataset_name)).split(' '))
            if dataset_remote is None:
                continue              

            if dataset_version != int(dataset_remote['version']):
                rmlog.info( "A new version was published!" )
                #for now we don't care, we use the newer one
                dataset_version = int(dataset_remote['version'])
                dataset_catalog = dataset_remote['catalog']
                dataset_size = -1
                dataset_filecount = -1
                rep_ds.version = dataset_version
                
            session = Session()
            local_ds = session.query(Dataset).filter(Dataset.name==dataset_name).first()    #should be 0 or 1 at most
            session.close()

            if local_ds:  #jfp never true, because Dataset is generic, all queries return None
                raise "jfp doesn't think this will ever happen"
                print "jfp - ok, I'm wrong"
                rmlog.info( "jfp - ok, I'm wrong" )
                version = local_ds.getVersion()
                if version > dataset_version:
                    rmlog.warn( "Dataset was retracted!" )
                    rep_ds.status = STATUS.ERROR
                    continue
                elif version == dataset_version:
                    rmlog.info( "dataset is up to date" )
                    rep_ds.status = STATUS.RETRIEVED
                    continue
                else:   
                    #we have a local older one- update!
                    #find local files and checksums
                    #dsver =session.query(Dataset, DatasetVersion).filter(Dataset.id==DatasetVersion.dataset_id).filter(Dataset.name==dataset_name).filter(DatasetVersion.version==version).first()
                    dsver = local_ds.getLatestVersion()
                    #handle the files
                    #e.g. dsver.files[0].location[len('/gpfs_750/projects/CMIP5/data'):]
                    missing_chksum = []
                    for file in dsver.files:
                        path = file.location[len(archive_root0):]
                        #...jfp: this line is wrong if there are four archive locations (archive_root0
                        # through archive_root3, but it doesn't matter because this whole section
                        # doesn't get exercised.
                        if path[0] == '/': path = path[1:]
                        filename = path[path.rindex('/')+1:]
                        path = path[:-len(filename)-1]
                        checksum = file.checksum
                        checksum_type = file.checksum_type
                        if checksum:
                            previous_files[checksum] = (file.location, file.checksum, file.checksum_type)
                        else:
                            missing_chksum.append(file)     
                            
                    if missing_chksum:
                        rmlog.warn( "There were %s files without checksum! Skipping this dataset..." %\
                                  len(missing_chksum) )
                        #don't continue with it, we have enough to do anyways.
                        rep_ds.status = STATUS.ERROR
                        continue
                    #this shouldn't really happen, but it's not impossible...
                    if len(dsver.files) != (len(missing_chksum) + len(previous_files)):
                        rmlog.warn( 'Unexpected md5 collsion!' )
                        #what now?! <- implement properly, or download one file more :-) ... one file more it is ;-)
            #else:
            #there's nothing local

            #now get remote files
            dataset_remote_files = gateway.main(('-g %s --parent %s -of' % (dataset_gateway, dataset_name)).split(' '))
            if dataset_remote_files is None:
                continue
            files_added=[]  # added by jfp for duplicate check
            for file in dataset_remote_files:
                if dataset.id.split('.')[1]=='output2':
                    # Not only is output2 data unimportant, but it is susceptible to drslib errors
                    # where the variable is not recognized by get_var_attr (called, ultimately, by
                    # the filename_to_drs() call below).
                    continue
                if dataset.id.split('.')[1]=='restricted':
                    # This is specific to CSIRO I don't know what this is, but probably we don't want it.
                    continue
                try:
                    drs = trans.filename_to_drs(file['name'])
                except (translate.TranslationError, ValueError):
                    # If drslib doesn't recognize a variable or other name, it shouldn't stop
                    # everything else!
                    rmlog.warn( "drslib error with " + file['name'] )
                    continue
                #complete this from the id of the remote
                drs.version = dataset_version
                drs.product, drs.institute, drs.model =  dataset_name.split('.')[1:4]
                
                #compute new path
                try:
                    abs_path = trans.drs_to_filepath(drs)
                except translate.TranslationError:
                    # If drslib doesn't recognize a variable or other name, it shouldn't stop everything else!
                    rmlog.warn( "drs_to_filepath error with" + file['name'] )
                    continue
                if abs_path[-len(file['name']):] != file['name']:
                    #jfp added special treatement of  '-clim' and '_clim':
                    l = -len(file['name'])
                    modold = (file['name']).replace('_clim','-clim')
                    modnew = (abs_path[l:]).replace('_clim','-clim')
                    if modold==modnew:
                        abs_path = abs_path.replace( abs_path[l:], file['name'] )
                    else:
                        rmlog.warn( "Filename changed by drs_lib!! to "+abs_path[-len(file['name']):]+\
                                    ' from '+file['name'] )
                        commit_changes = False
                        break

                #add file to DB
                file_checksum = None
                file_checksum_type = None
                if 'checksum_value' not in file and dataset_catalog:
                    if catalog_chksums is None:
                        import catalog
                        catalog_chksums = {}
                        #get it from the catalog
                        try:
                            md_files = catalog.getDatasetMetadata(dataset_catalog)['files']
                        except:    # may fail because url doesn't work any more
                            rmlog.warn( "problem finding catalog checksums for "+file['name'] )
                            continue
                        for md_file in md_files:
                            if 'checksum' in md_file:
                                catalog_chksums[md_file['file_id'].split('.')[9] + '.nc'] = (md_file['checksum'],
                                    md_file['checksum_type'])
                    
                    if file['name'] in catalog_chksums:
                        file_checksum, file_checksum_type = catalog_chksums[file['name']]
                    else:
                        if allow_empty_md5:
                            file_checksum = 'DUMMY'
                        else:
                            rmlog.warn( "Missing Checksum for "+file['name'] )
                            commit_changes = False
                            break

                elif 'checksum_value' in file:
                    file_checksum = file['checksum_value'].lower()
                    file_checksum_type = file['checksum_type']
                elif allow_empty_md5:
                    file_checksum = 'DUMMY'
                else:
                    rmlog.warn( "Missing Checksum" )
                    commit_changes = False
                    break
                try:
                    if abs_path in files_added:  #jfp added duplicate check
                        rmlog.warn( "skipping second instance of %s"%(abs_path) )
                        continue
                    else:
                        files_added.append(abs_path)
                        rep_s.add(ReplicaFile(\
                            abs_path=abs_path, dataset_name=dataset_name, checksum=file_checksum,\
                            checksum_type=file_checksum_type, size=file['size'] ))
                except:
                    rmlog.warn( "Could not add file to db. Skipping dataset %s" % dataset_name )
                    
                    rmlog.warn( sys.exc_info()[:3] )
                    commit_changes = False
                    break
                
                #compute file access
                # jfp added check on DUMMY - it's not an error to be warned about, but the previous_files
                # dictionary is useless in that case.
                if file_checksum in previous_files and file_checksum.upper()!="DUMMY":
                    if not os.path.basename(abs_path)== os.path.basename(previous_files[file_checksum][0]):
                        #report this as we have a file that looks like it... but somewhere else
                        rmlog.warn( "Same checksum %s but different name!! new:%s vs old:%s" %\
                                  ( file_checksum, os.path.basename(abs_path),\
                                    os.path.basename(previous_files[file_checksum][0])) )

                    #ishould we check the filename at least? The probability that we get the same checksum
                    #in another file in the same dataset is < 2^-100 !!
                    # Anyway, check that the file is really here.
                    # An alternative method would be to check the status.
                    location = previous_files[file_checksum][0]
                    if os.path.getsize(location)==file['size']:
                        #this file is locally available, so we use the file: protocol to mark this
                        #no other check is required
                        rep_s.add(ReplicaAccess(abs_path=abs_path,url='file://' + location,type='local'))

                #this file is new
                #jfp: In rare occasions (e.g. ICHEC) endpoints can have two copies of the same endpoint!
                # So first check eliminate any duplicates (just a duplicate url is bad enough).
                # Fortunately, it will be a very short list...
                eps = []
                eprange = range(len(file['endpoints']))
                for i in eprange:
                    ep1 = file['endpoints'][i]
                    good = True
                    for j in eprange[i+1:]:
                        if file['endpoints'][i]['url'] == file['endpoints'][j]['url']:
                            good = False
                    if good:
                        eps.append( file['endpoints'][i] )
                #jfp was for ep in file['endpoints']:
                for ep in eps:
                    rep_s.add(ReplicaAccess(abs_path=abs_path,url=ep['url'],type=ep['type']))

            #we are done with this dataset!
            if commit_changes:
                #should we update file count? (this happens if a new version was found)
                if int(rep_ds.filecount) <= 0:
                    rep_ds.filecount = len(rep_ds.files)
                    rep_ds.size = sum([f.size for f in rep_ds.files])
                rep_ds.status = STATUS.INIT
                try:
                    rep_s.commit()
                except exc.IntegrityError as e:
                    rmlog.warn("Cannot process dataset %s due to IntegrityError %s" % (dataset,e) )
                    rep_s.rollback()
            else:
                rmlog.warn( "rolling back changes" )
                rep_s.rollback()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            rmlog.warn( "Can't process dataset %s an exception was found." % dataset )
            rmlog.warn( sys.exc_info()[:3] )
            rep_s.rollback()
            raise

    #flush all pending operations (there are breaks in there, this is a MUST.
    #rep_s.commit()

    #we are done! We have stored how ot get to the next version.
    #Files should be retrieved



def update_replica_db():
    """Update the metadata we have with that from the catalog. This is mainly intended to 
    be used for those cases where the catalog changes without issuing a new version (e.g.
    adding or correcting checksums)"""
    rep_s = getReplicaDB()
    import gateway

    for dataset in rep_s.query(ReplicaDataset).filter(ReplicaDataset.name.like(dataset_match)):
        rmlog.info( "checking %s (%s files)" % (dataset.name, len(dataset.files)) )
        #cache file results
        results = {}
        allfiles = gateway.main(('-g %s --parent %s -fo' % (dataset.gateway, dataset.name)).split(' '))
        if allfiles is None: continue
        #...moving on in this case means we don't delete a dataset not found at the server - the right
        # thing to do if it's a temporary server problem, but wrong if the dataset really has been
        # withdrawn.  Maybe I should change this.
        for file in allfiles:
            #names are guaranteed to be unique within the same dataset
            results[file['name']] = file

        if len(results) != len(dataset.files):
            rmlog.warn( "WARNING!! Different number of files found! %s vs %s",\
                        len(results),len(dataset.files) )
        changed_files = 0
        for file in dataset.files:
            file_name = os.path.basename(file.abs_path)
            if file_name in results:
                changed = False
                remote_file = results[file_name]
                #update as required
                # checksum, upper/lower case and 'DUMMY' fixed by jfp
                if 'checksum_value' not in remote_file.keys():
                    # if the gateway no longer returns a checksum, then we should
                    # forget about the checksum we have, it's probably wrong.
                    remote_file['checksum_value']='DUMMY'
                if file.checksum.lower() != remote_file['checksum_value'].lower():
                    if remote_file['checksum_value'].upper()=='DUMMY':
                        file.checksum = 'DUMMY'
                    else:
                        file.checksum = remote_file['checksum_value'].lower()
                    changed=True
                    if file.checksum.upper()!='DUMMY':
                        if file.status == STATUS.RETRIEVED:
                            #this don't hold anymore
                            file.status = STATUS.ERROR
                        if dataset.status == STATUS.RETRIEVED:
                            dataset.status = STATUS.ERROR
                        # But note that a published file (STATUS.PUBLISHED>STATUS.RETRIEVED)
                        # or even a file submitted for publication (STATUS.FINAL_DIR)
                        # will not be changed to STATUS.ERROR - once published, it's unchangeably
                        # "good" forever (but might be superseded by a new version).
                # size, added by jfp
                if file.size != int(remote_file['size']):
                    file.size = remote_file['size']
                    changed = True
                    if file.status == STATUS.RETRIEVED:
                        #this don't hold anymore
                        file.status = STATUS.ERROR
                    if dataset.status == STATUS.RETRIEVED:
                        dataset.status = STATUS.ERROR
                # url, added by jfp
                # When there are more than one access point, then we can't simply replace
                # the url.  The problem is, which file_access entry should be changed?
                # Probably a new FileAccess object should be built and tacked on to the
                # list; and an old one deleted if an invalid one can be identified.
                # This is worth doing, but not today. Even this code will arbitrarily choose
                # one suitable endpoint when there could be many....
                #if file_name.find('hfss')>-1: #jfp debugging
                #    print "jfp file_name=",file_name
                #    print "jfp file.access=",file.access
                #    print "jfp remote_file['endpoints']=",remote_file['endpoints']
                if len(file.access)==0:
                    for endpoint in remote_file['endpoints']:
                        file.access.append( ReplicaAccess(url=endpoint['url'],\
                                           abs_path=file.abs_path,type=endpoint['name']))
                        changed = True
                elif len(file.access)==1:
                    accesstype = file.access[0].type # most often, 'HTTPServer'
                    for endpoint in remote_file['endpoints']:
                        if endpoint['name']==accesstype:
                            file_url = file.access[0].url
                            new_url = endpoint['url']
                            if file_url!=new_url:
                                file.access[0].url = new_url
                                changed = True
                elif len(file.access)>1:
                    # not tested!
                    typecounts={}
                    for facc in file.access:
                        typecounts[facc.type] = 1+getattr(typecounts,facc.type,0)
                    for facc in file.access:
                        if typecounts[facc.type]>1:
                            continue  # >=2 urls of same type, don't know how to update them
                        for endpoint in remote_file['endpoints']:
                            if endpoint['name']==facc.type:
                                # if there be 2 endpoints of the same type, we'll lose one here!
                                file_url = facc.url
                                new_url = endpoint['url']
                                if file_url!=new_url:
                                    facc.url = new_url
                                    changed = True
                #...maybe a better algorithm is to always add every endpoint;
                #and then delete the old ones if they are the same type, same server


                    
                if changed==True:
                    changed_files += 1
            else:
                rmlog.warn( "File %s not found anymore!" % file.abs_path )
        
        if rep_s.dirty:
            rmlog.info( "Committing changes for %s  file(s)" % changed_files )
            rep_s.commit()
        

#################################################
##### -- creating download lists -- ##############
################################################


# Special download list from well known repos 
repos = { 'badc': 
    { 'id' : 1, 'file_mod' : lambda file : 'gsiftp://capuchin.badc.rl.ac.uk:2812/badc/cmip5/data/' + file.abs_path},
    'dkrz':
    { 'id' : 2, 'file_mod' : lambda file : 'gsiftp://cmip2.dkrz.de:2812/' + file.abs_path},
    #jfp was { 'id' : 2, 'file_mod' : lambda file : 'gsiftp://cmip2.dkrz.de:2812/wdcc' + file.abs_path},
    'pcmdi':
    { 'id' : 3, 'file_mod' : lambda file : 'gsiftp://pcmdi7.llnl.gov:2812/' + file.access[0].url[53:]},
          # NCC (Norwegian) repo is not set up yet, because I don't have a working url to base it on.
    'ncc':
    { 'id' : 4, 'file_mod' : lambda file : 'gsiftp://norstore-trd-bio1.hpc.ntnu.no:2812//' + file.access[0].url[35:]},
    'nci':
    { 'id' : 5, 'file_mod' : lambda file : 'gsiftp://esgnode1.nci.org.au:2812//' + file.access[0].url[35:]},
    'ncar':
#    { 'id' : 6, 'file_mod' : lambda file : 'gsiftp://vetsman.ucar.edu:2811//datazone/' + file.access[0].url[37:]},
# ...NCAR/ucar's cmip5_data may or may not be required, c.f. July 2012 notes and emails...
   { 'id' : 6, 'file_mod' : lambda file : 'gsiftp://vetsman.ucar.edu:2811//datazone/cmip5_data/' + file.access[0].url[37:]},
    'csiro':
    { 'id' : 7, 'file_mod' : lambda file : 'gsiftp://esgnode1.nci.org.au:2812//' + file.access[0].url[35:]},
    'cnrm':
    { 'id' : 8, 'file_mod' : lambda file : 'gsiftp://esg.cnrm-game-meteo.fr:2812//esg_dataroot1/' + file.abs_path},
# ... CNRM's CMIP5 is in upper case, unlike the cmip5 in abs_path.  I may have to fix the urls by hand.
    'ipsl':
    { 'id' : 9, 'file_mod' : lambda file : 'gsiftp://vesg.ipsl.fr:2811//esg_dataroot/' + file.abs_path}
}
    
server_priority = {
    'local':7, # local and llnl should be changed to 0 once we've restored lost data
    'llnl':7,
    'dkrz':2,
    'badc':2, 'ceda':2,  # BADC
    'ncar':2, 'ucar':2,
    'default':3,
    'gfdl':3,      # USA
    'nasa':3,      # USA
    'norstore':3, 'norstore-trd-bio1':3,     # Norway
    'dias':3, 'dias-esg-rp':3, 'u-tokyo':3,  # Japan
    'cnrm':3, 'cnrm-game-meteo':3,           # France
    'nci':5,       # Australia
    'bcc':5, 'bcccsm':5, # China
    'bnu':5,       # China
    'lasg':6,      # China
    'ichec':6, 'e-inis':6  # Ireland
    }

def url_priority(url):
    """Identifies the server name in a url, looks up its priority in the dict server_priority,
    and returns the priority.  Priorities range from 0 (highest) to 7 (lowest)."""
    hostport = url.split('/')[2]   # e.g. "pcmdi7.llnl.gov:2811"; port number only for gsiftp.
    host = hostport.split(':')[0]  # e.g. "bmbf-ipcc-ar5.dkrz.de",
    # "http://dias-esg-nd.tkl.iis.u-tokyo.ac.jp",  "cmip-dn1.badc.rl.ac.uk"
    hw = host.split('.')
    for w in hw:
        if w in server_priority.keys():
            return server_priority[w]
    print "url_priority found no matches for host=",host  # for debugging during development
    return server_priority['default']
    
def create_repo_list(datasets, repo=None, output=None, skip_existing=True, type='HTTPServer'):
    if not output: mapfile = sys.stdout
    else: mapfile = open(output, 'w')
    
    if type.startswith('repo'):
        if len(type) <= 5:
            repo='badc'
        else:
            repo=type[5:]
        if repo not in repos:
            rmlog.warn( "Uknown repo" )
            return 1
        #change name for entry
        repo = repos[repo]
    
    for dataset in datasets:
        #wenn die dataset complete local is, the
        # jfp was if skip_existing and ( dataset.status >= STATUS.RETRIEVED ):
        if skip_existing and ( dataset.status >= STATUS.VERIFYING ):
            continue

        for f in dataset.files:
            # jfp: was if skip_existing and ( f.status >= STATUS.RETRIEVED ): 
            if skip_existing and ( f.status >= STATUS.VERIFYING ): 
                continue
            url = None
            urls_bypri = {0:[],1:[],2:[],3:[],4:[],5:[],6:[],7:[]}
            if False:
                #jfp temporarily disable this due to lost published files at PCMDI...
                for a in f.access:  # added by jfp.  Always use the local copy if there is one.
                    if a.type == 'local':   # file:///cmip5/scratch/...
                        path = a.url[7:]
                        if os.path.isfile(path) and os.path.getsize(path)==f.size:
                            url = a.url
                            break
            if url is None:  #jfp
                if repo and 'file_mod' in repo:
                    try:
                        url = repo['file_mod'](f)
                    except:
                        print "cannot find download url for file=",f
                        continue
                else:
                    for a in f.access:
                        if a.type == type:
                            pri = url_priority(a.url)
                            urls_bypri[pri].append(a.url)
                    for pri in range(8):
                        if len(urls_bypri[pri])>0:
                            url = urls_bypri[pri][0]
                            break
            if url:
                if f.status==STATUS.ERROR:
                    flags=-1
                else:
                    flags=0
                mapfile.write('%s\t%s\t%s\t%s\t%s\t%s\n' %\
                                  (url, f.getDownloadLocation(),\
                                       f.size, f.checksum, f.checksum_type, flags))
            #DON'T assume all files in dataset have the same access. E.g. if using local only those that hasn't
            #change will be listed.


    if output: mapfile.close()
    else: mapfile.flush()


def create_guc(dataset):
    raise "Changed and not finished"

    mapfile = open(os.path.join(map_dir,dataset.name) + '.guc', 'w')
    for f in dataset.files:
        for a in f.access:
            if a.type == 'GridFTP':
                mapfile.write('%s file://%s\n' % (a.url, os.path.join(files_dir2, a.abs_path)))

    mapfile.close()

def create_download_lists(file=None, type=None):
    #define how this will work
    args={}
    if type.startswith('list') or type.startswith('repo'):
        if len(type[5:]) >0:
            args['type']=type[5:]
        process = create_repo_list
    else:
        raise Exception("don't recognize download type")

    
    session = getReplicaDB()
    datasets_new = session.query(ReplicaDataset).filter(ReplicaDataset.status==STATUS.INIT).filter(ReplicaDataset.name.like(dataset_match)).all()
    for dataset in datasets_new:
        #first time. Just mark it and commit it at this point.
        dataset.status = STATUS.DOWNLOADING
        rep_s.commit()

    #Display all datasets marked as being downloaded or failed. (the ones from "first time" are marked as downloading, so they appear here at this point)
    datasets_pending = session.query(ReplicaDataset).filter(sql.or_(ReplicaDataset.status==STATUS.DOWNLOADING, ReplicaDataset.status==STATUS.ERROR)).filter(ReplicaDataset.name.like(dataset_match)).all()

    process(datasets_pending, output=file, **args)

#    if file:
#        process(datasets_pending, output=file, **args)
#    else:
#        #dataset_new already done
#         for dataset in datasets_pending:
#            process([dataset], **args)

    

def finish_first_checksum_process( procs, ds_incomplete, published=False, wherefrom=0 ):
    """This is called from verify_datasets().  It will have set up several checksumming processes, each
    of which can be identified by a [file,process] pair. where the process is a checksum computation on
    the file.  The first argument of this function, procs, is a list of such pairs.
    Here we wait for the first process of the list to complete; and then handle the results of the
    process.  The other arguments are the ds_incomplete flag of verify_datasets() and an output marker
    to identify where this function was called from. """

    chk_file, proc = procs[0]
    if proc.wait() != 0:    #wait until first is free:
        #clean and finish dataset
        rmlog.warn( "%s checksum reported an error: '%s'" %(chk_file.abs_path, proc.communicate()[1]) )
        if not published:
            chk_file.status = STATUS.ERROR
            chk_file.dataset.status = STATUS.ERROR
            ds_incomplete = True
        return None,None,ds_incomplete   #something unexpected is wrong, we can't continue
    else:
        #md5 are always 32 char lang
        chksum = proc.communicate()[0][:32]
        if published:
            if chksum == chk_file.checksum:
                # Checksum is good; for a published file (or just in its final directory) we knew it
                # was good; this just means we're looking at the same version of the file.
                chk_file.status = max(chk_file.status,STATUS.FINAL_DIR)
            else:
                # We trust the published file, and we trust the catalog checksum.
                # They don't agree!  But we don't (yet) know the published file's dataset version.
                # It must be different, we shouldn't have checked this file in the first place;
                # do nothing.
                pass
        else:   # unpublished data
            if chksum != chk_file.checksum:
                #checksum mismatch!
                rmlog.warn( "%s checksum mismatch %s vs %s (%s)" % \
                            (chk_file.abs_path, chksum, chk_file.checksum,wherefrom) )
                chk_file.status = STATUS.ERROR
                chk_file.dataset.status = STATUS.ERROR
                file = verify_previous_downloads(chk_file,do_checksums=True)
                ds_incomplete = True
                #there's no harm in continuing with the rest
            else:
                #That file is ready!
                chk_file.status = STATUS.RETRIEVED

    #we now have 1 process slot free
    procs = procs[1:]
    return procs, chk_file, ds_incomplete

def swapfile( fullpath1, fullpath2 ):
    """swap files in two locations, known  not to have a subdirectory tmp4jfp"""
    (path1,file1) = os.path.split(fullpath1)
    (path2,file2) = os.path.split(fullpath2)
    tmpdir = os.mkdir( os.path.join(path1,"tmp4jfp") )
    shutil.move( fullpath1, tmpdir )
    shutil.move( fullpath2, path1 )
    shutil.move( os.path.join(tmpdir,file1), path2 )
    os.rmdir( tmpdir)

def verify_previous_downloads( file, do_checksums=True ):
    """If a file has a bad checksum or size, check for previous downloads in directories bad0,bad1,...
    Are they really bad?  If two more are bad, set the status to reflect that.  If one is good, use it."""
    fullpath = file.location
    #rmlog.debug("entered verify_previous_downloads with %s in %s",file,fullpath)
    if not os.path.isfile(fullpath):
        rmlog.warn("verify_previous_downloads called incorrectly with %s",fullpath)
        return file
    if fullpath and fullpath.find('/data/')>0:
        # This is a check for whether we got here on published data, which is supposed to be impossible.
        # At the moment, we put published data in /data/, unpublished in /scratch/
        rmlog.error("verify_previous_downloads called on published file %s",fullpath)
        return file
    (path,filename) = os.path.split(fullpath)
    havegood = False
    checksum_done = False
    nbad = 1
    if not os.path.isfile( os.path.join(path,'bad1',filename) ):
        # shortcut: if bad1/file doesn't exist, probably bad0/file is bad and won't change file.status
        # at the end.  Let another download come before recomputing checksums.
        return file
    for n in range(2):
        baddir = os.path.join( path, 'bad'+str(n) )  # bad0 or bad1
        badfile = os.path.join( baddir,filename )
        if not os.path.isfile(badfile):
            continue    # baddir doesn't have the file in question.
        if os.path.getsize(badfile)<0:  # incomplete file in bad0 or bad1, get rid of it
            os.remove(badfile)
            continue
        if os.path.getsize(badfile)>0:  # size is wrong, so file is bad
            nbad = nbad + 1
            continue
        # At this point baddir has the file in question, of the right length.
        if do_checksums==False or file.checksum.upper()=='DUMMY':
            swapfiles(badfile,fullpath)
            havegood = True
            break
        if not( file.checksum_type == 'md5' or file.checksum_type=='MD5' ):
            continue  # shouldn't get here
        # Checksum for badfile.  This occurs rarely enough so we don't need the speedup of forking of
        # another process.
        import pymd5
        csum = pymd5.md5(badfile)
        if csum == file.checksum:
            swapfiles(badfile,fullpath)
            havegood = True
            checksum_done = True
            break
        else:
            nbad = nbad + 1
            continue
    if havegood==True:
        #rmlog.debug("leaving verify_previous_downloads, good file at %s",fullpath)
        if checksum_done==True:
            file.status = STATUS.RETRIEVED
        else:
            file.status = STATUS.VERIFYING
    elif nbad>=3:
        #rmlog.debug("leaving verify_previous_downloads, multiple errors for %s",fullpath)
        file.status = STATUS.MULTIPLE_ERRORS
    else:
        #rmlog.debug("leaving verify_previous_downloads, no changes made to %s",fullpath)
        pass
    return file

def pcmdi_dbify( location ):
    """locaton is a full path to a file on gdo2.llnl.gov, beginning '/cmip5/data/cmip5/'.
    This function will return a corresponding (strictly-DRS) path from the database,
    If there is more than one file found, the most recent one would be returned (ideally we'd
    compare checksums)."""
    filename = os.path.basename(location)
    db = getReplicaDB()
    dbq = db.query(ReplicaFile).filter(ReplicaFile.abs_path.like('%/'+filename))
    dbfiles = dbq.all()
    if len(dbfiles)==1:
        return (dbfiles[0]).abs_path
    elif len(dbfiles)==0:
        return None
    else:  # multiple paths for the same filename.  The difference will be the version number.
        # if these paths are DRS paths, then v will be a list of version numbers
        versions = [ os.path.basename(os.path.dirname(os.path.dirname(f.abs_path))) for f in dbfiles ]
        vmax = versions[0]
        fmax = dbfiles[0]
        for l in range(len(dbfiles)):
            f = dbfiles[l]
            v = versions[l]
            if v>vmax:
                vmax = v
                fmax = f
        return f

def pcmdi_ify( location ):
    """location is a full path to a file on gdo2.llnl.gov, beginning, e.g.,'/cmip5/data/cmip5/'.
    The path will formed as a root plus a (strictly-DRS) path from the database.
    This function will return a similar path where the file may actually be found, or None
    if it cannot be found.
    If there are multiple versions, this function will return the most recent version (ideally we'd
    compare checksums).  Note that, in this process, the DRS date-based version number is dropped.
    There is no guarantee that the returned path will correspond to this DRS version number."""
    # example in comments: '/cmip5/data/cmip5/output1/IPSL/IPSL-CM5A-LR/historical/mon/atmos/Amon/
    # r1i1p1/v20110406/ts/ts_Amon_IPSL-CM5A-LR_historical_r1i1p1_185001-200512.nc'
    file = os.path.basename( location ) # e.g. ts_Amon_IPSL-CM5A-LR_historical_r1i1p1_185001-200512.nc
    dir1 = os.path.dirname( location )  # e.g. .../r1i1p1/v20110406/ts/
    var = os.path.basename( dir1 )      # e.g. ts
    if var[0] in '0123456789':    # not a variable name; probably a PCMDI version and location is good
        return location
    dir2 = os.path.dirname( dir1 )
    drs_vers = os.path.basename( dir2 ) # e.g. v20110406
    dir3 = os.path.dirname( dir2 )      # e.g. .../r1i1p1/
    ndir2 = os.path.join( dir3, var )   # e.g. .../r1i1p1/ts/
    if os.path.isdir(ndir2):
        ld2 = os.listdir( ndir2 )           # e.g. ['1','2']
    else:
        return None
    ld2.sort()
    pcmdi_vers = ld2[-1]
    ndir1 = os.path.join( ndir2, pcmdi_vers ) # e.g. .../r1i1p1/ts/2/
    nlocation = os.path.join( ndir1, file )   # e.g. .../r1i1p1/ts/2/ts_...-200512.nc
    return nlocation

    
def no_v(strng):
    """Deletes any initial 'v' from a string.  This is for convenience in comparing version
    numbers from different sources - version 'v20110912' is the same as version '20110912'.
    """
    if type(strng)==str and strng[0]=='v' and len(strng)>1:
        return strng[1:]
    else:
        return strng

#####################################################
##### -- Dataset verification --- ######################
###################################################
def verify_datasets(skip_hardlinks=False,do_checksums=True,pcmdipub=False):
    
    """Verify if the dataset is completely downloaded.
    do_checksums=True to verify each file with a checksum.
    If do_checksums==False, this function will just check whether the file exists and has
    the right length.  If do_checksums='Verified', any file which exists is guaranteed to
    have the right checksum as well, so the checksum will not be re-computed."""
    import subprocess
    import pubpath2version

    if not pcmdipub:
        dlroot0 = files_dir0     # on gdo2: /cmip5/scratch
        dlroot1 = files_dir1     # on gdo2: /css01-cmip5/scratch
        dlroot2 = files_dir2     # on gdo2: /css02-cmip5/scratch
        dlroot3 = "/No/such/path"
        pubroot0= archive_root0  # on gdo2: /cmip5/data
        pubroot1= archive_root1  # on gdo2: /css01-cmip5/data
        pubroot2= archive_root2  # on gdo2: /css02-cmip5/data
        pubroot3= archive_root3  # on gdo2: /css02-cmip5/cmip5/data
    else:
        # Note: the pcmdipub code is becoming obsolete as we now check published directores in all cases.
        dlroot0 = archive_root0  # On gdo2, for PCMDI-published data
        dlroot1 = archive_root1  # on gdo2: /css01-cmip5/data
        dlroot2 = archive_root2  # On gdo2, /css02-cmip5/data
        dlroot3 = archive_root3  # On gdo2, /css02-cmip5/cmip5/data
    db=getReplicaDB()
    #datasets = db.query(ReplicaDataset).filter(ReplicaDataset.status==STATUS.DOWNLOADING).all()
    if not pcmdipub:
        datasets = db.query(ReplicaDataset).filter( sql.or_(\
        ReplicaDataset.status==STATUS.ERROR,ReplicaDataset.status==STATUS.DOWNLOADING,\
        ReplicaDataset.status==STATUS.UNINIT,ReplicaDataset.status==STATUS.INIT,\
        ReplicaDataset.status==STATUS.RETRIEVED,  # worth checking iff it might have been published
        ReplicaDataset.status==STATUS.VERIFYING ) )
        # sometimes needed here, as very often status 20 isn't getting set when a download
        # list is created; but checking all those files slows things down:
        # ReplicaDataset.status==STATUS.UNINIT,ReplicaDataset.status==STATUS.INIT,\
    else:
        datasets = db.query(ReplicaDataset).filter( sql.or_(\
        ReplicaDataset.status==STATUS.ERROR,ReplicaDataset.status==STATUS.DOWNLOADING,\
        ReplicaDataset.status==STATUS.UNINIT,ReplicaDataset.status==STATUS.INIT,\
        ReplicaDataset.status==STATUS.VERIFYING ) )
    global dataset_match, dataset_matchnot
    query1 = datasets.filter(ReplicaDataset.name.like(dataset_match))
    if dataset_matchnot:
        matching_datasets = query1.filter(sqlalchemy.not_(\
            ReplicaDataset.name.like(dataset_matchnot))).all()
    else:
        matching_datasets = query1.all()

    #jfp was for dataset in datasets.filter(ReplicaDataset.name.like(dataset_match)).all():
    for dataset in matching_datasets:
        # If checksums were requested but can't be done, checksums_done will be switched to False
        checksums_done = do_checksums
        idurls = None

        #if dataset.name.find("cmip5.output2")>=0:
        #    # For now, we're not interested in output2 data
        #    rmlog.info( "Not processing %s" % dataset.name )
        #    continue
        #if dataset.name.find("cmip5.output")<0:
        #    # For now, we're not interested in non-CMIP5 data
        #    rmlog.info( "Not processing %s" % dataset.name )
        #    continue
        rmlog.info( "Processing %s" % dataset.name )
        max_proc = 16
        procs= []
        ds_incomplete = False
        if len(dataset.files)!=dataset.filecount:
            # The dataset's filecount differs from the number of known files in the dataset!
            # Something's wrong!  This shouldn't happen, but it does.  Fix it!
            print "correcting dataset filecount. len(dataset.files)=",len(dataset.files),\
                  " filecount=",dataset.filecount
            dataset.filecount = len(dataset.files)
        for file in dataset.files:
            #print "jfp file=",file.abs_path
            #if file.status>=STATUS.RETRIEVED or file.status==STATUS.MULTIPLE_ERRORS:
            # Even retrieved files are worth checking if they might have been published...
            if file.status>=STATUS.FINAL_DIR or file.status==STATUS.MULTIPLE_ERRORS:
                #skip files we have already previously checked sufficiently
                continue

            abs_path = file.abs_path

            #jfp new code:
            location = None
            candidate_pcmdi_paths = []
            candidate_drs_paths = [ (os.path.join(dlroot3,abs_path),False),
                                    (os.path.join(dlroot2,abs_path),False),
                                    (os.path.join(dlroot1,abs_path),False),
                                    (os.path.join(dlroot0,abs_path),False),
                                    (os.path.join(pubroot0,abs_path),True),
                                    (os.path.join(pubroot1,abs_path),True),
                                    (os.path.join(pubroot2,abs_path),True),
                                    (os.path.join(pubroot3,abs_path),True) ]
            for path,pub in candidate_drs_paths:
                #print "jfp trying path=",path,pub
                if path and os.path.isfile(path) and os.path.getsize(path)>0:
                    #print "jfp found path=",path,pub
                    location = path
                    location_published = pub
                    break
            if location==None:
                candidate_pcmdi_paths = [ (pcmdi_ify(path),pub) for (path,pub) in candidate_drs_paths ]
                for path,pub in candidate_pcmdi_paths:
                    if path and os.path.isfile(path) and os.path.getsize(path)>0:
                        #print "jfp found path=",path,pub
                        location = path
                        location_published = pub
                        break
            #jfp end of new code, but here's a bit for testing
#            if location!=location0:
#                if os.path.isfile(location0):
#                    print "jfp old algorithm got",location0,", new algorithm got",location
#                #location = location0

            #check existence
            if (not location) or (not os.path.isfile(location)) or\
               (os.path.isfile(location) and os.path.getsize(location)==0) :
                #file still not downloaded, or we're looking in the wrong place
                if file.status==STATUS.VERIFYING:  # error in database, I've seen it (jfp)
                    # print "jfp can't find file",location
                    file.status = STATUS.DOWNLOADING
                ds_incomplete = True
                #jfp too common to be interesting (I frequently run this on incomplete dasets)
                #jfp rmlog.warn( "%s file missing." % location )
                continue
            else:
                file.location = location
            
            if location_published and file.status==STATUS.ERROR:
                # temporary section to undo an earlier mistake.
                # This will do no harm, because we should never get here.
                file.status = STATUS.INIT

            #check size
            if os.path.getsize(location) < file.size:
                if file.status>=STATUS.FINAL_DIR or location_published:
                    # probably comparing with a different version's file; leave it alone
                    continue
                #file still not downloaded
                if os.path.getsize(location)>0:
                    # The file may have a download in progress, or an aborted download,
                    # but more likely the downloaded file's small size is because it's bad.
                    # First check whether it is a download in progress: status is "downloading"
                    # and modification time within three days of now.
                    if file.status == STATUS.DOWNLOADING and\
                           os.path.getmtime(file.location)+259200>time.time():
                        continue
                    rmlog.warn( "%s file incomplete, size %d should be %d." % \
                                (location,os.path.getsize(location),file.size) )
                    file.status = STATUS.ERROR
                    file.dataset.status = STATUS.ERROR
                    file = verify_previous_downloads(file,do_checksums)
                    ds_incomplete = True
                else:  # 0-length file.
                    #Almost always that means a download attempt failed to start.
                    file.status = STATUS.DOWNLOADING
                    ds_incomplete = True
                continue
            elif os.path.getsize(location) > file.size:
                if file.status>=STATUS.FINAL_DIR or location_published:
                    # probably comparing with a different version's file; leave it alone
                    continue
                #we got an error here!
                ds_incomplete = True
                file.status = STATUS.ERROR
                file.dataset.status = STATUS.ERROR
                rmlog.warn( "file %s exceeds the expected size" % location )
                file = verify_previous_downloads(file,do_checksums)
                continue

            #update timestamp            
            file.mtime = os.path.getmtime(location)

            #avoid checksumming if this file is linked and the previous is already checked
            # but only if it's worth the while. Set a threadshold (~100Mb).
            if skip_hardlinks and file.size > 1050000000:
                if os.stat(file.getDownloadLocation()).st_nlink > 1:
                    #we have a linked file we should check the previous file is the one we
                    #expect and is published (only one version per file)
                    #pass
                    # PCMDI doesn't use links, so this isn't relevant here...
                    file.status = STATUS.RETRIEVED
                    continue
 
            # If we're not doing checksums, we're almost finished.
            # If the file's checksum is 'DUMMY', we're not checksumming it!
            if do_checksums==False or file.checksum.upper()=='DUMMY':
                if file.status<STATUS.VERIFYING and not location_published:
                    # Note: for a "published file" location (e.g. .../data/...) we may not be able
                    # to verify that the location corresponds to correct version number, without
                    # doing checksums.  Thus upgrading the file's status is precluded if we can't
                    # do checksums.
                    file.status = STATUS.VERIFYING
                checksums_done = False
                continue
            elif do_checksums=="Verified":
                if file.status<STATUS.RETRIVED:
                    file.status = STATUS.RETRIEVED
                continue
            if file.status>=STATUS.RETRIEVED and location_published!=True:
                # file is already verified, not published
                continue
            if location_published and file.size>0:
                # Some version has already been published; trust it if it's the same version.
                if idurls == None:
                    idurls = pubpath2version.get_idurls_from_dataset( dataset.name )
                pubversion = pubpath2version.pubpath2version(location,idurls)
                if no_v(pubversion)==no_v(str(dataset.version)):
                    # print "published file",location," version",pubversion," same as dataset"
                    file.status = STATUS.FINAL_DIR
                    continue
                else:
                    # A file of this name is in a directory for published data, but the version
                    # is different. Thus this file isn't at the location, let alone published there
                    # (unless the version is None which just means the version remains unknown).

                    # If we get here, it may signal a need to do a better job
                    # of choosing a path which has the same file of the same version.
                    print "published file",location," version",pubversion,\
                          " differs from dataset version",dataset.version
                    if pubversion!=None:
                        location_published = False
                        continue
                    # If pubversion==None, keep on going; we may be able to determine the
                    # version from checksums.

            #check checksum
            #print "jfp location=",location
            if file.checksum_type == 'md5' or file.checksum_type=='MD5':
                if len(procs) >= max_proc:
                    #we are not refering to the current file, but some other
                    procs, chk_file, ds_incomplete = finish_first_checksum_process(\
                        procs, ds_incomplete, location_published, 1 )
                    if procs==None: break
                # if here we may start a new process
                procs.append((file, subprocess.Popen(['pymd5',location],shell=False, stdout=subprocess.PIPE)))
            elif file.checksum_type is not None:
                rmlog.warn( "checksum of type %s not implemented yet!" % file.checksum_type )
            else: 
                #we have no info, just let it for later... we might want to trigger something here...
                rmlog.info( "No checksum infor for %s. Skipping" % location )

        #process last checksums!
        while procs:
            #we are not refering to the current file, but some other
            procs, chk_file, ds_incomplete = finish_first_checksum_process(\
                procs, ds_incomplete, location_published, 2 )
            if procs==None: break

        #All files processed check status and update if necesary.
        if ds_incomplete:
            # cleanup dataset status - sometimes the status is somehow DOWNLOADING
            # when there has been no attempte to download any file - added by jfp
            if dataset.status > STATUS.DOWNLOADING:
                # Even a published dataset could become "downloading" if new files were added to it.
                dataset.status = STATUS.DOWNLOADING
            if dataset.status == STATUS.DOWNLOADING:
                #jfp, Mar2013 I don't know why this is here; it seems pointless...
                dataset.status = STATUS.INIT
                for file in dataset.files:
                    if file.status!=STATUS.INIT and file.status!=STATUS.UNINIT:
                        dataset.status = STATUS.DOWNLOADING
                        break
            rmlog.info( "incomplete" )
        else:
            #all verified! We are ready!
            rmlog.info( "ok!" )
            if dataset.filecount==0:
                # happens often, but it's a suspicious situtation.
                # Don't mark it as RETRIEVED because then wel'll never look at it again.
                dataset.status = STATUS.VERIFYING
            elif checksums_done==True or do_checksums=="Verified":
                if dataset.status<STATUS.RETRIEVED:
                    dataset.status = STATUS.RETRIEVED
            else:
                if dataset.status<STATUS.RETRIEVED:
                    dataset.status = STATUS.VERIFYING

        if dataset.status == STATUS.RETRIEVED:
            unpublished_files = [ file for file in dataset.files if file.status<STATUS.FINAL_DIR ]
            if len(unpublished_files)==0:
                dataset.status = STATUS.FINAL_DIR

        #update DB
        db.commit()


#####################################################
##### -- Existing Dataset --- ######################
###################################################
#  Get download information on an existing dataset.
#  It may have been downloaded by some other method.
#  Assume that checksums have already been done.
#  The example I have in mind is the INM data in
#  gdo2.ucllnl.org:/cmip5/data/cmip5/output1/INM/ .
#  This function just calls verify_datasets with different arguments.
def discover_datasets( skip_hardlinks=False, do_checksums=False ):
    
    """Discover files which have already been downloaded and verified."""
    # I decided that due to the difficulty in definitively identifying a file,
    # checksums should used; although they should come from the THREDDS table if available.
    #All files are in /cmip5/data and are assumed to have been
    #verified unless do_checksums==True. """
    print ">>>> don't use discover_datasets until it's correctly checking version numbers <<<<"
    return

    if do_checksums!=True:
      do_checksums="Verified"
    verify_datasets( skip_hardlinks=skip_hardlinks, do_checksums=do_checksums,\
                     pcmdipub=True )

if __name__ == '__main__':
    #configure logging
    logging.basicConfig(level=logging.DEBUG)

    import sys
    result=main(None)
    if result != 0: print usage
    sys.exit(result)

