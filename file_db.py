#!/usr/local/cdat/bin/python
"""This modules provides means for storing a directory structure starting
from a given directory in a DB and to compare it back to a given directory 
structure.
The metadata of the files stored are:
- relative path
- file name
- size
- modification time
- checksum
"""

from sqlalchemy import Column, Integer, String, Float, Boolean, or_
import logging 
log = logging.getLogger('file_db')
from utils_db import DAO, DB, Base
import os, hashlib

_ALG_BLOCK_SIZE=2**14

import threading, Queue, subprocess
class Checksum(threading.Thread):
    """Python Thread for managing Checksum process"""
    _in = Queue.Queue()
    _out = Queue.Queue()
    _threads = []

    _kill_token = object()

    running = True
    _check_kwargs = {}

    def __init__(self):
        threading.Thread.__init__(self)
        self.daemon = False
       
    def run(self):
        while Checksum.running:
            file = self._in.get()
            if file is self._kill_token:
                self._in.task_done() 
                break


            try:
                result = self.process(file)
                
                self._out.put((file, result))
            except Exception as e:
                log.error('Error: %s', e)
            finally:
                self._in.task_done()

    def process(self, file):
        p = subprocess.Popen(['md5sum', file.getLocalFile()], stdout=subprocess.PIPE)
        retcode = p.wait()
        stdout, stderr = p.communicate()
        checksum = stdout.split(' ')[0]
        
        return file.check(checksum_value=checksum, **self._check_kwargs)

    @staticmethod
    def add(file):
        Checksum._in.put(file)

    @staticmethod
    def setCheckArgs(**kwargs):
        Checksum._check_kwargs = kwargs

    @staticmethod
    def addThread(amount=1):
        while amount > 0:
            log.debug('added new thread')
            t = Checksum()
            Checksum._threads.append(t)
            t.start()
            amount -= 1

    @staticmethod
    def waitUntilFinished():
        #wait for queue
        Checksum._in.join()

    @staticmethod
    def stop():
        #nobody else should continue
        Checksum.running = False

        #waiting threads should wake up
        for i in range(len(Checksum._threads)):
            Checksum.add(Checksum._kill_token)
            

class DirectoryDAO(Base, DAO):
    __tablename__ = 'dir_cache'
    path = Column(String, nullable=False, primary_key=True)
    mtime =  Column(Float)

    @staticmethod
    def _load(db):
        db.open()
        q = db._session.query(DirectoryDAO)
        dirs = {}

        for dir in q: dirs[dir.path] = dir

        return dirs
            

class FileDAO(Base, DAO):
    """DAO for accessing and manipulating DB entries."""
    __tablename__ = 'local_files'

    path = Column(String, nullable=False, primary_key=True)
    name = Column(String, nullable=False, primary_key=True)
    size = Column(Integer)
    mtime = Column(Float)
    checked = Column(Boolean)

    checksum_type = Column(String(5), default='md5')
    checksum_value = Column(String(32))
    
    #Values not stored in the DB
    file = None #location of the file (root/path/name or other if given separately)
    drs = None  #drs Object representing this file (if any)

    #access FileDAO.root for rooting all files
    root = ''

    def __init__(self, **kwargs):
        if 'root' in kwargs: self.root = kwargs['root']
        if 'file' in kwargs:

            if len(self.root) > 0  and kwargs['file'].startswith(self.root):
                #file already rooted! make it relative
                self.file = kwargs['file']
                kwargs['file'] = self.file[len(self.root)+1:]
            else: 
                self.file = os.path.join(self.root, kwargs['file'])

            if 'name' not in kwargs: kwargs['name'] = os.path.basename(kwargs['file'])
            if 'path' not in kwargs: kwargs['path'] = os.path.dirname(kwargs['file'])
            #avoid passing this to base, as it will overwrite the one we've already stored
            del kwargs['file']

        Base.__init__(self, **kwargs)
        
    def getLocalFile(self):
        if not self.file:
            self.file = os.path.join(self.root, self.path, self.name)
        return self.file

    def updateFromLocalFile(self, overwrite=True, recheck_checksum=False, skip_checksum=True, checksum_value=None, checksum_type=None):
        file = self.getLocalFile()

        if not self.size or overwrite: self.size = os.path.getsize(file)
        if not self.mtime or overwrite: self.mtime = os.path.getmtime(file)
        if not skip_checksum and (not self.checksum_value or recheck_checksum):
            if not checksum_type: checksum_type = 'md5'
            if not checksum_value: checksum_value = FileDAO.__getChecksum(file, checksum_type)

            self.checksum_value = checksum_value
            self.checksum_type = checksum_type
                

    def check(self, update=False, skip_checksum=False, checksum_value=None):
        """Check db file against local one.
            update: if set, updates current db entry with data from local file
            skip_checksum: if set skips checksum calculation (which is quite expensive)
            checksum_value: value against which the checksum will be checked (if not given, it will be read from DB)."""
        result = True
        file = self.getLocalFile()
        if not os.path.isfile(file): return False
        
        size = os.path.getsize(file)
        mtime = os.path.getmtime(file)

        if size != self.size:
            result = False
            if update: self.size = size

        if mtime != self.mtime:
            #this is normal!
            #result = False
            if update: self.mtime = mtime

        if not skip_checksum and result:
            #if we failed prior to this, we won't check it.
            checksum = checksum_value
            
            if not checksum: checksum = FileDAO.__getChecksum(file, self.checksum_type)
    
            if checksum != self.checksum_value:
                result = False
                if update: self.checksum_value = checksum
        else:
            log.debug('Checksum not controlled for %s', file)
                
                
        return result

    def getCMIP3dataset(self):
        return '.'.join(self.path.split(os.sep)[:5])
    def getCMIP5DrsId(self):
        if not self.drs:
            import drs
            self.drs = drs.DRS(path=self.path, filename=self.name)
        return self.drs.getId()
    def getCMIP5MapEntry(self):
        if not self.drs:
            import drs
            self.drs = drs.DRS(path=self.path, filename=self.name)
        atts = [self.drs.getId(), self.getLocalFile(), str(self.size), 'mod_time=%r' % self.mtime ]
        if self.checksum_value:
            atts.append('checksum=' + self.checksum_value)
            atts.append('checksum_type=' + self.checksum_type.upper())

        return ' | '.join(atts)
    

    @staticmethod
    def __getChecksum(file, algorithm):
        alg = hashlib.__dict__[algorithm]()
        f = open(file, 'r')
        block_size=_ALG_BLOCK_SIZE
        data = f.read(block_size)
        while data:
            alg.update(data)
            data = f.read(block_size)
        f.close()

        return alg.hexdigest()

class FileDB(DB):
    """Representsthe local files DB"""

    def __init__(self, db_url, root):
        DB.__init__(self, db_url)
        #trim last slash if present
        if root[-1:] == '/': root = root[:-1]

        self.root = root
        FileDAO.root = root

    def addAll(self, files):

        self.open()

        results = self._session.query(FileDAO).filter(FileDAO.name.in_([ f.name for f in files])).all()

        new = []
        existing = []

        in_session = self._add_all(files, db_objects=results, new=new, existing=existing )
        return new + existing

    def get(self, **filter):
        self.open()
        return self._session.query(FileDAO).filter_by(**filter)

    def getQuery(self):
        self.open()
        return self._session.query(FileDAO)



    def compareWithLocal(self, batch_size=100, only_new=False, **kwargs):
        """Compared DB with the current directory and report differences.
            only_new: compare only files which weren't checked before (flag in DB mark this)
            batch_size: number of files to be compared before updating DB.
            update: Only checked, result will not be stored in the DB.
            **kwargs: passed to the Checksum Thread manager for configuration."""
        self.open()

            

        ok_num = 0
        failed_num = 0
    
        Checksum.setCheckArgs(**kwargs)
        Checksum.addThread(amount=10)    

        workToDo = True

        while workToDo:
            ok = []
            failed = []
                
            if only_new: query = self._session.query(FileDAO).filter(FileDAO.checked==None)
            else: query = self._session.query(FileDAO).filter(or_(FileDAO.checked==False, FileDAO.checked==None))
            files = query[:batch_size]

            if not files and ok_num + failed_num == 0:
                #first time run and nothing to be done
                log.info("Nothing to do. Exiting")
                break #return True

            #parse this lasst batch
            if len(files) < batch_size: workToDo = False
        
            for file in files:
                Checksum.add(file)

            q = Checksum._out
            while not Checksum._in.empty():
                file, result = Checksum._out.get()
                if result: ok.append(file)
                else: failed.append(file)

            log.debug('Batch almost done...')
            
            Checksum.waitUntilFinished()

            log.debug('Batch Finished, getting the last results.')
            
            while not Checksum._out.empty():
                file, result = Checksum._out.get()
                if result: ok.append(file)
                else: failed.append(file)
            
            
            log.debug('done ok:%s, failed:%s',len(ok), len(failed))

            
            for f in ok: f.checked = True
            for f in failed:
                if only_new: f.checked = True   #if these are new, we could have compared it to anything, so it's asumed to be ok
                else: f.checked = False

            #update session
            self._session.commit()


            ok_num += len(ok)
            failed_num += len(failed)

        Checksum.waitUntilFinished()

        log.debug('Finished, getting the last ones')
        
        while not Checksum._out.empty():
            file, result = Checksum._out.get()
            if result: ok.append(file)
            else: failed.append(file)
        
        for f in ok: f.checked = True
        for f in failed: f.checked = False

        #update session
        self._session.commit()

        ok_num += len(ok)
        failed_num += len(failed)
        
        log.debug('TOTAL ok:%s, failed:%s',ok_num, failed_num)
        Checksum.stop()
        
        return failed_num == 0

    def updateFromDir(self, start_from=None, dryrun=False, cache=False):
        """Update DB with current files in a given directory structure. No md5 will be performed.
            start_from: start comparison from this subdir (might be absolute or relative to self.root)
                if not given, starts from self.root
            dryrun: just report differences, don't update DB."""
        ## TODO: handle removing for directories
        ## this caching procedure works only with new files, if they are modifed it does not.

        self.open()
        query = self._session.query(FileDAO)

        #define where to start looking for files (might differ from root!)
        if start_from:
            #check if it's absolute (includes root). if not make it.
            if not start_from.startswith(self.root):
                #also delete last slash ot make it relative (thus +1)
                start_from = os.path.join(self.root, start_from)              
        else: start_from = self.root
           
        results = DirectoryDAO._load(self) 


        

        missing = []
        partially_missing = []
        unchanged = []
        offset = len(self.root) + 1
        
        dirs = [start_from]
        while dirs:
            dir = dirs.pop()
            path = dir[offset:]
            if path not in results:
                #new
                missing.append(DirectoryDAO(path=path, mtime=os.path.getmtime(dir)))
                continue    #nothing more to be done
            elif os.path.getmtime(dir) != results[path].mtime:
                #something changed
                results[path].mtime = os.path.getmtime(dir)
                partially_missing.append(results[path])
            else:
                #nothing changed since last call
                unchanged = results[path]

            #get subdirs
            for entry in os.listdir(dir):
                abs = os.path.join(dir, entry)
                if os.path.isdir(abs): dirs.append(abs)


        log.info('Found %s new directories for. A total of %s may have changed.', len(missing), len(partially_missing))

                    
        if not dryrun:
            new_files = []
            counter = 0
            #theses are plain files missing
            for dir in partially_missing:
                for file in os.listdir(os.path.join(self.root, dir.path)):
                    f = FileDAO(file=os.path.join(dir.path,file))
                    f.updateFromLocalFile()
                    new_files.append(f)

                    counter += 1
                    if counter > 100: 
                        print '.',
                        counter = 0

            #these are whole directory trees
            for dir in missing:
                for base, dirs, files in os.walk(os.path.join(self.root, dir.path)):
                    for dir in dirs:
                        #add all dirs to the cache
                        self._session.add(DirectoryDAO(path=os.path.join(base[offset:],dir), mtime=os.path.getmtime(os.path.join(base, dir))))

                    for file in files:
                        #files might be empty
                        f = FileDAO(file=os.path.join(base,file))
                        f.updateFromLocalFile()
                        log.debug('Updating: %s', f)
                        new_files.append(f)

                        counter += 1
                        if counter > 100: 
                            print ',',
                            counter = 0
            #update cache
            self._session.add_all(missing)
            self.addAll(new_files)

        else:
            #produce a report
            print "New Directories: %s\nModified Directories: %s" % (len(missing), len(partially_missing))
            if log.isEnabledFor(logging.INFO) and ( missing or partially_missing):
                log.info('New Directories:\n' + '\n'.join([ d.path for d in missing]))
                log.info('Changed Directories:\n' + '\n'.join([ d.path for d in partially_missing]))
            
        return partially_missing + missing 

        
    def getFromCMIP3Dataset(self, drs):
        self.open()
        return self._session.query(FileDAO).filter(FileDAO.path.like(drs.replace('.','/') + '%')).all()
        
    def getMapFile(self, drs=None):
        content = []
        if drs:
            for file in self.getFromCMIP3Dataset(drs):
                content.append('|'.join([drs, file.getLocalFile(), str(file.size), \
                    'mod_time=' + str(file.mtime), 'checksum=' + file.checksum_value, \
                    'checksum_type=' + file.checksum_type]))
        return content
    def exportCMIP5Mapfile(self, target, query=None, onePerDataset=False):
        #if no subset then all
        if not query: query = self.getQuery()
        if not target: raise Except('Target required')
        
        if onePerDataset and not os.path.isdir(target): raise Except('Target must be an existing directory when generating multiple files')
        if onePerDataset: files = {}
        else: fp = open(target, 'w')
               
        for r in query:
            #if one per dataset is set, find proper file!
            if onePerDataset:
                if r.getCMIP5DrsId() not in files: files[r.getCMIP5DrsId()] = open(os.path.join(target,r.getCMIP5DrsId() + '.map'), 'w')
                fp = files[r.getCMIP5DrsId()]

            #write normally to this file
            fp.write(r.getCMIP5MapEntry())
            fp.write('\n')
            
        if onePerDataset: 
            for fp in files.values(): 
                fp.close()
        else:   
            fp.close()
                

def parseMapFile(file, trim_root=None):
    """Extract data from an esgpublisher map file"""
    fo = open(file, 'r')
    
    files = []
    for line in fo:
        elements  = line.split('|')

        drs = elements[0].strip()
        
        fullPath = elements[1].strip()
        if trim_root: fullPath = fullPath[trim_root:]
        size = int(elements[2].strip())
        mtime = None
        checksum = None
        checksum_type = None

        for elem in elements[3:]:
            type,value = elem.strip().split('=')
            if type == 'mod_time': mtime = float(value)
            elif type == 'checksum_type': checksum_type = value.lower()
            elif type == 'checksum' : checksum = value
            else: log.warn('Unknown modifier "%s"', elem)

        files.append(FileDAO(file=fullPath, size=size, mtime=mtime, checksum_value=checksum, checksum_type=checksum_type))

    return files
        

    
        
def __testData():
    return [FileDAO(file='cmip3_drs/output/BCCR/BCM2/1pctto2x/day/atmos/hfls/r1/v1/hfls_A2_BCM2_1pctto2x_r1_-10-0.nc')]

def __parseCmip3MapFiles(db):
    import sys
    args = sys.argv[1:]
    if args:
        data = []
        for mapfile in args: 
            data.extend(parseMapFile(mapfile, trim_root=len('/badc/')))
    else:
        data = __testData()
    if data: print data[0]
    else: log.error('No data found for %s', args)
    data = db.addAll(data)

def __createCMIP3MapFiles(db, target_dir='.'):
    if not os.path.isdir(target_dir):
        raise Exception('Can\'t find/write to {0}'.format(target_dir))

    import sys
    args = sys.argv[1:]
    datasets = []
    if args:
        if len(args) == 1:
            if os.path.isfile(args[0]):
                f = open(args[0], 'r')
                for ds in f: datasets.append(ds[:-1])
                f.close()
                    
        if not datasets: datasets.extend(args)

        
        for ds in datasets:
            log.info('Generating %s', ds)
            mapfile = db.getMapFile(ds)
            f = open(os.path.join(target_dir,ds + '.map'), 'w')
            for line in mapfile:
                f.write(line + '\n')
            f.close()




usage="""file_db.py [opt] root
Manages a directory structure hooked at <root>.
Opt:
    -D <db_name>    : Define name for the current DB (default: files.db)
    -u              : Update DB with current files in the root.
    --source <dir>  : When updating start from this directory instead of root
                      (can be relative to root or absolute)
    --report        : Report differences between DB and file system
                      (no checksumiming)

    -q          : quiet mode
    -v          : verbose mode
    -d          : debug mode

"""

def main(argv=None):
    if argv is None: argv = sys.argv[1:]
 
    import getopt
    try:
        args, lastargs = getopt.getopt(argv, "D:ucdvqh", ['help', 'source=', 'report', 'all'])
    except getopt.error:
        print sys.exc_info()[:3]
        return 1

    #init values
    db_name = 'files.db'
    root = '.'
    source = None
    update = check = all = report = False
    #parse arguments
    for flag, arg in args:
        if flag=='-D':              db_name = arg
        elif flag=='--source':      source = arg
        elif flag=='--report':      report = True
        elif flag=='-u':            update = True
        elif flag=='-c':            check = True

        elif flag=='--all':         all = True

        elif flag=='-d':            log.setLevel(logging.DEBUG)
        elif flag=='-v':            log.setLevel(logging.INFO)
        elif flag=='-q':            log.setLevel(logging.NONE)
        elif flag=='-h' or flag=='--help':
            print usage
            return 0

    if not lastargs:
        return 1
    root = lastargs[0]
    
    #check args:
    if ( update or check ) and  not root:
        raise Excpetion('You have to define a root (-r)')

    db = FileDB('sqlite:///%s' % db_name, root)    

    if update:
        db.updateFromDir(start_from=source)

    if report:
        db.updateFromDir(start_from=source, dryrun=True)

    if check:
        db.compareWithLocal(only_new=not all)
        
    

                
def nccReplication():
    root='/gpfs_750/transfer/replication_cmip5/ncc'
    db = FileDB('sqlite:///ncc.local2.db', root)
    db.updateFromDir(start_from="CMIP5_backup")
    #print db.compareWithLocal(update=True, only_new=True)

def ipslReplication():
    root='/gpfs_750/transfer/replication_cmip5/cmip5/data'
    db = FileDB('sqlite:///ipsl.local.db', root)

    #only first time
    #db.updateFromDir(start_from="CMIP5/output1/IPSL")
    print db.compareWithLocal(update=True, only_new=True)
    
if __name__ == '__main__':
    import sys
    #configure logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    log.setLevel(logging.WARN)

    ret_val = main()
    if isinstance(ret_val, int) and ret_val != 0: print usage
    sys.exit(ret_val)

    #root='/gpfs_750/transfer/replication_cmip3/files'
#    root= root + '/cmip3_drs/output/CNRM/CM3/1pctto4x/mon/ocean/vo/r1/v1'
    #db = FileDB('sqlite:///files.db', root)
    #__parseCmip3MapFiles(db)

    #print db.compareWithLocal(update=True, only_new=True)
    #print db.updateFromDir()
#    result = db.getFromCMIP3Dataset('cmip3_drs.output.BCCR.BCM2.1pctto2x.day.atmos')
#    print len(result), result[0].getCMIP3dataset()
    #db.getMapFile('cmip3_drs.output.BCCR.BCM2.1pctto2x.day.atmos')
    #__createCMIP3MapFiles(db, '/gpfs_750/transfer/replication_cmip3/publication/mapfiles')

