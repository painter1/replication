#!/usr/apps/esg/cdat6.0a/bin/python

#!/usr/local/cdat/bin/python
"""Handle Download of files in a multithreaded manneri.
The current handler used for actually grabbing the file is 
started in a separate process to improve the robustness of this method.
For Example, you could shut down a stalled download stream without affecting
the current Python VM"""

import time
from threading import Thread, Lock
from Queue import PriorityQueue
import sys, os, errno, re, subprocess
import utils, protocol_handler, shutil
import logging
log = logging.getLogger('download')
log.addHandler( logging.FileHandler('download.log') )

class DownloadThread(Thread):
    """Manages the download of one file."""
    #mark to stop thread (i.e. if found in queue)
    STOP = (None, None)

    #Define the message priority
    PRIO_HIGH = 10
    PRIO_NORMAL = 100

    #Define some callbacks values
    EXIT_DONE = 0
    EXIT_STOP = 1
    EXIT_IO_ERR = -1
    EXIT_DOWNLOAD_ERR = -2
    
    protocol_pat = re.compile("^([^:]+):")
    
    
    def __init__(self, name, priorityQueue, callback=None):
        Thread.__init__(self)
        self.queue = priorityQueue
        self.name = name
        self.running = True
        self.active_file = None
        self.downloaded = None
        self.benchmarks = None

        self.daemon = True
        self._handler = None
        
        #this is called when something is performed
        self.callback = callback
        
    def run(self):
        while self.running:
            #We are using a priority queue the first value is the proiority
            # and thus not really interesting
            entry = self.queue.get()[1]
            if entry == self.__class__.STOP:
                self.queue.task_done()
                break
            else:
                try:
                    (val, data_dict) = self._check_download(*entry)
                    if self.callback: self.callback(val, **data_dict)
                except: # except clause added by jfp (try & finally were already there)
                    print "exception caught in run() 2"
                    print sys.exc_info()[:3]
                    raise
                finally:
                    self.queue.task_done()

        if self.callback: self.callback(self.__class__.EXIT_STOP, thread=self)

    def _check_download(self, full_path, url, size, flags):
        """Assures everything some requirements are met and then starts download.
            Some requirements are: check for existing directory structure, etc)"""
        print "jfp full_path=",full_path,"< url=",url,"< size=",size," flags=",flags
        end = size  #assume one part only
        start = 0
        # log.debug("in _check_download, args= %s,%s,%s,%s",full_path,url,size,flags)
        (path, file) = os.path.split(full_path)
        if not os.path.isfile(full_path):
            #file doesn't exists
            try:
                os.makedirs(path)
            except OSError as exc:
                if exc.errno == errno.EEXIST:
                    pass
                else:
                    #we can't do much here. We won't let the Thread die
                    log.warn('cannot create %s',path)  # added by jfp
                    return (self.__class__.EXIT_IO_ERR, {'exception':exc, 'url':url, 'size':0} )
            #download complete file
        elif int(flags)==-1:
            # File has already been downloaded but is bad (bad length or bad checksum).
            # Move the old one out of the way and proceed.
            try:
                newdir = self.move_bad_file( full_path)
            except EnvironmentError as exc:
                log.warn('cannot move bad file %s due to %s',full_path,exc)
                return (self.__class__.EXIT_DOWNLOAD_ERR, \
                        {'file':full_path, 'url':url, 'start':size, 'end':size,\
                         'exitcode':-1, 'size':0} )
            if newdir==None:
                log.warn("file is bad and has already been downloaded enough times: %s",\
                         full_path)
                return( self.__class__.EXIT_DONE,\
                        {'file':full_path, 'already_there':True, 'url':url, 'size':0})
        else:
            #continue download from what's left
            start = os.path.getsize(full_path)
            if start == size:
                #nothing to do! (start > end might imply file changed...)
                log.debug('file already present: %s',file)
                return (self.__class__.EXIT_DONE,\
                        {'file':full_path, 'already_there':True, 'url':url, 'size':0})
            if start > end:
                #File changed and is smaller.
                log.warn("Won't download from %s, file %s exists and is too big -"%(url,full_path))
                #...jfp added
                return (self.__class__.EXIT_DOWNLOAD_ERR,
                        {'file':full_path, 'url':url, 'start':start, 'end':end, 'size':0} )
        #define protocol for file gathering
        mo = DownloadThread.protocol_pat.match(url)
        if mo:
            protocol = mo.group(1)
        else:
            protocol = 'file'

        return self._download(full_path, url, protocol, start, end)

    def move_bad_file(self, full_path):
        # The download list has a file which exists but is bad.
        # Move it out of the way (without deleting it).
        # Delete the file if it's empty.
        (path, file) = os.path.split(full_path)
        # log.debug("in move_bad_file,full_path=%s",full_path)
        if not os.path.isfile(full_path):  # probably already moved, but new one never downloaded
            # log.debug("in move_bad_file, not a file: full_path=%s",full_path)
            return path
        if os.path.isfile(full_path) and os.path.getsize(full_path)==0:
            # log.debug("in move_bad_file, will delete: full_path=%s",full_path)
            os.remove(full_path)
            return path
        for n in range(2):
            # log.debug("in move_bad_file,n=%s",n)
            newdir = os.path.join( path, 'bad'+str(n) )
            if os.path.isdir(newdir) and os.path.isfile(os.path.join(newdir,file)):
                continue
            # log.debug("in move_bad_file, full_path=%s will make dir %s",full_path,newdir)
            if not os.path.isdir(newdir):
                os.mkdir(newdir)
            shutil.move( full_path, newdir )
            # log.debug("in move_bad_file, moved full_path=%s to newdir=%s",full_path,newdir)
            break
        if os.path.isfile(full_path):
            # The file wasn't moved, which is because too many downloads have already been tried.
            # log.debug("in move_bad_file, nothing done. full_path=%s",full_path)
            return None
        else:
            # log.debug("in move_bad_file, returning newdir=%s",newdir)
            return newdir

    def _download(self, full_path, url, protocol, start, end):
        log.debug("Downloading %s [%s-%s] (%s)",full_path, start, end, protocol)
        #start download
        self.active_file = full_path
        self.benchmarks = utils.Struct(time=time.time(), bytes=start, avg= 0)
        self.start_byte = start
        
        #downloading

        #this should get the file or fail if it's impossible. Never block..
        self._handler = protocol_handler.getHandler(url)

        try: # try/except added by jfp
            ret = self._handler.getFile(full_path, start=start, end=end)
        except:
            print "_download caught exception"
            raise
        
        #download done!
        self._handler = None
        self.active_file = None

        if ret == 0:
            return (self.__class__.EXIT_DONE, {'file':full_path, 'url':url, 'size':end-start})
        else:
            return (self.__class__.EXIT_DOWNLOAD_ERR, {'file':full_path, 'url':url, 'start':start, 'end':end, 'exitcode':ret, 'size':0})


    def isActive(self):
        """If this thread is currently downloading a file. As this is a thread there is no guarantee
            that the thread remains active after this method returns."""
        return self.active_file != None

    def killProcess(self):
        h = self._handler
        if h: 
            try:
                h.killProcess()
            except: pass    #The process might have just been ended

    def __getTargetFileSize(self):
        file = self.active_file
        if file:
            try:
                return os.path.getsize(file)
            except:
                #maybe the file hasn't been created yet, in any case this is 0
                return 0 

    def getAlreadyDownloaded(self):
        return self.__getTargetFileSize() - self.start_byte

    def getThroughput(self):
        """Mean throughput (byte/s) from last time this method got called or repeat the last measurement
            if called within a second from last measurement (to avoid returning the instant speed)."""
        #we measure this as the mean since last call
        # but to avoid errors if called twice in a row, we set a minimum
        # lag of one second. That means that if called within one second
        # from last call, we will retrieve the last mean instead of
        # calculating it again.
        if self.active_file is None:
            return 0.0
        curr_time  = time.time()
        elapsed_time = curr_time - self.benchmarks.time
      
        if elapsed_time < 1: return self.benchmarks.avg

        curr_bytes = self.__getTargetFileSize()
        if curr_bytes is None:
            log.error( "In getThroughput, curr_bytes is None!  File is %s" % (self.active_file) )
        else:
            avg = (curr_bytes - self.benchmarks.bytes)/elapsed_time

        #update measurement
        self.benchmarks.time = curr_time
        self.benchmarks.bytes = curr_bytes
        self.benchmarks.avg = avg

        return avg


class DownloadManager(object):
    """Manages multiple donloadThreads.

        This class itself is intentionally 'not' multithreading conform. 
        So don't use it in multithreading environments unless you know 
        what you are doing""" 

    def __init__(self):
        self.priorityQueue = PriorityQueue()
        self.threads = []
        self.started = False
        self.lock = Lock()
        self.currentThreads = 0
        self.retry = False   #retries until the file is succesfully download
        self.results = utils.Struct(done=0,failed=0, failed_data=[],doneB=0,doneDL=0,time=time.time())
        log.info("Initialized download manager at %s"%(time.ctime()))

        #init (this can be configurable)
        self.startThreads = 2
        self.maxThreads = 4
        
    def _callback(self, status, **data):
        """This MUST be multithread secured"""
        self.lock.acquire()
        
        if status == DownloadThread.EXIT_STOP: 
            log.debug('Thread %s stoped was stopped.', data['thread'].name)
        elif status == DownloadThread.EXIT_DONE: 
            log.debug('%s done.', data['file'])
            self.results.done += 1
            if data['url'].find('file://')!=0 and 'size' in data.keys():
                self.results.doneB += (data['size'])
                self.results.doneDL += 1  # doneDL only counts files downloaded over the Internet,
                # unlike done which counts all files including local copies
        elif status == DownloadThread.EXIT_DOWNLOAD_ERR:
            log.warn('The download from %s failed.', data['url'])
            if self.retry:
                #reschedule this download
                log.warn('Error downloading %s, retrying..', data['url'])
                self.download(data['file'], data['url'])
            else:
                self.results.failed += 1
                self.results.failed_data.append(data)
                log.error('Could not download %s', data['url'])

        self.lock.release()

    def _addThread(self):
        #we have a limited number of threads don't exceed that
        if self.currentThreads >= self.maxThreads: return

        self.currentThreads += 1
        t = DownloadThread('DownloadThread-{0}'.format(len(self.threads)), self.priorityQueue, callback=self._callback)
        t.start()

        self.threads.append(t)


    def _removeThread(self):
        #we might want to wait a little but this guaranties we'll have at most 1 dead thread in the list
        self._cleanList()

        #nothing to remove!
        if self.currentThreads <= 0: return
        self.currentThreads -= 1

        #next token will cause a thread to be killed
        self.priorityQueue.put((DownloadThread.PRIO_HIGH, DownloadThread.STOP))
        
    def _cleanList(self):
        """Remove al dead threads. Return number of removed threads."""
        #
        #if not self.threads: return 0

        old = self.threads
        self.threads = filter(lambda t: t.isAlive(), old)
        return len(old) - len(self.threads)

    def start(self):
        if self.started: return

        for i in range(self.startThreads):
            self._addThread()

        self.started = True
    

    def stop(self, *force):
        threads = self.threads

        for i in range(len(self.threads)):
            self._removeThread()

        self.started = False
        if force:
            for t in threads: t.killProcess()

    def download(self, url, file, size=None, flags=0, **args):
        """Schedule the download of this file. It will be queued in the current working queue.
            This Methods returns immediately and does not guarantee the time at which the
            download starts. It guarantees it will be sceduled"""
        #log.debug('Adding %s..%s(%s)',url[:30], url[-30:], size)
        
        try:
            self.priorityQueue.put((DownloadThread.PRIO_NORMAL, (file, url, size, flags)))
        except:
            print "jfp exception thrown from priorityQueue.put() for",url
            print sys.exc_info()[:3]
            raise

    def manage(self, verbose=False, benchmark_callback=None):
        """Tells the DownloadManager to manage the download. This call blocks until every file is 
            tried to be downloaded at least once or, if retry is activated, until every file is succesfully
            downloaded (this implies it might never return if, e.g. the server is taken out of producion)
            The current thread will be used for managing which involves, creating Threads and removing them as required.
            bechmark_callback:= call back function(speed=byte/s, files_done=#file_finished, threads=#active_threads)"""

        last_speed = 0.0001
        old_speeds = []
        checking = True
        try_new_thread = True
        last_time = time.time()
        improvement_index = 0
        threshold = 1<<17   #128k
        speeds = self._getSpeeds()

        while self.priorityQueue.qsize() > 0:
            try:
                #sleep a little
                time.sleep(2)

                speeds = self._getSpeeds()

                if not speeds and self.currentThreads: continue #No active threads. Nothing to do.
                
                total_speed = sum(speeds)
                old_speeds.append(total_speed)

                if verbose: self.showStatus(speeds)
                
                #pass benchmarks if a call back was defined
                if benchmark_callback: benchmark_callback(speed=total_speed, files_done=self.results.done, threads=len(speeds))
                log.debug("%s Active Threads at %.2f MB/s (%.2f Mbps). files done:%s, failed:%s, still:%s on %s",len(speeds), total_speed/1024/1024, total_speed*8/1000000, self.results.done, self.results.failed, self.priorityQueue.qsize(),time.ctime())

                if total_speed < threshold: continue #Something's wrong here, more threads will not help, that's for sure

                now = time.time()
                elapsed_time = now - last_time
                if self.currentThreads < self.maxThreads:
                    
                    #in this case we count the average from the first one 
                    total_speed = sum(old_speeds)/len(old_speeds)
                    #until we reach the threads limit we'll dynamically add threads 

                    if checking:
                        improvement_index = total_speed/last_speed
                        log.debug('index=%.2f, threads=%s', improvement_index, len(speeds))
                        if improvement_index > 1.2:
                            #this worked! reset everything 
                            try_new_thread = True
                        elif elapsed_time > 30:
                            #stop checking this is it, don't add any more threads
                            checking = False
                            if self.currentThreads > 1 and improvement_index < 0.8: 
                                #This is worse (definitely not better), so let's take down a thread
                                log.debug('Performance dropped, removing latest thread...')
                                self._removeThread()
                            else:
                                log.debug('No performance increase. Let\'s leave it here...')
                

                    if try_new_thread or elapsed_time > 300:
                        #reset time and start checking again
                        log.debug("Let's see if a new thread helps...")
                        last_speed = total_speed
                        self._addThread()
                        #start procedure
                        old_speeds = []
                        last_time = now
                        checking = True
                        try_new_thread = False
                elif elapsed_time > 600 and self.currentThreads > 1:
                    #all threads are created, let's end one and see what happens...
                    log.debug('Everythings fine... we need some chaos, let\'s drop a thread...')
                    last_time = now
                    self._removeThread()
                    checking = True
                    

    
            except (KeyboardInterrupt, SystemExit):
                #let us end the program if desired!
                #we are going down, take all threads with us!!
                self.stop(True)
                raise
            except:
                # code from http://code.activestate.com/recipes/52215/
                import traceback
                tb = sys.exc_info()[2]
                while 1:
                    if not tb.tb_next:
                        break
                    tb = tb.tb_next
                stack = []
                f = tb.tb_frame
                while f:
                    stack.append(f)
                    f = f.f_back
                stack.reverse()
                traceback.print_exc()
                log.error("Locals by frame, innermost last\n")
                for frame in stack:
                    log.error("Frame %s in %s at line %s", frame.f_code.co_name, frame.f_code.co_filename, frame.f_lineno)
                
        log.debug('The queue is empty, waiting for last threads to finish')
        #The queue is empty, now we only have to wait for the threads to finish
        self.priorityQueue.join()
        #We should check again that the queue is empty, in case the last threads connection broke....
        if verbose:  # added by jfp
            self.showStatus(speeds) #jfp
            timeelapsed = time.time() - self.results.time
            log.info("Completed download at %s"%(time.ctime()))
            log.info('downloaded %d files, %.1f MB in %d seconds'%\
                     ( self.results.doneDL, float(self.results.doneB)/1024/1024, timeelapsed ))
            if timeelapsed>0:
                avspeed = float(self.results.doneB)/timeelapsed
                log.info('Average speed %.1f MB/s, %d Mb/s'%( avspeed/1024/1024, avspeed*8/1000/1000 ))
        log.debug('Last thread reported back. Good bye.')

            
    def _getSpeeds(self):
        """Returns an array of the mean speeds (byte/s) of all active threads, no particular order though"""
        return [ t.getThroughput() for t in filter(lambda t: t.isActive(), self.threads)]
            
        
    def showStatus(self, *speed ):
        if not speed: speed = self._getSpeeds()
        else: speed = speed[0]

        total = sum(speed)
        if speed: avg = total/len(speed)
        else: avg = 0
            
        #jfp added failed statisitic
        print ("{0} Active Threads at {1:.2f} MB/s ({2:.2f} Mbps)."+\
               " files done:{3}, failed:{4}, still:{5} on {6}").\
              format(len(speed), total/1024/1024, total*8/1000000,\
                     self.results.done, self.results.failed, self.priorityQueue.qsize(),time.ctime())
        #jfp was print "{0} Active Threads at {1:.2f} MB/s ({2:.2f} Mbps). files done:{3}, still:{4}".format(len(speed), total/1024/1024, total*8/1000000, self.results.done, self.priorityQueue.qsize())





#### TESTS ####
def test_local():
    source = 'gsiftp://cmip2.dkrz.de:2812//hamburg/cmip5/output1/MPI-M/MPI-ESM-LR/historical/mon/seaIce/OImon/r1i1p1/v20110502/prsn/prsn_OImon_MPI-ESM-LR_historical_r1i1p1_185001-185112.nc'
    #source = 'http://releases.mozilla.org/pub/mozilla.org/firefox/releases/latest/source/firefox-4.0.1.source.tar.bz2'
    q = PriorityQueue()

    def callback(status, **data):
        print "Done", status, data


    t = DownloadThread('test1',q, callback=callback)
    q.put((DownloadThread.PRIO_NORMAL, ('/tmp/replica_test/file1', source, None)))
    t.start()

    time.sleep(1)

    q.put((DownloadThread.PRIO_HIGH, DownloadThread.STOP))

    counter = 40
    while counter > 0:
        counter -= 1
        if q.qsize() == 0: break
        else: log.debug(">> avg: %s, tot:%s, q:%s",t.getThroughput(), t.getAlreadyDownloaded(), q.qsize())
        time.sleep(1)
    
def test_multithreading():
    import threading

    threads_count = threading.activeCount()

    log.debug('Thread count: %s ', threads_count)
    log.debug('Staring DownloadManager...')
    dm = DownloadManager()
    dm.start()

    threads_count = threading.activeCount()
    log.debug('Thread count: %s (dm manages %s, list %s)', threads_count, dm.currentThreads, len(dm.threads))
    
    log.debug('adding one more')
    dm._addThread()
    threads_count = threading.activeCount()
    log.debug('Thread count: %s (dm manages %s, list %s)', threads_count, dm.currentThreads, len(dm.threads))
   
    log.debug('removing one...')
    dm._removeThread()
    threads_count = threading.activeCount()
    log.debug('Thread count: %s (dm manages %s, list %s)', threads_count, dm.currentThreads, len(dm.threads))
    
    log.debug('Let the Thread process stop token...')
    time.sleep(0.1)
    log.debug('checking again...')
    threads_count = threading.activeCount()
    log.debug('Thread count: %s (dm manages %s, list %s)', threads_count, dm.currentThreads, len(dm.threads))
    
    log.debug('Cleaning...')
    removed = dm._cleanList()
    threads_count = threading.activeCount()
    log.debug('Thread count: %s (dm manages %s, list %s) (removed %s)', threads_count, dm.currentThreads, len(dm.threads), removed)

    dm.stop()

def __getDatasetsForReplica():
    from gateway import Gateway
    gateway = 'cmip-gw.badc.rl.ac.uk'
    log.debug('Contacting remote Gateway: %s', gateway)
    g = Gateway('http://' + gateway )
    
    return [ds['id'] for ds in g.listDatasets('cmip5') if ds['id'].startswith('cmip5.output1')]

def __getGatewayFiles(dataset):
    from gateway import Gateway
    gateway = 'cmip-gw.badc.rl.ac.uk'
    log.debug('Contacting remote Gateway: %s', gateway)
    g = Gateway('http://' + gateway)
    
    log.debug('getting files from %s', dataset)


    files = g.listFiles(dataset)

    if not files: raise Exception("No file found! Probably the test is outdated....")
    return files

def main(argv=None):
    from benchmark_db import BenchmarkDB
    from replica_db import ReplicaDB

    if argv is None: argv = sys.argv[1:]

    import getopt
    try:
        args, lastargs = getopt.getopt(argv, "h:D:B:t:e:c:dvqhf:", ['help', 'db-name=', 'db-benchmark-name=', 'target=', 'verbose-benchmark',
                            'start-threads=', 'max-threads=', 'endpoint=', 'batch-size='])
    except getopt.error:
        print sys.exc_info()[:3]
        return 1

    #init values
    db_name = 'replica.db'
    db_benchmark_name = 'benchmark.' + db_name
    endpoint_type = 'GridFTP'
    start_threads = 1
    max_threads = 5
    bench_verbose = False
    batch_size = 20
    target_dir ='.'
    failures = input_file=None
    
    #parse arguments
    for flag, arg in args:
        if flag=='-h' or flag=='--help': return 1
        elif flag=='-D' or flag=='--db-name':               db_name = arg
        elif flag=='-B' or flag=='--db-benchmark-name':     db_benchmark_name = arg
        elif flag=='-t' or flag=='--target':                target_dir = arg
        elif flag=='-e' or flag=='--endpoint':              endpoint_type=arg
        elif flag=='--verbose-benchmark':                   bench_verbose = True
        elif flag=='--start-threads':                       start_threads = int(arg)
        elif flag=='--max-threads':                         max_threads = int(arg)
        elif flag=='--batch-size':                          batch_size = int(arg)
        elif flag=='-f':            input_file = arg

        elif flag=='-d':            log.setLevel(logging.DEBUG)
        elif flag=='-v':            log.setLevel(logging.INFO)
        elif flag=='-q':            log.setLevel(logging.NONE)
        elif flag=='-c':            comment=arg  # comment is ignored
    
    if input_file:
        #no DB here, behave different (I must change the concept, this is a fast workaround)
        file=None
        try:
            file = open(input_file, 'r')
            dm = DownloadManager()
            dm.startThreads = start_threads
            dm.maxThreads = max_threads
            dm.start()
            try:
                for line in file:
                    data = line.split('\t')
                    if len(data)<3: continue   # bad line, probably blank
                    url, local, size = data[:3]
                    if len(data)>5:   # control flags, presently 0 or -1
                        flags=data[5]
                    else:
                        flags=0
                    dm.download(url, local, size=int(size), flags=flags)
            except:
                print "jfp exception caught in Download.main() line loop",line
                print sys.exc_info()[:3]
                raise
            try:
                dm.manage( verbose=bench_verbose)
            except:
                print "jfp exception caught in Download.main() call of dm.manage()"
                print sys.exc_info()[:3]
                raise
            try:
                dm.stop()
            except:
                print "jfp exception caught in Download.main() call of dm.stop()"
                print sys.exc_info()[:3]
                raise

            if dm.results.failed_data:
                if failures is None: failures=[]
                failures.extend(dm.results.failed_data)
                log.error('%s files failed in this batch', len(dm.results.failed_data))

    
        except:
            print "jfp exception caught in Download.main()"
            print sys.exc_info()[:3]
            return 1
        finally:
            if file: file.close()
        return 0
            

    #some checks
    print "jfp db_name=",db_name
    if not os.path.isfile(db_name):
        log.error('Replica DB not found: %s', db_name)
        return 1
    if not os.path.isdir(target_dir):
        log.warn('Target directory does not exists. Creating new: %s', target_dir)
        os.makedirs(target_dir)

    
    db = ReplicaDB('sqlite:///'+ db_name)
    db_bench = BenchmarkDB('sqlite:///' + db_benchmark_name)

    log.debug('Querying DB for files...')
    start = 0

    #jfp: I'm not sure when this second download section would really happen.
    #     So I haven't added any provision for control flags here.
    files = db.getQuery()[start:start+batch_size]
    
    failures = []

    while len(files) > 0:
        log.info('%s files retrieved', len(files))

        def bench_callback(speed, **other):
            db_bench.add(utils.Struct(inbound=speed))

        dm = DownloadManager()
        dm.startThreads = start_threads
        dm.maxThreads = max_threads
        dm.start()

        log.info('Enpoints per file: %s', max([len(f.endpoints) for f in files[:5]]))

        for f in files:
            #for each file we set a source url and a target path to the destination file
            url = None
            
            for ep in f.endpoints:
                if ep.type == endpoint_type:
                    url = ep.url
                    break
            if not url:
                log.error('No endpoint for file: %s', f)
                continue

            local = os.path.join(target_dir, f.path, f.name)
            dm.download(url, local, size=f.size)

        log.debug('Starting dm manage function')
        dm.manage( benchmark_callback=bench_callback, verbose=bench_verbose)
        dm.stop()

        if dm.results.failed_data:
            failures.extend(dm.results.failed_data)
            log.error('%s files failed in this batch', len(dm.results.failed_data))

        #next batch
        start += batch_size
        files = db.getQuery()[start:start+batch_size]


    if failures:
        log.error('A total of %s files failed:', len(failures))
        for data in failures:
            log.error('%s -> %s', data['url'], data['file'])
    else:
        log.info('All files completely transfered')

if __name__=='__main__':
    #configure logging
    logging.basicConfig(level=logging.INFO)
    
    error_code = main()
    if error_code != 0:
        print """Download.py [opt]
Manages the file download from a given replica DB.
Opt:
    -h, --help                  : show this help
    -D, --db-name <file>        : replica DB to use (default: ./replica.db)
    --db-benchmark-name <file>  : benchmark DB to use (default: ./benchmark.<db-name>.db)
    -t, --target <path>         : Target location where files will get downloaded
    -e, --endpoint <name>       : endpoint type for downloading 
                                (HTTPServer, OpenDAP, etc - default: GridFTP)
    --start-threads <#> : Number of threads to start at the beginning
    --max-threads <#>   : Max number of threads the pool can grow to
    --batch-size <#>    : Number of entries from DB to be processed at once.
    --verbose-benchmark : Display benchmark statistics

    -q          : quiet mode
    -v          : verbose mode
    -d          : debug mode
"""
        sys.exit(error_code)

def test_old():
    replica_dbstr = 'replica.db'
    target_dir = '/gpfs_750/transfer/replication_cmip5/mohc'
    db = ReplicaDB('sqlite:///'+ replica_dbstr)
    log.debug('Querying DB for files...')
    start = 0
    size = 1000

    files = db.getQuery()[start:start+size]
    try:
        os.makedirs(target_dir)
    except: pass

    db_bench = BenchmarkDB('sqlite:///bench_' + replica_dbstr)
    while len(files) > 0:
        log.info('%s files retrieved', len(files))

        def bench_callback(speed, **other):
            db_bench.add(utils.Struct(inbound=speed))

        dm = DownloadManager()
        dm.startThreads = 3
        dm.maxThreads = 4
        dm.start()

        log.debug('Enpoints per file: %s', max([len(f.endpoints) for f in files]))

        for f in files:
            url = f.endpoints[0].url
            local = os.path.join(target_dir, f.path, f.name)
            dm.download(url, local, size=f.size)

        log.debug('Starting dm manage function')
        dm.manage( benchmark_callback=bench_callback, verbose=True)
        dm.stop()

        if dm.results.failed_data:
            log.error('Failed files **************')
            for data in dm.results.failed_data:
                log.error('%s -> %s', data['url'], data['file'])
    
        #next batch
        start += size
        files = db.getQuery()[start:start+size]

def test():
    import re
    pat = re.compile('^[^:]+://[^/]+/?(/thredds/fileServer/esg_dataroot|/badc/cmip5/data)?(/.*)$')

    datasets =  __getDatasetsForReplica()
    print datasets
    #Start from ehre we left..
    counter = 0
    for dataset in datasets[0:1]:

        log.debug('Retrieving files...')
        files = __getGatewayFiles(dataset) 
        log.info('Downloading a total of %s file(s) for dataset(%s): %s', len(files), counter, dataset)

        #BADC exclusive :-)
        for f in files:
            #change url
            
            url = pat.sub('gsiftp://cmip-bdm1.badc.rl.ac.uk:2812/badc/cmip5/data\\2', f['endpoints'][0]['url'])
            local = pat.sub(target_dir + '\\2',  f['endpoints'][0]['url'])
            dm.download(url, local, size = int(f['size']))

        dm.manage( benchmark_callback=bench_callback)
        log.debug('Finished Dataset: %s', dataset)
        counter += 1
    dm.stop()


