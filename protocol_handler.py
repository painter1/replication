#!/usr/local/cdat/bin/python
"""Handle download of files in a subprocess according to the given
protocol."""
import subprocess, re, time, os
import logging
log = logging.getLogger('download')

class BaseProtocol(object):
    """Base class for all protocol handlers."""
    
    def __init__(self, url):
        """All protocols have some kind of url"""
        self.url = url
        self._proc = None

    def _createProc(self, *args, **kwargs):
        """Every protocol must implement this!"""
        raise Exception('Not Implemented!')

    def getProc(self, *args, **kwargs):
        return self._proc

    def killProcess(self):
        p = self._proc
        if p:
            try:
                p.kill()
            except: pass    #The process might have just been ended
       

    def getFile(self, target_file, block_check = True,  **params):
        """gets a file either in blocking or secured blocking mode.
            secured blocking, blocks, but not forever. If the download stalls it stops."""
        self._proc = self._createProc(target_file, **params)
        if target_file.startswith('file://'):
            local_file = target_file[7:]
        else:
            local_file = target_file
        
        #simple block
        if not block_check: return self._proc.wait()

        #non always-blocking
        #this should get the file or fail if it's impossible. Never block forever...

        #heuristic warmup period
        time.sleep(2)

        stall_counter = 0   #current stall round counter
        max_stall_counter = 10  #rounds without downloads after a stall is declared
        # max_stall_counter = 25  #jfp increased from 10 to 25 for NIMR (Korean) data
        downloaded = 0      #size of downloaded file
        check_secs = 5      #round time
        # check_secs = 250  #jfp increased from 5 to 250 for NIMR (Korean) data
        if target_file.find("BNU")>0:
            check_secs = 60
            max_stall_counter = 20

        while self._proc.poll() is None:
            #wait actively for this to be complete
            time.sleep(check_secs)

            #check current file size
            try:
                curr = os.path.getsize(target_file)
            except:
                curr = 0 #File might have not been created
            if downloaded == curr:
                stall_counter += 1
                log.debug('this process appears to be stalled, add a counter now in %s', stall_counter)

                if stall_counter > max_stall_counter:
                    #this is it, we consider this stalled

                    log.warn('Process appears to be stalled, aborting download')
                    #kill the stalled  process normally
                    self._proc.kill()

                    max_killing_wait = 5
                    while self._proc.poll() is None:
                        if max_killing_wait < 0:
                            #can't kill the process softly, terminate it (all puns were intended)
                            log.warn("We had to force process termination!")
                            self._proc.terminate()
                        #wait a little
                        time.sleep(3)
                        max_killing_wait -= 1

                    #proces was killed, we will let this loop die self._proc.poll() should return a non 0 return value

            else:
                downloaded = curr
                stall_counter = 0
                #log.debug('Download is running')

        return self._proc.poll()


class __DummyProtocol(BaseProtocol):
    """Test dummy protocoll."""
    def __init__(self, url, **init_args):
        super(self.__class__, self).__init__(url)

    def _createProc(self, target_file, start = 0, end = 1024, duration = 3, **params):
        log.debug("Dummy Downloading %s from %s [%s-%s]", target_file, self.url, start, end)

        #now wait a second in a subprocess to mimic normal behaviour and write empty file
        steps = 100
        stepsize = int((end-start)/steps)
        cmd = 'for i in $(seq {0}); do dd if=/dev/zero of={1} bs=1 seek=$((i*{2}-1+{5})) count=1 2>/dev/null; sleep {3}; done; dd if=/dev/zero of={1} bs=1 seek={4} count=1 2>/dev/null'.format(steps, target_file,stepsize, float(duration)/steps, end-1, start)
        log.debug(cmd)
        return subprocess.Popen(cmd, shell=True)


class __GsiftpProtocol(BaseProtocol):
    """handle gsiftp"""
    def __init__(self, url, **init_args):
        super(self.__class__, self).__init__(url)

    def _createProc(self, target_file, start = 0, end = None,\
                    globus_args = '-binary -q -tcp-bs 5242880', **params):
#jfp was                    globus_args = '-binary -q -tcp-bs 1048576', **params):
                
        if start > 0: globus_args += ' -partial-offset {0}'.format(start)
        if end and end > start: globus_args += ' -partial-length {0}'.format(end-start)

        cmd = 'globus-url-copy {0} {1} {2}'.format(globus_args, self.url, target_file).split(' ')
        log.debug(' '.join(cmd))
        return subprocess.Popen(cmd)


class __ftpProtocol(BaseProtocol):
    """handle ftp via wget"""
    # added by jfp, same as __HTTPProtocol
    # globus-url-copy supports ftp, but not really - it requires "adjust socket buffer" in the server
    def __init__(self, url, **init_args):
        print "jfp starting __ftpProtocol.init()"
        BaseProtocol.__init__(self, url)
        self.cert = None
        self.cacert = None

    def _createProc(self, target_file, start = 0, end = None, wget_args = '', **params):
        wget_args += ' --no-check-certificate'
        wget_args += '  secure-protocol=TLSv1' #jfp don't use SSLv3
        wget_args += ' --certificate {0} --private-key {0} --ca-directory {1}'.format(*self.getSecurity())
        if end:
            log.info('End set to %s but wget cannot handle this.'%(end))
        if start > 0:
            wget_args += ' -c'

        cmd = 'wget {0} {1} -O {2}'.format(wget_args, self.url, target_file).split(' ')
        log.debug(' '.join(cmd))
        return subprocess.Popen(cmd)

    def getSecurity(self):
        if not self.cert:
            self.cert = os.getenv('X509_USER_PROXY')
            if not self.cert: raise Exception('Certificate not found')
            self.cacert = os.getenv('X509_CERT_DIR')
            if not self.cacert: raise Exception('CA dir not found, set X509_CERT_DIR properly')
        return (self.cert, self.cacert)


class __sftpProtocol(BaseProtocol):
    """handle sftp directly.  Actually this runs scp under expect."""
    # added by jfp.  scp is a disgusting way to run large amounts of data, but sometimes you have to.
    def __init__(self, url, **init_args):
        BaseProtocol.__init__(self, url)
        self.cert = None
        self.cacert = None

    def _createProc(self, target_file, start = 0, end = None, sftp_args = '', **params):
        sftp_args += ''
        if start>0: 
            log.error("\nsftp doesn't support partial download, url=%s"%self.url)
            return subprocess.Popen('false')

        # Break down the url and put it back together as wanted for a sftp command line.
        url1 = self.url.split('@')  # e.g. [ 'sftp://usr:pwd', 'host/path/file' ]
        usrpwd = url1[0].split('//')[1]  # e.g. 'usr:pwd'
        usr = usrpwd.split(':')[0]
        if len(usrpwd.split(':'))>1:  pwd = usrpwd.split(':')[1]
        else: pwd = None
        url3 = url1[1].split('/')
        urlhost = url3[0]    # e.g. 'host'
        urlpath = '/'+'/'.join(url3[1:])  # e.g. '/path/file'
        sftp_args = usr+'@'+urlhost       # e.g. usr@host
        scp_cmd = 'scp -q {0}\:{1} {2}'.format( sftp_args, urlpath, target_file ).split(' ')
        print "jfp scp_cmd=",scp_cmd
        if pwd:
            expect_prefix = 'expect -d -c \'exp_internal 1; spawn'
            expect_postfix = '; expect "password:" { send "hg2ao@KMA\n"}; set timeout -1; expect eof\''
            cmd = [expect_prefix] + scp_cmd + [expect_postfix]
        else:
            cmd = scp_cmd

        # Here is an example of what cmd will look like (newlines added for readability):
        # expect -d -c 'exp_internal 1; spawn
        #   scp guest@ksv.inm.ras.ru\:/arhive/guest/CMIP5/output/INM/inmcm4/1pctCO2/mon/atmos/cl/r1i1p1/cl_Amon_inmcm4_1pctCO2_r1i1p1_209001-209912.nc
        # /cmip5/scratch/cmip5/output1/INM/inmcm4/1pctCO2/mon/atmos/cl/r1i1p1/cl_Amon_inmcm4_1pctCO2_r1i1p1_209001-209912.nc;
        # expect "password:" { send "arhivarius\n"}; set timeout -1; expect eof'

        cmd = ' '.join(cmd)   # It's not a list of commands, so Popen will only understand this as a string.
        log.debug(cmd)
        return subprocess.Popen(cmd,shell=True)  #jfp shell=True is necessary to run expect

class __sftpProtocol_curl(BaseProtocol):
    """handle sftp via curl"""
    # added by jfp, same as __HTTPProtocol_curl.  Doesn't presently work because curl was built without sftp.
    def __init__(self, url, **init_args):
        BaseProtocol.__init__(self, url)
        self.cert = None
        self.cacert = None

    def _createProc(self, target_file, start = 0, end = None, curl_args = '-s --location --cookie /dev/null', **params):
        #BUGFIX: we need the cookie flag although it points to nothing...
        #(cert, cacert) = self.getSecurity()
        curl_args += ' --cert {0} --capath {1}'.format(*self.getSecurity())
        if start > 0: 
            curl_args += ' -C {0}'.format(start)
            if end and end > start: curl_args += ' -r {0}-{1}'.format(start, end-start)

        #BUG!!! the -o redirects ALL OUTPUT to the file, even if you get a 403...
        cmd = 'curl {0} {1} -o {2}'.format(curl_args, self.url, target_file).split(' ')
        log.debug(' '.join(cmd))
        return subprocess.Popen(cmd)

    def getSecurity(self):
        if not self.cert:
            self.cert = os.getenv('X509_USER_PROXY')
            if not self.cert: raise Exception('Certificate not found')
            self.cacert = os.getenv('X509_CERT_DIR')
            if not self.cacert: raise Exception('CA dir not found, set X509_CERT_DIR properly')            
        return (self.cert, self.cacert) 

class __HTTPProtocol_curl(BaseProtocol):
    """handle http via curl"""
    def __init__(self, url, **init_args):
        BaseProtocol.__init__(self, url)
        self.cert = None
        self.cacert = None

    def _createProc(self, target_file, start = 0, end = None, curl_args = '-s --location --cookie /dev/null', **params):
        #BUGFIX: we need the cookie flag although it points to nothing...
        #(cert, cacert) = self.getSecurity()
        curl_args += ' --cert {0} --capath {1}'.format(*self.getSecurity())
        if start > 0: 
            curl_args += ' -C {0}'.format(start)
            if end and end > start: curl_args += ' -r {0}-{1}'.format(start, end-start)

        #BUG!!! the -o redirects ALL OUTPUT to the file, even if you get a 403...
        cmd = 'curl {0} {1} -o {2}'.format(curl_args, self.url, target_file).split(' ')
        log.debug(' '.join(cmd))
        return subprocess.Popen(cmd)

    def getSecurity(self):
        if not self.cert:
            self.cert = os.getenv('X509_USER_PROXY')
            if not self.cert: raise Exception('Certificate not found')
            self.cacert = os.getenv('X509_CERT_DIR')
            if not self.cacert: raise Exception('CA dir not found, set X509_CERT_DIR properly')            
        return (self.cert, self.cacert) 

class __HTTPProtocol_wget(BaseProtocol):
    """handle http via wget"""
    def __init__(self, url, **init_args):
        BaseProtocol.__init__(self, url)
        self.cert = None
        self.cacert = None

    def _createProc(self, target_file, start = 0, end = None, wget_args = '', **params):
        wget_args += ' --secure-protocol=TLSv1'
        wget_args += ' --no-check-certificate'
        wget_args += ' --certificate {0} --private-key {0} --ca-directory {1}'.format(*self.getSecurity())
        wget_args += ' --progress=dot'   #jfp: less output to nohup.out
        wget_args += '  secure-protocol=TLSv1' #jfp don't use SSLv3
        if end:
            log.info('End set to %s but wget cannot handle this.'%(end))
        if start > 0:
            wget_args += ' -c'

        cmd = 'wget {0} {1} -O {2}'.format(wget_args, self.url, target_file).split(' ')

        log.debug(' '.join(cmd))
        return subprocess.Popen(cmd)

    def getSecurity(self):
        if not self.cert:
            self.cert = os.getenv('X509_USER_PROXY')
            if not self.cert: raise Exception('Certificate not found')
            self.cacert = os.getenv('X509_CERT_DIR')
            if not self.cacert: raise Exception('CA dir not found, set X509_CERT_DIR properly')
        return (self.cert, self.cacert)

class __HTTPSProtocol(__HTTPProtocol_wget):
    """handle https via wget, the same as with http."""

class __FileProtocol(BaseProtocol):
    """Handle files available locally. This wil create hardlinks if possible"""
    def __init__(self, url, **init_args):
        super(self.__class__, self).__init__(url)

    def _createProc(self, target_file, start = 0, end = None, **params):
        abs_path = self.url[7:]
        path = os.path.dirname(target_file)

        if not os.path.isdir(path):
            log.debug("creating dir %s" %  path)
            os.makedirs(path)

        if not os.path.isfile(abs_path):
            log.error("Missing source: %s" % abs_path)
        elif os.path.isfile(target_file) and os.path.getsize(target_file)>0:  #jfp added getsize check
            log.error("Target %s already exists" % target_file)
        else:
            #everything is fine
            #jfp was cmd = 'ln %s %s' % (abs_path, target_file)
            cmd = 'cp %s %s' % (abs_path, target_file)
            log.debug(cmd)
            return subprocess.Popen(cmd.split())

        #something was wrong
        return subprocess.Popen('false')



## Basic methods
__handler_pat = re.compile('^([^/:]+):')
__handlers = {}

def __getProtocol(url):
    mo = __handler_pat.match(url)
    if mo:  return mo.group(1)
    else:   return 'file'

def addHandler(protocol, function, **args):
    if protocol in __handlers and 'overwrite' in args and args['overwrite']:
        raise Exception('Handler allready set.')

    #we could assert this function is well-formed...
    __handlers[protocol] = function

def getHandler(url):
    print "jfp entered protocol_handler.getHandler(",url,")"
    protocol = __getProtocol(url)
    if protocol not in __handlers:
        raise Exception('No handler defined for "{0}"'.format(protocol))
    return __handlers[protocol](url)

def getFile(url, file, **kwargs):
    return getHandler(url).getFile(file, **kwargs)

## init 


addHandler('http', __HTTPProtocol_wget)
addHandler('gsiftp', __GsiftpProtocol)
addHandler('ftp', __ftpProtocol)
addHandler('sftp', __sftpProtocol)
addHandler('file', __FileProtocol)
addHandler('https', __HTTPSProtocol)



if __name__=='__main__':

    print "jfp entered protocol_handler main"
    #configure logging
    log.basicConfig(level=log.DEBUG)
    url1 = 'http://wiki.python.org/moin/SSL'
    url2 = 'gsiftp://cmip-bdm1.badc.rl.ac.uk:2812/badc/cmip5/data/cmip5/output1/MOHC/HadGEM2-ES/historical/6hr/atmos/6hrLev/r1i1p1/v20101208/hus/hus_6hrLev_HadGEM2-ES_historical_r1i1p1_197106010600-197109010000.nc'
    url3 = 'http://vesg.ipsl.fr/thredds/fileServer/esg_dataroot/CMIP5/output1/IPSL/IPSL-CM5A-LR/aqua4K/3hr/atmos/3hr/r1i1p1/v20110429/clt/clt_3hr_IPSL-CM5A-LR_aqua4K_r1i1p1_197901010130-198511262230.nc'
    #file = '/gpfs_750/transfer/replication_cmip5/ipsl/CMIP5/output1/IPSL/IPSL-CM5A-LR/aqua4K/3hr/atmos/3hr/r1i1p1/v20110429/clt/clt_3hr_IPSL-CM5A-LR_aqua4K_r1i1p1_197901010130-198511262230.nc'
    file = '/tmp/test_ph'
    size = '743675192'

    h = getHandler(url3)
    import os.path
    current_size=os.path.getsize(file)
    
    ret = h.getFile(file, start=current_size)

    import sys
    sys.exit(0)

    #h = getHandler(url2)
    #ret = h.getFile(file, globus_args='-vb -p 1', end=1024*1024*100)
    log.debug('Returncode: %s', ret)

    h = getHandler(url1)
    ret = h.getFile(file, start=10, end=200, duration=10 )
    log.debug('Returncode: %s', ret)

    ret = getFile(url1, file, start=10, end=200, duration=3 )
    log.debug('Returncode: %s', ret)
        

