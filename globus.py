#!/usr/apps/esg/cdat6.0a/bin/python
#jfp was #!/usr/local/cdat/bin/python

import subprocess, pprint

# logging setup added by jfp
import logging
log = logging.getLogger('globus')
log.addHandler( logging.FileHandler('globus.log') )

pp = pprint.PrettyPrinter()

usage="""globus.py [opt] 

    --list          : list datasets (default action)
    --list-raw      : just recurse and list the urls instead of the datasets
opt:
    --dataset       : first part of a dataset used for listing (default: cmip5.output1)
    --repo <name>   : give the repo referred [pcmdi, badc, dkrz] or a gsiftp or endpoint
    --max-procs <nr>: Nr. of max parallel processes to use (default = 10)
    --components <nr> : Nr. of components to retrieve (starting from --dataset, default: 9 or 8 if --no-version)
    --no-version  : skip version and list just datasets
    -q          : quiet mode
    -v          : verbose mode
    -d          : debug mode
"""
def main(argv=None):
    import getopt

    if argv is None: argv = sys.argv[1:]

    try:
        args, lastargs = getopt.getopt(argv, "hdvq", ['help', 'dataset=', 'repo=', 'list', 'max-procs=', 'no-version',
            'components=', 'list-raw'])
    except getopt.error:
        print sys.exc_info()[:3]
        return 1

    #init values
    version = True
    show_list = True
    show_list_raw = False
    repo = 'badc'
    dataset = None
    max_procs = 10
    components = None

    #parse arguments
    for flag, arg in args:
        #actions
        if flag=='-h' or flag=='--help':
            global usage
            print usage
            return 0
        elif flag=='-d':            log.setLevel(logging.DEBUG)
        elif flag=='-v':            log.setLevel(logging.INFO)
        elif flag=='-q':            log.setLevel(logging.NONE)

        elif flag=='--dataset':         dataset = arg
        elif flag=='--repo':            repo = arg
        elif flag=='--list':            show_list = True
        elif flag=='--list-raw':        
            show_list_raw = True
            show_list = False
        elif flag=='--max-procs':       max_procs = int(arg)
        elif flag=='--no-version':      version = False
        elif flag=='--components':      components = int(arg)
    


    g = globus(max_procs=max_procs)

    if show_list:
        if dataset:
            ds = g.list_datasets(repo,start_from=dataset.replace('.', '/'), include_version=version, components=components)
        else:
            ds = g.list_datasets(repo, include_version=version, components=components)
        #remove duplicates
        if not version: ds = list(set(ds))
        ds.sort()
        for d in ds:
            print d

    if show_list_raw:
        #jfp was if not components: components = 99999
        if not components: components = 9 #jfp
        start = g.get_repo_url(repo)+dataset.replace('.','/')+'/'  #jfp
        #jfp was dirs = g.find(g.get_repo_url(repo), components)
        dirs = g.find(start, components)  #jfp
        for dir in dirs:
            print dir

    return 0

class globus(object):
    _err = None
    repos = { 
        'badc'  : 'gsiftp://capuchin.badc.rl.ac.uk:2812/badc/cmip5/data/',
        'pcmdi' : 'gsiftp://pcmdi7.llnl.gov:2812/',
        # jfp was... 'dkrz'  : 'gsiftp://cmip2.dkrz.de:2812/wdcc/',
        'dkrz'  : 'gsiftp://cmip2.dkrz.de:2812/',
        'nci'   : 'gsiftp://esgnode1.nci.org.au:2812//',
        'ncar'  : 'gsiftp://vetsman.ucar.edu:2811//datazone/' }
    
    def __init__(self, max_procs=10):
        self.max_procs = max_procs
        self._err = []
    
    def __parse_globus_listing(self, answer):
        list = answer[:-2].split('\n    ')
        base = list[0]
        subdirs = filter( lambda entry: entry[-1]=='/' and entry[0]!='.', list[1:])   #select only dirs, skip files and hidden dirs
        if len(subdirs)>0: #jfp
            return [base + subdir for subdir in subdirs]
        else: #jfp
            return [base]
        
    def find(self, start, max_depth):
        """Grabs the subdirectories of a gridFTP server by listing its contents in parallel."""
        if max_depth <= 0:
            return [start]
        #globus requires an ending slash
        if start[-1] != '/': start = start + '/'
        
        to_scan = [(start, max_depth - 1)]
        done = []
        procs = []
        
        while to_scan or procs:
            if to_scan:
                start, depth = to_scan.pop()
                procs.append((subprocess.Popen(['globus-url-copy', '-list', start], stdout=subprocess.PIPE, stderr=subprocess.PIPE), start, depth))
            
            #should we wait for a free slot?
            if len(procs) >= self.max_procs:
                #take the first out and process
                proc, loc, proc_depth = procs[0]
                procs = procs[1:]
                
                #if it's not ready wait for it
                if proc.wait() != 0:
                    self._err.append((loc, proc.communicate()[1]))
                else:
                    list = self.__parse_globus_listing(proc.communicate()[0])
                    #check if we are done
                    if proc_depth <= 0:
                        #done! Prepare list for retrieval
                        done.extend(list)
                    else:
                        #go one level deeper
                        to_scan.extend([(l, proc_depth - 1) for l in list])
            elif not to_scan:
                #nothing to do, so wait for one to be ready
                proc, loc, proc_depth = procs[0]
                procs = procs[1:]
                
                if proc.wait() != 0:
                    self._err.append((loc, proc.communicate()[1]))
                else:
                    list = self.__parse_globus_listing(proc.communicate()[0])
                    #check if we are done
                    if proc_depth <= 0:
                        #done! Prepare list for retrieval
                        done.extend(list)
                    else:
                        #go one level deeper
                        to_scan.extend([(l, proc_depth - 1) for l in list])
        return done

    def get_repo_url(self, repo):
        """Return the url of the repo, None if unknown or the repo string
            itself if it's already an url"""
        if repo.startswith('gsiftp://'):
            #globus requires this and we need to count it to be consistent
            if repo[-1] != '/': repo += '/'
            return repo
        else:
            repo = repo.lower()
            if repo not in self.repos: return None
            return self.repos[repo]
        
    def list_datasets(self, repo, start_from='cmip5/output1',include_version=True, components=None):
        base = self.get_repo_url(repo)
        if not base:
            print "Unknown repo '%s' Select one of %s \n\
or define it using gsiftp://...)" % (repo, self.repos.keys())
            return []
        
        #globus requires this and we need to count it to be consistent
        if start_from[-1] != '/': start_from += '/'

        if components:
            level = components
        else:
            drs_levels = 9
            level = drs_levels - (len(start_from.split('/')) - 1)
        #this makes only sence if not manually overridden 
        if include_version and not components: level += 1
        results = [ r[:-1] for r in self.find(base + start_from, level) if not ( r.endswith('files/') or r. endswith('latest/'))]
        
        return [dataset[len(base):].replace('/','.') for dataset in results]

if __name__ == '__main__':
    #configure logging

    import sys
    result=main(None)
    if result != 0: print usage
    sys.exit(result)

