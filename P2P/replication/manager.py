#!/usr/local/cdat/bin/python
"""Manages replication"""
from model import p2p
from model import replica_db
from model import catalog
from urllib2 import URLError
import logging
import re
import time
import os,os.path
import sys
import pymd5
from drslib.translate import TranslationError

log = logging.getLogger(__name__)
log.addHandler( logging.FileHandler('manager.log') )
logging.basicConfig(level=logging.DEBUG)

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

class Manager(object):
    DEFAULT_CONSTRAINTS = {
        'project':'CMIP5',
        'product':'output1',
    }
        
    BASIC_SEARCH = {
        'time_frequency': ['fx','yr','mon'], 
        'experiment':['amip','historical','piControl','rcp26','rcp45','rcp60','rcp85']
    }

#jfp was:
#   LOCAL_TRANSLATION = {
#        re.compile('^http://(bmbf-ipcc-ar5|cmip3).dkrz.de/thredds/fileServer/|gsiftp://(bmbf-ipcc-ar5|cmip3).dkrz.de[^/]*/') : 'file:///gpfs_750/project/CMIP5/data/',
#    """Stores translations (compiled regexps)from urls to local paths"""
#    }
    LOCAL_TRANSLATION = {}

    #jfp was  local_node='esgf-data.dkrz.de', node_domain='.dkrz.de'
    def __init__(self, local_node='pcmdi9.llnl.gov', node_domain='.llnl.gov'):
        self.local_node = local_node
        self.original_node = local_node #can be changed. But the node at creation time is "special"

        self.p2p = p2p.P2P(node=local_node, defaults=Manager.DEFAULT_CONSTRAINTS)
        self.replicas = replica_db.getSession(shared=False)
        self.drs_writer = None

        #This defines the domain of all datanodes controlled by the replicating agent
        #these are used to see what has already being replicated and which files are
        #already avalable
        self.node_domain = node_domain

    def __setup_drs_writer(self):
        if not self.drs_writer:
            #this is for cmip5 only at this time
            from replication.model import cmip5
            self.drs_writer = cmip5.Cmip5DRSTranslator()

    def find_remote_datasets(self, search=BASIC_SEARCH, force=False):
        """Finds remote not-replicated  datasets defined in the search using the search API and a
distributed search.
The datasets might be already ingested but are not published yet.
The datasets returned will all have latest=True."""
        #start by some arbitrary unit: model
        basic_unit = 'model'
        #print "jfp search=",search
        #jfp normal: potential_models = self.p2p.get_facets(basic_unit, distrib=True, replica=False,
        #               latest=True, query="-data_node:*%s" % self.node_domain, **search)[basic_unit]
        potential_models = self.p2p.get_facets(basic_unit, distrib=True, replica=False,
                 latest=True, query="-data_node:*%s" % self.node_domain, **search)[basic_unit]
        llnl_no_hyphen = False
        #jfpquery = "-data_node:*%s" % self.node_domain
        #print "jfp in find_remote_datasets, query=",jfpquery
        #potential_models = self.p2p.get_facets\
        #                   (basic_unit,
        #                    query=jfpquery, **search)[basic_unit]
        if potential_models=={}:
            # For queries from LLNL about data published at LLNL, the hyphen before "data_node" won't
            # work. From LLNL to elsewhere, it's needed.  I certainly don't know why! (jfp).
            # Re-doing the query when it returns {} is not always be correct, but it's a
            # quick fix which will work more often than not.
            potential_models = self.p2p.get_facets(basic_unit, distrib=True, replica=False,
                     latest=True, query="data_node:*%s" % self.node_domain, **search)[basic_unit]
            llnl_no_hyphen = True

        ##filter what we have already replicated
        if force:  #jfp
            replicated_models = {}
        elif llnl_no_hyphen:
            # jfp: The purpose of the following query seems to be to avoid ingesting data on datasets
            # which have replicated.  But it also throws out datasets which have been originally
            # published here.  So don't do it if we've discovered that any of the data lives at LLNL.
            replicated_models = {}
        else:
            # jfp: This time there's no hyphen before data_node because we're always looking at LLNL.
            replicated_models = self.p2p.get_facets(
                basic_unit, distrib=False, latest=True,
                query="data_node:*%s" % self.node_domain, **search)['model']

        remote = set()
        for model, totals in potential_models.items():
            log.debug("Checking model: %s, totals=%s", model,totals)
            print "Checking model:", model," totals=",totals
            if model in replicated_models:
                if totals == replicated_models[model]:
                    log.debug("looks up to date (must check more thoroughly)")
                    print "jfp looks up to date (must check more thoroughly)"
                    continue
                elif totals > replicated_models[model]:
                    log.debug("we are missing %s datasets", totals - replicated_models[model])
                else:
                    #this normally means we have versions not available in the original repo anymore
                    log.error("We have more datasets than the original repo! remote:%s vs local:%s", 
                            totals, replicated_models[model])
                    #just continue...
            elif force==False:
                log.debug("model %s not replicated"%model)
                print "model %s not replicated"%model

            #process the model and find exactly what's missing (datasets and version numbers)
            #get all interesting datasets from remote data_nodes.
            try:
                lsearch = search.copy() #jfp copy search to lsearch so we can change it safely
                if 'model' in lsearch.keys():  #jfp get_dataset_names won't accept two model
                    del lsearch['model']       #jfp specifications
                    #jfp note that in this case the model is the only element of potential_models
                if llnl_no_hyphen:
                    all_remote = self.p2p.get_datasets_names(
                        model=model, distrib=True, query="data_node:*%s" % self.node_domain, latest=True, **lsearch)
                else:
                    all_remote = self.p2p.get_datasets_names(
                        model=model, distrib=True, query="-data_node:*%s" % self.node_domain, latest=True, **lsearch)
            except URLError as e:
                log.error("dataset listing from %s failed with %s",model,e)
                continue
                
            #now perform the same search but only on local nodes
            if force: #jfp
                already_replicated = set([])
            elif llnl_no_hyphen:
                # jfp: Again, we won't catch data published at LLNL if we run the next query,
                # It seems to ask what datasets exist at LLNL.
                already_replicated = set([])
            else:
                already_replicated = self.p2p.get_datasets_names(
                    model=model, distrib=False,
                    query="data_node:*%s" % self.node_domain, latest=True, **lsearch)
            remote.update(all_remote - already_replicated)

        return remote


    def find_missing(self, search=BASIC_SEARCH, force=False):   #jfp added force
        """Finds the datasets defined in the search that are not replicated nor in the process of being replicated."""
        #jfp  This returns missing datasets. A dataset is identified by name + version.
        missing = self.find_remote_datasets(search,force)

        #we still need to check the dataset "currently" being replicated
        db_datasets = self.replicas.query(replica_db.ReplicaDataset.name, 
                    replica_db.ReplicaDataset.version).all()
        #jfp:
        #few_datasets=[p for p in db_datasets if p[0].find('cmip5.output1.CNRM-CERFACS.CNRM-CM5.historicalMisc.mon.ocean.Omon')==0]
        #print "jfp"
        #import pprint
        #print "jfp in find_missing, remote_datasets="
        #pprint.pprint(missing)
        #print "jfp in find_missing, few_datasets="
        #pprint.pprint(few_datasets)

        return missing - set(db_datasets)

    def find_withdrawn( self, search=BASIC_SEARCH ):   #jfp added 
        """Finds the datasets in the database which are not discovered in the search.
        But if the search finds hardly any datasets, with this or BASIC_SEARCH,
        bail out - maybe the server is temporarily down.
        """

        knownnow = self.find_remote_datasets(search,force=True)
        if len(knownnow)<1:
            known_basic = self.find_remote_datasets(Manager.BASIC_SEARCH,force=True)
            if len(known_basic)<1:
                return []

        db_datasets = self.replicas.query(replica_db.ReplicaDataset.name, 
                                          replica_db.ReplicaDataset.version).all()
        # <<< should filter this according to search <<<
        #print "jfp in find_withdrawn, search=",search
        # crude, inflexible (only CMIP5 DRS) but easily coded substitute for filtering
        # (instead of all()) db_datasets:
        drsdict = {'project':0, 'product':1,'institute':2, 'model':3, 'experiment':4,
            'time_frequency':5, 'realm':6, 'cmor_table':7, 'ensemble':8 }
        for k in search.keys():
            if k=='project':
                # The database understands lower-case 'cmip5', not 'CMIP5'.
                search[k]=search[k].lower()
            kk = drsdict[k]
            db_datasets = [ p for p in db_datasets for pp in [p[0].split('.')] if
                            len(pp)>=8 and pp[kk]==search[k] ]

        #jfp:
        #few_datasets=[p for p in db_datasets if p[0].find('cmip5.output1.COLA-CFS.CFSv2-2011.decadal1985.mon.ocean.Omon')==0]
        #print "jfp"
        #import pprint
        #print "jfp in find_withdrawn, known-now datasets="
        #pprint.pprint(knownnow)
        #print "jfp in find_withdrawn, db_datasets="
        #pprint.pprint(db_datasets)
        #print "jfp in find_withdrawn, few_datasets="
        #pprint.pprint(few_datasets)

        return set(db_datasets) - knownnow

    def forget_withdrawn( self, search=BASIC_SEARCH ):  #jfp added
        """Finds the datasets which have been withdrawn, and moves them into the near-nonexistence
        of renamed datasets with status=STATUS.WITHDRAWN."""
        candidates = self.find_withdrawn( search )
        # ...typical element (cmip5.output1.COLA-CFS.CFSv2-2011.decadal1985.mon.ocean.Omon.r1i1p1, 20120515)

        for (dataset,version) in candidates:
            qrd=self.replicas.query(replica_db.ReplicaDataset).filter(\
                replica_db.ReplicaDataset.name==dataset)
            for rd in qrd:
                if rd.status==STATUS.WITHDRAWN or rd.status==STATUS.OBSOLETE:
                    continue
                if rd.status>=STATUS.VERIFYING:
                    continue
                previous_files = {}
                self.push_dataset_aside( rd, previous_files, new_status=STATUS.WITHDRAWN )

    def push_dataset_aside( self, rd, previous_files, new_status=STATUS.OBSOLETE ):   #jfp
        # based on replica_manager.push_dataset_aside(), written for the gateway system.
        # rd is a ReplicaDataset found from database self.replica_db (like rep_s before).
        # This function makes a re-named copy of it and associated files, and then deletes it
        # and the associated files.  The purpose is to make room to put a newer version of the
        # dataset into the database replica.datasets. But this also can be called to deal with
        # a withdrawn dataset.  In that case, set new_status to STATUS.WITHDRAWEN
        # previous_files is a checksum-based dictionary by which the files in the old rd can be
        # found later.
        import time
        t0=time.time()

        # rd is obsolete.  Make a copy with a different name and status...
        name_for_obsolete = "old_"+str(rd.version)+'_'+rd.name
        # would be better to restrict the query so qname is shorter - at least use the institute:
        qname = self.replicas.query(replica_db.ReplicaDataset.name).all()
        qname = [ tup[0] for tup in qname ]
        #print "jfp initial name_for_obsolete=",name_for_obsolete
        # the following two lines are for debuggin only
        # print "jfp qname=",qname
        # raise Exception("jfp debugging, don't want to go on")
        if name_for_obsolete in qname:
            for i in range(99):
                name_for_obsolete = "old_"+str(i)+'_'+str(rd.version)+'_'+rd.name
                #print "jfp next name_for_obsolete=",name_for_obsolete
                if not name_for_obsolete in qname: break
            if i>=98:
                raise Exception("Can't build a name for obsolete version of dataset %s",rd.name)
        print "jfp final name_for_obsolete=",name_for_obsolete
        mod_rd = replica_db.ReplicaDataset(\
            name=name_for_obsolete, version=rd.version,\
            status=new_status, gateway=rd.gateway, parent=rd.parent,\
            size=rd.size, filecount=rd.filecount, catalog=rd.catalog )
        self.replicas.add(mod_rd)
        # The new copy of the dataset still has those files...
        # The files pointing to rd should instead point to the copy, mod_rd.
        for file in rd.files:
            if file.dataset_name==rd.name:
                file.dataset_name = mod_rd.name
##                location = file.getDownloadLocation()
##                if not os.path.exists(location):
##                    location = file.getFinalLocation()
##                    if not os.path.exists(location):
##                        location = None
                location = None
                for loc in file.getDownloadLocations():
                    if os.path.exists(loc):
                        location = loc
                        break
                if location==None:
                    # final locations are a bit trickier because the dataset version is not
                    # visible in the path
                    for loc in file.getFinalLocations():
                        if os.path.exists(loc):
                            # could get the version: locver = pubpath2version.pubpath2version(location)
                            #                        if locver==rd.version:
                            # but it's about as fast (if doing one file at a time) and more reliable
                            # to compute the checksum:
                            cksum = pymd5.md5(loc).lower()
                            if file.checksum==cksum:
                                location = loc
                                continue
                if location is not None:
                    previous_files[file.checksum] = (location, file.checksum, file.checksum_type)

        # Now that copies have been made of the existing (old) dataset and its files, we want
        # to delete the originals to make room for the new versions; and also delete those files'
        # file_access rows (there's no need for a copy of the file_access data - it's old, so if we
        # don't have it, we don't want it).
        self.replicas.flush()  # makes the new file.dataset_name visible to file_access rows
        # N.B. the with_labels() is probably unnecessary: it's a remnant of some messing around...
        fsquery = self.replicas.query(replica_db.ReplicaFile.abs_path).filter(\
            replica_db.ReplicaFile.dataset_name.like(mod_rd.name) ).with_labels()
        aquery = self.replicas.query(replica_db.ReplicaAccess).filter(\
            replica_db.ReplicaAccess.abs_path.in_(fsquery) )
        an = aquery.delete(synchronize_session='fetch')

        log.info("jfp %d file_access rows deleted; about to commit"%(an))
        # The commit is needed so that the dataset will know that there are no longer files pointing
        # to it.  Maybe just a flush() would be good enough.
        self.replicas.commit()

        # Finally, it's safe to delete the old dataset:
        self.replicas.delete(rd)
        self.replicas.commit()

    def ingest_dataset(self, datasets, drs_translate=True, force=False, use_original_server=True):
        """Ingests the given (dataset, version) tuple into the replica_db.
This implies extracting the metadata from the catalog.
We expect the dataset to be the latest version (i.e., latest=True)"""
        #print "jfp entering ingest_dataset, datasets=",datasets
        if len(datasets)<1: return None  #jfp
        if not isinstance(datasets, list): datasets = [datasets]
        if isinstance(datasets[0], basestring): datasets = [(d, None) for d in datasets]

        #make sure this is setup if we are going to use it
        if drs_translate: self.__setup_drs_writer()

        if use_original_server:
            search = {'fields': 'url,size,number_of_files,index_node', 'replica':False}
        else:  # use a replica server
            search = {'fields': 'url,size,number_of_files,index_node', 'replica':True}
        #>>>>> replica:True gets only replicas, replica:False gets only original.  We want both!
        #>>>>> In fact, we want to find all servers for every file!
        for dataset, version in datasets:
            log.info("Processing  dataset %s (%s)", dataset, version)
            print "Processing  dataset %s (%s)" % (dataset, version) #jfp

            if version:
                res = self.p2p.get_datasets(master_id=dataset, version=version, **search)
                #jfp test res = self.p2p.get_datasets(instance_id=dataset, version=version, **search)
            else:
                #just get the latest
                res = self.p2p.get_datasets(master_id=dataset, latest=True, **search)
            if not res or len(res) != 1:
                log.error("Invalid number of results returned!\n->%s<-", \
                          self.p2p.show(res, return_str=True)) #jfp added res as a bugfix
                if not res: continue
            for res0 in res:
                print "jfp looking at res0=",res0
                catalog_url = self.p2p.extract_catalog(res0)
                if catalog_url:
                    res = res0
                    break
                #>>>>> Here we have just one result for one catalog url.  Why not get them all?? <<<<<<<
            print "jfp catalog_url=",catalog_url
            if not catalog_url:
                log.error("Skipping. Can't find catalog")
                continue
            metadata = catalog.getDatasetMetadata(catalog_url)
            if not metadata:
                log.error("Skipping. Can't extract metadata ")
                continue
            facc = [f['access'] for f in metadata['files']]  #jfp debug
            faccflat = [a for fa in facc for a in fa] #jfp debug
            #print "jfp non-pcmdi cl file accesss=",\
            #      [ a for a in faccflat if a['url'].find('pcmdi')<0 and a['url'].find('/cl/')>0]

        
            #now we have all we need.
        
            if 'number_of_files' in res:
                print "jfp res['number_of_files']=",res['number_of_files']," len(metadata['files'])=",len(metadata['files'])
                if res['number_of_files'] != len(metadata['files']):
                    log.error("File count between index and node doesn't match! index:%s, node:%s",
                            res['number_of_files'], len(metadata['files']))
                    #now what?!
                    continue

            # fixed: use merge to add entries with new version
            #jfp No, it's not fixed: if the dataset already exists, we should move it out of the
            #jfp way or delete it, not change its version number and leave the database filled
            #jfp with garbage as the following merge command would do.
            #jfp added the lines below...
            qrd=self.replicas.query(replica_db.ReplicaDataset).filter(\
                replica_db.ReplicaDataset.name==dataset)
            get_the_new_version = True
            previous_files = {}
            if not version and 'version' in res: version = res['version']
            for rd in qrd:
                #jfp fixup, no longer needed (I hope)
                #from push_files_aside import *
                #push_files_aside( self, rd, previous_files )
                #...jfp fixup

                # Version numbers: normally the numerically greater version number is the
                # latest, e.g. 20120430 > 20110911.  But not always; in 2012, CSIRO restarted its
                # versions with version 1.  Now that I've harvested all of those v1 files, it will be
                # safe to ignore the CSIRO special case.
                # Why not simply test version numbers for equality, i.e. use "!=" in place
                # of "<" below?  After all, the query which produced the new version requires
                # only the "latest=True" versions of datasets to be returned.  The problem is
                # that some nodes will lie to you.  For example, a node may have an out-of-date
                # replica from another node, but still tell you that it is the latest version
                # > taken out for CSIRO, put this back soon > if rd.version<version:
                # special versions for CSIRO, should no longer be needed:
                # if rd.version<version or (dataset.find('CSIRO')>0 and rd.version!=version):
                if rd.version<version or (dataset.find('CSIRO')>0 and\
                                          rd.version>20110100 and rd.version<20120701 ):
                    log.warn( ("dataset %s with version %d has been superseded\n"+10*' '+
                                 "by version %s.  We'll try to get it!" )\
                                 % ( dataset, rd.version, version ) )
                    print ("dataset %s with version %d has been superseded\n"+10*' '+
                                 "by version %s.  We'll try to get it!" )\
                                 % ( dataset, rd.version, version )
                    if rd.version>version:
                        log.warn( "Strange version numbers: %s<%s but %s is the latest!"\
                                  % ( version, rd.version, version ) )
                        print "Strange version numbers: %s<%s but %s is the latest!"\
                                  % ( version, rd.version, version )
                    self.push_dataset_aside( rd, previous_files )  #jfp
                    get_the_new_version = True
                    break
                elif rd.status == STATUS.WITHDRAWN or rd.status==STATUS.UNAVAILABLE:
                    # dataset was retracted/withdrawn or simply messed up, then republished
                    rd.status = STATUS.INIT
                    get_the_new_version = False
                else:
                    get_the_new_version = False
            # TO DO (jfp) make use of previous_files - saves a lot of downloading
            # BUT maybe current_files (see below) does the same job
            print "jfp get_the_new_version=",get_the_new_version,"  version=",version
            if get_the_new_version:
                #...jfp added the lines above and introduced variable rdnew...
                rdnew = replica_db.ReplicaDataset(name=dataset,version=version, 
                            gateway=res['index_node'], parent='P2P', size=res['size'], 
                            filecount=len(metadata['files']),  catalog=catalog_url,
                            status=replica_db.STATUS.INIT)
                self.replicas.merge(rdnew)

            #compare size and number_of_files with metadata
            if 'number_of_files' in res and len(metadata['files']) != res['number_of_files']:
                log.error("Skipping. File count doesn't match. Solr:%s, catalog:%s",
                            res['number_of_files'], len(metadata['files']) )
                continue
            if 'size' in res and sum([long(f['size']) for f in metadata['files']]) != res['size']:
                log.error("Skipping. Total size doesn't match. Solr:%s, catalog:%s",
                            res['size'], sum([long(f['size']) for f in metadata['files']]))
                continue

            #prepare files in a proper structure (in dict with key = checksum)
            current_files = {}
            for file in metadata['files']:
                #sanity mod: assure checksums are in lower caps
                if 'checksum' in file.keys():
                    file['checksum'] = file['checksum'].lower()
                    file['checksum_type'] = file['checksum_type'].lower()

                #alter the access to have a set by url
                new_access = {}
                for acc in file['access']:
                    acc_url = acc['url']
                    del acc['url']
                    new_access[acc_url] = acc
                file['access'] = new_access
                #add to known files
                if 'checksum' in file.keys():
                    current_files[(file['checksum'], file['checksum_type'])] = file

            #Expand the files to be replicated by those from the same dataset but any other 
            #version that has being replicated and has the same checksum.
            #This will intentionally include old versions already replicated at the current node.
            # jfp revised to catch urlopen timeout errors
            log.debug("jfp will query file files in dataset %s",dataset)
            try:
                filels = self.p2p.files(fields='checksum,checksum_type,url', 
                                  distrib=True, replica=True, query='dataset_id:%s' % (dataset))
                for file in filels:
                    #skip those which provide no checksum info (we can't be sure they are right)
                    #jfp look at this later, maybe I don't want to skip here>>>>
                    #print "jfp file=", file
                    if 'checksum' not in file or 'checksum_type' not in file:
                        #I see no need of logging this...
                        continue

                    #get all possible keys (more than one checksum possible in search API!
                    keys = [(file['checksum'][i].lower(), file['checksum_type'][i].lower()) \
                            for i in range(len(file['checksum']))]
                
                    for key in keys:
                        if key in current_files:
                            #Add this accesses and transform local
                            for url_code in file['url']:
                                url, mime, url_type = url_code.split('|')
                                for pattern in self.LOCAL_TRANSLATION:
                                    if pattern.search(url):
                                        url = pattern.sub(self.LOCAL_TRANSLATION[pattern], url)
                                        url_type = 'local'
                                        break
                                current_files[key]['access'][url] = {'type':url_type}
            except URLError as e:
                print "file listing failed (1) with",e
                log.error("file listing failed (1) with %s",e)
                continue
            except Exception as e:
                print "file listing failed (2) with",e
                log.error("file listing failed (2) with %s",e)
                continue

            #now we have everything in place, ingest the files
            log.info('Ingesting %s files', len(current_files))
            files_actually_added = 0
            for file in current_files.values():
                #extract the file name from the url as there's no other field with this information!
                file_name = None
                for url in file['access']:
                    if file['access'][url]['type'] in ['HTTPServer', 'GridFTP']:
                        file_name=url.split('/')[-1]
                        break
                if not file_name: raise Exception("Can't find file name")
                try:
                    abs_path = self.drs_writer.get_file_path(file_name, version, dataset)
                except TranslationError as e:
                    print "drslib cannot parse file name=",file_name
                    if file_name.find('EC-EARTH')>0:
                        # One of the two institutes publishing EC-EARTH model output is ICHEC.
                        # ICHEC is known to have published bad file names.
                        # They can't be parsed automatically.
                        print "will ignore the file and continue"
                        continue
                    else:
                        # Otherwise, an error may indicate something which needs to be fixed in
                        # drslib, or in ~/.metaconfig.conf .
                        raise e

                # added by jfp....  Let's not wipe out information on an existing file, if
                # the newly harvested file is identical, and we have it!
                # If we don't have it, there's nothing much to lose, and we may gain a
                # better url...
                if abs_path==None or len(abs_path)<1:
                    continue
                apexp = replica_db.ReplicaFile.abs_path.like( abs_path )
                fq = self.replicas.query(replica_db.ReplicaFile).filter( apexp )
                oldfiles = fq.all()
                if len(oldfiles)>0:
                    if len(oldfiles)>1: print "jfp many oldfiles", abs_path, len(oldfiles)
                    oldfile = oldfiles[0]
                    if oldfile.status>=replica_db.STATUS.RETRIEVED:
                        continue
                    if oldfile.status>=replica_db.STATUS.VERIFYING and\
                       oldfile.checksum==file['checksum']  and\
                       oldfile.checksum_type==file['checksum_type'] and\
                       oldfile.size==long(file['size']):
                        continue
                    elif oldfile.status>=replica_db.STATUS.VERIFYING:
                        if oldfile.checksum!=file['checksum']:
                            print "file",file_name,"checksum has changed from",\
                                  oldfile.checksum,type(oldfile.checksum)," to",\
                                  file['checksum'],type(file['checksum'])
                        elif oldfile.size!=long(file['size']):
                            print "file",file_name,"size has changed from",\
                                  oldfile.size,type(oldfile.size)," to",\
                                  long(file['size']),type(long(file['size']))
                        else:
                            print "file",file_name,"something odd has changed"
                #...added by jfp

                #create the files objects and add them to the DB, we are done!
                try:
                    self.replicas.merge(replica_db.ReplicaFile(
                        abs_path=abs_path, dataset_name=dataset, 
                        checksum=file['checksum'], checksum_type=file['checksum_type'], 
                        size=file['size'], status=replica_db.STATUS.INIT,
                        mtime=time.mktime(time.strptime(file['mod_time'], '%Y-%m-%d %H:%M:%S'))))
                    files_actually_added += 1
                except ValueError as e:
                    print "EXCEPTION:",e
                    print "WARNING skipping file",file_name
                    print "because of above exception.  file['mod_time']=",file['mod_time']
                    continue

                #yes, we do a second loop just not to mix things up. If here, everything is fine.
                for url in file['access']:
                     self.replicas.merge(replica_db.ReplicaAccess(abs_path=abs_path, url=url,
                                type=file['access'][url]['type']))

            sys.stdout.flush()  # If there were a log.flush function, it would be better.
            #ready, commit or rollback
            try:
                self.replicas.commit()
            except:
                log.error("Couldn't commit... rolling back changes\n%s", sys.exc_info())
                self.replicas.rollback()
            sys.stdout.flush()  # If there were a log.flush function, it would be better.

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
    import re
    import copy
    facet_pat = re.compile(r'(.*[^\\])=(.*)')

    if argv is None: argv = sys.argv[1:]
    try:
        args, lastargs = getopt.getopt(argv, "f:h",
                                       ['help','facet=','list-missing', 'list-withdrawn',
                                        'forget-withdrawn', 'list-remote', 'ingest', 'force-ingest',
                                        'default-search',
                                        'use-original-server', 'use-replica-servers'])
    except getopt.error:
        print sys.exc_info()[:3]
        return 1

    #jfp added force_ingest here and below...
    list_missing = list_withdrawn = list_remote = forget_withdrawn = ingest = force_ingest = False
    #facets = copy.deepcopy(Manager.BASIC_SEARCH)
    facets = {}
    use_original_server = True
    #parse arguments *!!!*
    for flag, arg in args:
        if flag=='--list-withdrawn':      #List withdrawn datasets
            list_withdrawn = True
        elif flag=='--list-missing':        #List datasets available for replication
            list_missing = True
        elif flag=='--list-remote':          #List only remote datasets (includes those currently in the replication process)
            list_remote = True
        elif flag=='--forget-withdrawn':    #Forget about withdrawn datasets (i.e., new name and status)
            forget_withdrawn = True
        elif flag=='--ingest':              #Ingest result into replica DB for processing
            ingest = True
        elif flag=='--force-ingest':        #Ingest result even if already present.  Resets status, so you may need to re-run a verify.
            force_ingest = True
        elif flag=='-h' or flag=='--help':        #This help
            usage()
            return 0
        elif flag=='-f' or flag=='--facet':  #Set facet for search (e.g. institute=MPI) can be used multiple times.
            facet, value = facet_pat.match(arg).groups()
            facets[facet] = value
        elif flag=='--default-search':   #Use the default search (cmip5, output1 and predefined sets of time-frequencies and experiments are hard coded)
            facets = dict(facets, **copy.deepcopy(Manager.BASIC_SEARCH))
        elif flag=='--use-original-server':
            use_original_server=True
        elif flag=='--use-replica-servers':
            use_original_server=False


    #jfp: checking multiple nodes should be completely unnecessary, but I'll leave that
    # here until I'm finished testing...
    # nodedoms = [['esgf-data.dkrz.de','dkrz.de'],['pcmdi9.llnl.gov','llnl.gov'],['esgf.nccs.nasa.gov','nasa.gov']]
    print "jfp in main facets=",facets
    if use_original_server:
        nodedoms = [['pcmdi9.llnl.gov','llnl.gov']]
    else:   # The way the inner searches work, they'll only pick up one replica per ESGF node.
            # So query multiple nodes.
        nodedoms = [['pcmdi9.llnl.gov','llnl.gov'],['esgf-data.dkrz.de','dkrz.de']]
    for node,node_domain in nodedoms:
        print "jfp node=",node,"node_domain=",node_domain
        mng = Manager(local_node=node,node_domain=node_domain)

        if list_missing: 
            #jfp normalprint '\n'.join(['%s#%s' % dv for dv in sorted(mng.find_missing(search=facets))])
            #jfp as in force_ingest:
            print '\n'.join(['%s#%s' % dv for dv in sorted(mng.find_missing(search=facets,force=True))])

        if list_withdrawn: 
            print '\n'.join(['%s#%s' % dv for dv in sorted(mng.find_withdrawn(search=facets))])

        if list_remote: 
            #jfp normal print '\n'.join(['%s#%s' % dv for dv in sorted(mng.find_remote_datasets(search=facets))])
            #jfp as in force_ingest:
            print '\n'.join(['%s#%s' % dv for dv in sorted(mng.find_remote_datasets(search=facets,force=True))])

        if forget_withdrawn:
            mng.forget_withdrawn( search=facets )

        if ingest:
            mng.ingest_dataset( list(mng.find_missing(search=facets)),
                                use_original_server=use_original_server )

        if force_ingest:
            mng.ingest_dataset( list(mng.find_remote_datasets(search=facets,force=True)),
                                use_original_server=use_original_server )

    return 0


if __name__ == '__main__':
    import sys
    result=main(None)
    if isinstance(result, int):
        if result != 0: usage()
        sys.exit(result)


