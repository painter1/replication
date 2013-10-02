import os
from replication.model import replica_db
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


def dvers_from_abspath( abs_path ):
    """from a file's DRS-compliant abs_path string, this finds and returns the dataset version as an integer"""
    # Here's a sample abs_path:
    # cmip5/output1/NASA-GISS/GISS-E2-R/past1000/mon/atmos/Amon/r1i1p127/v20120824/wap/wap_Amon_GISS-E2-R_past1000_r1i1p127_125101-130012.nc
    file_version = (os.path.normpath(abs_path)).split('/')[9]
    if file_version[0]=='v' and len(file_version)>1:
        file_versnum=int(file_version[1:])
    else: file_versnum = int(file_version)
    return file_versnum

def push_files_aside( mgr, rd, previous_files ):
    # based on replica_manager.push_dataset_aside(), written for the gateway system.
    # This uses the P2P system and does not rename the whole dataset.
    # Instead it creates a new one, with another version number, and puts there the files
    # which belong to that version number.
    # rd is a ReplicaDataset found from database self.replica_db (like rep_s before).
    # This function should be used if a dataset+version might contain files belonging to a version
    # different from the dataset+version.  This function will make another dataset+version and
    # move the files to the appropriate version (the abstract files are moved in the database; the
    # actual files are not moved, of course).
    # The purpose is to clean up a situation where files belonging to two versions of the
    # dataset are listed as being together in one version.
    # - mgr is a Manager object from the P2P ingest system, defined in manager.py
    # - rd is a ReplicaDataset found from replication database (like rep_s before).
    # - previous_files is a checksum-based dictionary by which the files in the old rd can be made
    # available for copying into the new dataset, if the dataset revision doesn't affect the file.
    import time
    t0=time.time()
    print "jfp rd=",rd

    maxprint =10
    maxprint1=10
    maxprint2=10
    maxprint3=10
    old_versions = {}
    for file in rd.files:
        maxprint=maxprint-1
        if maxprint>0:
            print file.abs_path
        if file.dataset_name != rd.name and maxprint>0:
            print "ERROR In dataset %s is a file %s with dataset_name %s!" %\
                  (rd.name,file.abs_path,file.dataset_name)
            continue
        file_versnum = dvers_from_abspath( file.abs_path)
        if file_versnum == rd.version:
            #if maxprint>0:
            #    print "file %s is in dataset %s version %s, nothing to do" %\
            #          (file.abs_path,rd.name,rd.version)
            continue
        elif file_versnum != rd.version:
            # rd.version comes from find_remote_datasets() which is guaranteed to only
            # return the latest.
            if file_versnum in old_versions:
                old_versions[file_versnum]['filecount']=1+old_versions[file_versnum]['filecount']
                old_versions[file_versnum]['size']=old_versions[file_versnum]['size']+file.size
            else:
                old_versions[file_versnum]={'versnum':file_versnum, 'filecount':1, 'size':file.size}
                # better to make the old version's ReplicaDataset later, when we have a filecount, etc.
            if maxprint1>0:
                print "file %s belongs in dataset version %s but is in version %s" %\
                      (file.abs_path, file_versnum, rd.version)
                print "new old_versions:",old_versions
                maxprint1 = maxprint1 -1
    print "old_versions is",old_versions
    for overs in old_versions:
        vers = old_versions[overs]
        versnum=vers['versnum']
        mod_rd = replica_db.ReplicaDataset(\
            name="old_"+str(versnum)+'_'+rd.name, version=versnum,\
            status=STATUS.OBSOLETE, gateway=rd.gateway, parent=rd.parent,\
            size=vers['size'], filecount=vers['filecount'], catalog=rd.catalog )
        vers['dataset'] = mod_rd
        mgr.replicas.add(mod_rd)
        print "mod_rd=",mod_rd
    for file in rd.files:
        file_versnum = dvers_from_abspath( file.abs_path)
        if file_versnum == rd.version:
            continue
        # file_versnum != rd.version:
        mod_rd = old_versions[file_versnum]['dataset']
        if file.dataset_name==rd.name:
            file.dataset_name = mod_rd.name
            location = file.getDownloadLocation()
            if not os.path.exists(location):
                location = file.getFinalLocation()
                if not os.path.exists(location):
                    location = None
            if location is not None:
                previous_files[file.checksum] = (location, file.checksum, file.checksum_type)
    mgr.replicas.commit()

