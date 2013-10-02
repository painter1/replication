import catalog,re
import sys

tds_fix_re = re.compile('(.*\.nc)_[0-9]*$')

def compare_keys(first, second):
    keys_1 = set(first.keys())
    keys_2 = set(second.keys())
    
    return (keys_1 & keys_2, keys_1 - keys_2, keys_2 - keys_1)


def compare_files(master_list, replica_list):
    comp_att = ['size', 'checksum']
    m_att = {}
    for m in master_list:
        #workaround for file_id uniquenes...
        f_id = tds_fix_re.match(m['file_id'])
        if f_id: f_id = f_id.group(1)
        else: f_id = m['file_id']
        m_att[f_id] = m
    
    r_att = {}
    for r in replica_list:
        #workaround for file_id uniquenes...
        f_id = tds_fix_re.match(r['file_id'])
        if f_id: f_id = f_id.group(1)
        else: f_id = r['file_id']
        r_att[f_id] = r
    
    both, m_only, r_only = compare_keys(m_att, r_att)
    if m_only:
        m_only = list(m_only)
        m_only.sort()
    if r_only: 
        r_only = list(r_only)
        r_only.sort()
    difference = {}
    for key in both:
        for att in comp_att:
            if m_att[key][att] != r_att[key][att]:
                if att == 'file_id':
                    #workaround for file_id uniquenes...
                    tds_fix_re.match(m_att[key][att]).group(0) == tds_fix_re.match(r_att[key][att]).group(0)
                    
                if key not in difference: difference[key] = {}
                difference[key][att] = {'master':  m_att[key][att], 'replica': r_att[key][att]}
    return (both, m_only, r_only, difference)


def compare_catalogs(master_url, replica_url):
    master = catalog.getDatasetMetadata(master_url)
    replica = catalog.getDatasetMetadata(replica_url)
    
    both, m_only, r_only, difference = compare_files(master['files'], replica['files'])
    if difference or m_only or r_only:
        print ">> Difference at: \n%s\n%s" % (master_url, replica_url)
        return both, m_only, r_only, difference
    return False

    
def compare_root_catalogs(master, replica, data_re):
    results = {}
    
    #get the interesting subset from master catalogs[dataset][version]
    m_catalogs = catalog.getAllCatalogs(master)
    for dataset in m_catalogs.keys():
        if data_re.match(dataset) is None:
            del m_catalogs[dataset]
    
    #get the interesting subset from the replica
    r_catalogs = catalog.getAllCatalogs(replica)
    for dataset in r_catalogs.keys():
        if data_re.match(dataset) is None:
            del r_catalogs[dataset]
    
    both, m_only, r_only = compare_keys(m_catalogs, r_catalogs)
    if m_only: 
        print "Not replicated: ", m_only
        for d in m_only:
            for v in m_catalogs[d]:
                results[dataset] = {}
                results[dataset][v] = (False, True, False, False)
    if r_only: 
        print "Not in original: ", r_only
        for d in r_only:
            for v in r_catalogs[d]:
                results[dataset] = {}
                results[dataset][v] = (False, False, True, False)
    
    for dataset in both:
        print "Checking: ", dataset
        results[dataset] = {}
        
        m_ver = m_catalogs[dataset]
        r_ver = r_catalogs[dataset]
        both_v, m_only_v, r_only_v = compare_keys(m_ver, r_ver)
            
        if m_only_v: 
            print "Version not replicated: %s (%s)" % (dataset, m_only_v)
            for v in m_only_v:
                results[dataset][v] = (False, True, False, False)
        if r_only_v: 
            print "Version removed: %s (%s)" % (dataset, r_only_v)
            for v in r_only_v:
                results[dataset][v] = (False, False, True, False)
        
        for ver in both_v:
            results[dataset][ver] = compare_catalogs(m_ver[ver], r_ver[ver])
            if results[dataset][ver]:
                both, m_only, r_only, differences = results[dataset][ver]
    return results

def print_diff(diffs):
    for dataset in diffs.keys():
        for ver in diffs[dataset].keys():
            #no diff at all
            if not diffs[dataset][ver]: continue
            
            both, m_only, r_only, differences = diffs[dataset][ver]
            if both and not (m_only or r_only or differences):
                #no diff
                print 'x'
            elif m_only and not (both or r_only or differences):
                #Only in master
                print "Not replicated:\n%s (%s)" % (dataset,ver)
            elif r_only and not (both or m_only or differences):
                #only in replica
                print "Removed from master:\n%s (%s)" % (dataset,ver)
            else:
                #in both
                print "Changes in %s (v%s)" % (dataset, ver)
                print "both:%s m_only:%s r_only:%s difference:%s" % (len(both), len(m_only), len(r_only), len(differences))
                if m_only: print "Not replicated:\n%s" % '\n'.join(m_only)
                if r_only: print "Removed from master:\n%s" % '\n'.join(r_only)
                if differences: print "differences:\n%s" % '\n'.join(differences)
                


def print_rename(diffs):
    data_root='/gpfs_750/projects/CMIP5/data/'
    to_rename = {}
    for dataset in diffs.keys():
        for ver in diffs[dataset].keys():
            if not diffs[dataset][ver]: continue
            
            both, m_only, r_only, differences = diffs[dataset][ver]
            #just display files I need to rename
            if not (m_only and r_only): continue
            
            map_file = '%s_%s_ESG-PCMDI.map' % (dataset, ver)
            target = ['%s/v%s/%s/%s.nc' % (data_root+dataset.replace('.','/'), ver, f.split('.')[-2].split('_')[0],f.split('.')[-2]) for f in m_only]
            source = ['%s/v%s/%s/%s.nc' % (data_root+dataset.replace('.','/'), ver, f.split('.')[-2].split('_')[0],f.split('.')[-2]) for f in r_only]
            to_rename[map_file] = (source, target)
            print "Changes in %s" % map_file
            print "both:%s m_only:%s r_only:%s difference:%s" % (len(both), len(m_only), len(r_only), len(differences))
    return to_rename


def rename_files(to_rename):    
    #rename files
    map_root='/gpfs_750/transfer/replication_cmip5/replica_manager/map/'
    rename_map_root='/gpfs_750/transfer/replication_cmip5/replica_manager/correct/'
    for map in to_rename.keys():
        map_string = open(map_root + map, 'r').read()
        source, target = to_rename[map]
        for i in range(len(source)):
            if map_string.find(source[i]) < 0:
                print "source not found!"
                continue
            map_string = map_string.replace(source[i], target[i])
        
        fo = open(rename_map_root + map, 'w')
        fo.write(map_string)
        fo.close()


master='http://norstore-trd-bio1.hpc.ntnu.no/thredds/esgcet/catalog.xml'
replica='http://bmbf-ipcc-ar5.dkrz.de/thredds/esgcet/catalog.xml'
datasets='cmip5.output1.NCC.%'
#datasets='cmip5.output1.NCC.NorESM1-M.rcp26.day.land.day.r1i1p1'

data_re = re.compile(datasets.replace('.','\.').replace('%','.*'))

results = compare_root_catalogs(master, replica, data_re)
print_diff(results)

