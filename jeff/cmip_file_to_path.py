# Tools to produce download lists for Estani's scripts, given directory listings, etc.
# This will be a perpetual work-in-progress.

import sys

def file2path( file = 'snw_LImon_GISS-E2-R_amip_r5i1p1_195101-201012.nc',
               institute='GISS', version = 'v20120315', product='output1' ):
    # Convert a DRS-compliant file name to a DRS-like path.
    # e.g.  'snw_LImon_GISS-E2-R_amip_r5i1p1_195101-201012.nc' to
    # '/cmip5/scratch/cmip5/output1/GISS/GISS-E2-R/amip/mon/landIce/LImon/r5i1p1/v20120315/
    #  snw/snw_LImon_GISS-E2-R_amip_r5i1p1_195101-201012.nc'

    table2realm = { 'Amon':'atmos', 'Lmon':'land', 'Omon':'ocean', 'OImon':'seaIce',\
                    'LImon':'landIce', 'aero':'aerosol' }
    table2freq  = { 'Amon':'mon', 'Lmon':'mon', 'Omon':'mon', 'OImon':'mon',\
                    'LImon':'mon', 'aero':'mon' }

    (variable,table,model,experiment,ensemble,datesnc) = file.split('_')
    try:
        freq  = table2freq[table]
        realm = table2realm[table]
    except KeyError:
        print "ERROR: file2path does not know freq,realm for table",table
        raise

    path = '/cmip5/scratch/cmip5/'+'/'.join([product,institute,model,experiment,freq,realm,\
                                            table,ensemble,version,variable])+'/'+file
    return path


def fix_dllist_1( dllist="download-all"):
    # "fix" a partially constructed download list.  It already consistes of one line per file
    # to be downloaded, with fields separated by tabs.  The second field is just a filename.
    # This function will convert that field to a full DRS-compliant path.
    f = open(dllist)
    g = open(dllist+'_1','w')
    for line in f:
        fields = line.split('\t')
        if len(fields)<1: continue
        path = file2path( fields[1] )
        fields[1] = path
        line_1 = '\t'.join(fields)
        g.write(line_1)
    f.close()
    g.close()

if len(sys.argv)<2:
    print "Please provide a filename argument, output from 'replica_manager.py --download-list...'."
    file = 'download-all'  # for testing convenience
else:
    file = sys.argv[1]
fix_dllist_1(file)
