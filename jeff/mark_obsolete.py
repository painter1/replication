#!/usr/apps/esg/cdat6.0a/bin/python

# Suppose that a file exists in two versions, each represented by a different row of the table
# replica.files.  They will each have the same dataset_name.  Here we identify the older-version
# file, change its status to 50 ("obsolete").
# Ideally we would add a prefix "old_" to its dataset_name, but that means defining a new dataset,
# so we don't do that (yet).

# test case:
sample_datasets = 'cmip5.output1.IPSL.IPSL-CM5A-LR.abrupt4xCO2.6hr.atmos%'

import sqlalchemy
from esgcet.config import loadConfig
from cmip_gc import existing_versions

def filename(row):
    return row['abs_path'].split('/')[-1]
def rowversion(row):
    return row['abs_path'].split('/')[9]

def mark_obsolete_in_dataset( dataset_name, engine, table ):
    """For each file (in the table replica.files) in the specified dataset (name should appear in
    replica.datasets), changes its status to 50 ("obsolete") if it is not the latest known version.
    The file's version is a dataset version number, extracted from the file's CMIP-5 DRS-compliant
    abs_path.
    """
    s = table.select( table.c.dataset_name==dataset_name ) 
    result = conn.execute(s)  # all rows of replica.files with the specified dataset_name

    sr = []
    srf = {}
    for row in result:
        # Note that you can loop through result this way only once.
        sr.append(row)
        fn = filename(row)
        if fn in srf:
            srf[fn].append(row)
        else:
            srf[fn] = [row]

    #sr.sort( key=filename )

    for fn,rows in srf.items():
        if len(rows)<=1: continue
        rows.sort( key=rowversion )
        print "jfp will keep abs_path=",rows[-1]['abs_path'],"status=",rows[-1]['status'],\
              "dataset_name=",rows[-1]['dataset_name']
        for row in rows[0:-1]:
            abs_path = row['abs_path']
            dataset_name = "old_"+row['dataset_name']
            print "jfp will do update for abs_path=",abs_path,"status from",row['status'],"to 50"
            s = table.update().where( table.c.abs_path==abs_path ).\
                                  values( status=50 )
            #if dataset_name.find('old_old_')!=0:
            #    s = table.update().where( table.c.abs_path==abs_path ).\
            #        values( dataset_name=dataset_name )
            # ... doesn't work, you first have to create a row in replica.datasets with this name.
            result = conn.execute(s)


config = loadConfig(None)
engine = sqlalchemy.create_engine(config.get('replication', 'esgcet_db'), echo=False, pool_recycle=3600)
conn = engine.connect()
md = sqlalchemy.MetaData(engine)
replica_files_table = sqlalchemy.Table( 'files', md, autoload=True, schema='replica' )

sqlst = "SELECT name FROM replica.datasets WHERE name LIKE '%s'"%sample_datasets
report = engine.execute( sqlalchemy.sql.text(sqlst)).fetchall()
dataset_names = [r[0] for r in report]

for dataset_name in dataset_names:
    mark_obsolete_in_dataset( dataset_name, engine, replica_files_table )

