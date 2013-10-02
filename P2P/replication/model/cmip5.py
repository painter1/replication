"""Utilities for handling and interacting with CMIP5 data"""
import os

#helpclass for creating DRS conform paths.
class Cmip5DRSTranslator(object):
    """Manages CMIP5 DRS translation duties"""
    _trans = None

    def __init__(self):
        from drslib import cmip5
        self._trans = cmip5.make_translator('cmip5')


    def get_file_path(self, file_name, version, drs_id):
        try:
            drs = self._trans.filename_to_drs(file_name)
            #complete this from the id of the remote
            drs.version = version
            drs.product, drs.institute, drs.model,drs.experiment, drs.frequency , drs.realm =  drs_id.split('.')[1:7]

            path = self._trans.drs_to_filepath(drs)
            if os.path.basename(path) == file_name:
                return path
            #there'S one known exception, the -clim suffix (from _clim), we *must* change this.
            elif os.path.basename(path)  == (file_name[:-8] + '-' + file_name[-7:]):
                return path
            else:
                print "Filename changed by drs_lib!! from %s to %s" % (file_name, os.path.basename(path))
        except:
            import sys
            print "Exception thrown while computing path of (%s, %s, %s)" % (file_name, version, drs_id)
            print sys.exc_info()
            raise
        return None

