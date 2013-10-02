#!/usr/local/cdat/bin/python
import logging as log

CMIP5 = {
    'id' : {
        'parts' : 'activity.product.institute.model.experiment.frequency.realm.table.ensemble'.split('.'),
        'separator' : '.'
    },
    'path' : {
        'parts' : 'activity/product/institute/model/experiment/frequency/realm/table/ensemble/version/variable'.split('/'),
        'separator' : '/'
    },
    'cmor_path' : {
        'parts' : 'activity/product/institute/model/experiment/frequency/realm/variable/ensemble'.split('/'),
        'separator' : '/'
    },
    'filename' : {
        'parts' : 'variable_table_model_experiment_ensemble_?temporal'.split('_'),
        'separator' : '_'
    }
        

}

class DRS(dict):
    __id = None
    __dir = None
    __file = None
    __cmor_dir = None
 
    def __init__(self, id=None, path=None, filename=None, file_id=None, cmor_path=None, generator=CMIP5, **dict):
        """Create a DRS object from many different sources
            id: a valid DRS dataset id
            path: a valid DRS path
            filename: a vlid DRS filename
            file_id: tds file id composed of dataset id and filename
            cmor_path: the path as created by CMOR (no version and a little different to DRS)
            generator: a dictionary containing information on how to parse those (defaults to CMIP5)
            dict: other values will be ingested directly if present"""
        self.__generator = generator
        #self.__id = self.__dir = self.__file = None
        self.__suffix = '.nc'

        if dict: self.update(dict)

        if file_id:
            sep = file_id.rfind('.', 0, -5)
            if not id: id = file_id[:sep]
            if not filename: filename = file_id[sep+1:]

        if id:
            self.__id = id
            self.update(self.__extract('id', id))
        if path:
            #trim last slash if present
            if path[-1:] == '/': path = path[:-1]
            self.update(self.__extract('path', path))
        if cmor_path:
            if cmor_path[-1:] == '/': cmor_path = cmor_path[:-1]
            self.update(self.__extract('cmor_path', cmor_path))

        if filename:
            #Extract file suffix
            location = filename.rfind('.')
            if location > 0 and location > len(filename) - 5:
                #some minor checking...
                self.__suffix = filename[filename.rfind('.'):]
                filename = filename[:filename.rfind('.')]
            
            self.update(self.__extract('filename', filename))
        

        #Some workarounds....
        self.__applyWorkarounds()
    
        
    def __split(self, strings, parts):
        dict = {}
        if len(strings) > len(parts): raise Exception('Can\'t parse this: {0}'.format(strings))
        for n in range(len(strings)):
            if parts[n][0] == '?': dict[parts[n][1:]] = strings[n]
            else: dict[parts[n]] = strings[n]

        return dict

    def __extract(self, type, string):
        return self.__split(string.split(self.__generator[type]['separator']), self.__generator[type]['parts'])


    def __get(self, type):
        parts = self.__generator[type]['parts']
        join = self.__generator[type]['separator']

        if parts and join:
            list = []
            for part in parts:
                if part[0:1] == '?':
                    part = part[1:]
                    if not part in self: continue
                else:
                    if not part in self: raise Exception("Can't create {0} because of missing DRS part: {1}".format(type, part))
                list.append(self[part])
            return join.join(list)
        else:
            raise Exception('Type {0} not properly configured'.format(type))

    def __applyWorkarounds(self):

        #CMIP5 only
        if self.__generator == CMIP5:
            if 'activity' in self and self['activity'] != 'cmip5':
                self['activity'] = 'cmip5'
            
            # it's still not clear how to proceed. version "might, or might not
            # have a v.... 
            if 'version' in self and not self['version'].startswith('v'):
                self['version'] = 'v'+ self['version']

           
    def define(self, **dict):
        for part in dict:
            self[part] = str(dict[part])
        self.__applyWorkarounds()

    def __getattr__(self, attr):
        if attr in self: return self[attr]
        return None


    def getDefinitions(self):
        return self.copy()
        
    def getVersionNumber(self):
        if 'version' not in self: return -1 
        try:
            if self['version'].startswith('v'):
                return int(self['version'][1:])
            else: return int(self['version'])
        except:
            #if here we couldn't parse it!
            log.error('Could not parse version number: %s',self['version'])
            return -1

        
    def getId(self):
        if not self.__id: self.__id = self.__get('id')
        return self.__id

    def getPath(self):
        if not self.__dir: self.__dir = self.__get('path')
        return self.__dir

    def getCMORPath(self):
        if not self.__cmor_dir:
            #there are no partition of output at CMOR
            self.__cmor_dir = self.__get('cmor_path').replace('/output2/','/output/').replace('/output1/','/output/')
        return self.__cmor_dir

    def getFilename(self):
        if not self.__file: self.__file = self.__get('filename') + self.__suffix
        return self.__file

    def copy(self):
        return DRS(id=self.getId(), path=self.getPath(), filename=self.getFilename())
            

if __name__=='__main__':
    file_id = 'cmip5.output1.MPI-M.MPI-ESM-LR.historical.mon.seaIce.OImon.r1i1p1.tas_OImon_MPI-ESM-LR_historical_r1i1p1_1910-2010.nc'
    id = 'cmip5.output1.MPI-M.MPI-ESM-LR.historical.mon.seaIce.OImon.r1i1p1'
    dir = 'cmip5/output1/MPI-M/MPI-ESM-LR/historical/mon/seaIce/OImon/r1i1p1/v20110903/tas'
    file = 'tas_OImon_MPI-ESM-LR_historical_r1i1p1_1910-2010.nc'

    cmip5_id = DRS(id=id)
    cmip5_dir = DRS(path=dir)
    cmip5_file = DRS(filename=file)
    cmip5_file_id = DRS(file_id=file_id)
    cmip5 = DRS(id=id, path=dir, filename=file)

    print "{0} - id:{1}\n{2} - dir:{3}\n{4} - file:{5}".format(cmip5_id.getId() == id, cmip5_id.getId(), 
        cmip5_dir.getPath() == dir, cmip5_dir.getPath(), cmip5_file.getFilename() == file, cmip5_file.getFilename())

    try:
        cmip5_id.getDir()
        print "ERROR!!!"
    except: pass

    #complete
    cmip5_id.define(version='20110903', variable='tas', temporal='1910-2010')
    cmip5_dir.define(temporal='1910-2010')
    cmip5_file.define(activity='cmip5', product='output1', institute='MPI-M', frequency='mon', realm='seaIce', version='20110903')
    cmip5_file_id.define(version='20110903')
    print "Extracting version in dict: {0}".format(cmip5_id['version'])
    print "Extracting version in fromGetVersion: {0}".format(cmip5_id.getVersionNumber())

    print "All id are equal: ", cmip5_id.getId() == cmip5_dir.getId() == cmip5_file.getId() == cmip5_file_id.getId() == cmip5.getId()
    print "All directories are equal: ", cmip5_id.getPath() == cmip5_dir.getPath() == cmip5_file.getPath() ==  cmip5_file_id.getPath() == cmip5.getPath()
    same = cmip5_id.getFilename() == cmip5_dir.getFilename() == cmip5_file.getFilename() ==  cmip5_file_id.getFilename() == cmip5.getFilename()
    print "All file names are equal: ", same
    if not same:
        print cmip5_id.getFilename(), cmip5_dir.getFilename(), cmip5_file.getFilename(),  cmip5_file_id.getFilename(), cmip5.getFilename()

    same = cmip5_id.getDefinitions() == cmip5_dir.getDefinitions() == cmip5_file.getDefinitions() ==  cmip5_file_id.getDefinitions() == cmip5.getDefinitions()
    print "All dictionaries are equal: ", same

    if not same:
        def dict_diff(dict_a, dict_b):
            set_ka=set(dict_a.keys())
            set_kb=set(dict_b.keys())
            only_a=list(set_ka - set_kb)
            only_b=list(set_kb - set_ka)
            diff_dict = {}
            for key in set_ka.intersection(set_kb):
                if dict_a[key] != dict_b[key]:
                    diff_dict[key] = (dict_a[key], dict_b[key])
                #else: print "{0}\n{1}\n".format(dict_a[key], dict_b[key])
            return (only_a, only_b, diff_dict)


        print "Sizes: {0}, {1}, {2}, {3}".format(len(cmip5_id.getDefinitions()), 
            len(cmip5_dir.getDefinitions()),len(cmip5_file.getDefinitions()), len(cmip5.getDefinitions()))
        dicts = [cmip5_id.getDefinitions(), cmip5_dir.getDefinitions(), cmip5_file.getDefinitions(), cmip5_file_id.getDefinitions(), cmip5.getDefinitions()]
        
        for n in range(len(dicts)-1):
            print "Diff between {0} and {1}: ".format(n, n+1) ,dict_diff(dicts[n], dicts[n+1])
        

