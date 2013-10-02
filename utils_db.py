#!/usr/local/cdat/bin/python
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy import Column, Integer, String, create_engine, orm, ForeignKey, ForeignKeyConstraint
import logging as log
Base =  declarative_base()


class DAO(object):
    """This class provides some basic functionallity to all DAO classes."""
    
    def _setValues(self, dict, *values, **args):
        """Set some values (normally attributes in columns) from a given dictionary.
            dict := extract values from keys in this dictionary that are
            *values := list of names to be considered from dict (if not present the whole dictionary will be used)
            **args may define:
                skip := string or list of strings for items to be skipped from dict"""

        if 'skip' in args:
            skip=args['skip']
            if isinstance(skip, basestring):
                skip=[skip]
        else:
            skip=[]

        #extract intersection of dictionary and current columns
        items = [ c.name for c in self.__table__.columns if c.name in dict]
        
        if values:
            #further specify the values (if not in items, it won't be even considered)
            items = [ i for i in items if i in values]
        
        #insert all the attributes as long as not in 'skip'
        for att in items:
            if att not in skip:
                type = self.__table__.columns[att].type.__class__.__name__
                if type == 'String': 
                    self.__dict__[att] = dict[att]
                elif type == 'Integer' and dict[att]: 
                    self.__dict__[att] = int(dict[att])
                else: 
                    #in other cases just use the string or NoneType
                    self.__dict__[att] = dict[att]
    def getPrimaryKeys(self):
        """Return the primary keys values of this object"""
        return orm.object_mapper(self).primary_key_from_instance(self)

    def __new2__(cls, data=None, session=None):
        if session and data:
            #session.query(cls).get(
            pk = [ data[k.name] for k in cls.__table__.primary_key]
            obj = session.query(cls).get(pk)
            if obj: 
                #log.debug('getting object from session %s', pk)
                obj._inSession = session
                return obj
        #else:
            #log.debug('no session %s', cls)
        #log.debug('creating object %s', cls)
        return super(DAO, cls).__new__(cls)
    def __init__(self):
        self._hash = "".join([ str(k) for k in self.getPrimaryKeys()]).__hash__()
        log.info('here!')

    def __init__(self, dict):

        #extract intersection of dictionary and current columns
        items = [ c.name for c in self.__table__.columns if c.name in dict]

        #insert all the attributes as long as not in 'skip'
        for att in items:
            type = self.__table__.columns[att].type.__class__.__name__
            if type == 'String':
                self.__dict__[att] = dict[att]
            elif type == 'Integer' and dict[att]:
                self.__dict__[att] = int(dict[att])
            else:
                #in other cases just use the string or NoneType
                self.__dict__[att] = dict[att]

        #get the hash value
        self._hash = "".join([ str(k) for k in self.getPrimaryKeys()]).__hash__()
        
    def __str__(self):
        """Short representation, only primary keys"""
        return "<{0} id:{1}>".format(self.__class__.__name__, "".join(self.getPrimaryKeys()))

    def __repr__(self):
        """Complete representation of this object."""
        return "<{0} ({1})>".format(self.__class__.__name__, \
            ",".join(map(lambda k: k+"="+str(self.__dict__[k]), filter(lambda name: name[0] != '_', self.__dict__))))

    def __eq__(self, other):
        """They are equal if same object and primary keys are equal"""
        return self.__hash__() == other.__hash__()
        #return self.__class__ == other.__class__ and all([ self_pk == other_pk for (self_pk, other_pk) in zip(self.getPrimaryKeys(), other.getPrimaryKeys())])

    def __hash__(self):
        if '_hash' not in self.__dict__:
            self.__dict__['_hash'] = "".join([ str(k) for k in self.getPrimaryKeys()]).__hash__()
        return self._hash


class DB(object):

    def __init__(self, db_url):
        self._db_url = db_url
    
        self._engine = create_engine(self._db_url)
        self._session = None

    def open(self):
        """Assure we have access to the DB and that it's properly created"""
        if not self._session:
            Base.metadata.create_all(self._engine)
            self._session = orm.scoped_session(orm.sessionmaker(self._engine, autoflush=False, autocommit=False, expire_on_commit=False))
    def _merge_all(self, objects):
        self.open()
        map(self._session.merge, objects)
        self._session.commit()

    def _add_all(self, objects, db_objects=[], new=None, existing=None, alter_session=True, update_function=None):
        """Adds many objects at once. In case the objects are already present, an update callback function
        can be used for handling those.
        objects: objects to be inserted in the DB
        db_objects: a list/query of db objects which correspond to those being inserted. E.g. if the key is a composit, selecting
            all objects from the DB that have only one part of the key in common with those being inserted will do.
        new: if a list is provided, the newly created objects will get returned.
        existing: if a list is provided, the updated objects will be returned.
        alter_session: If the session schould be altered (and thus persist) (if not, nothing will change)
        update_function: f(db, new) - function which should handle update of objects (db object will remain, new will get discarded)"""
        self.open()
        
        #we need a hashed set to make this run faster
        db = {}
        for db_o in db_objects: db[db_o]=db_o

        if new is None: new = []
        if existing is None: existing = []


        for o in objects:
            if o in db: 

                #old in DB: db[g], new: g
                #update db[g] if required, it will get updated after commiting
                if update_function: update_function(db[o], o) # update_function(db_data, new_data)
                existing.append(db[o])
            else:
                #this object wasn't in the DB
                new.append(o)

        if alter_session:
            if new: self._session.add_all(new)
            self._session.commit()

        return new + existing
            

    def commit(self):
        self.open()
        self._session.commit()

    def update(self):
        self.open()
        self._session.flush()

    def close(self):
        if self._session:
            self._session.close()

if __name__=='__main__':
    raise Exception('Class intended for import only')
