#!/usr/local/cdat/bin/python

from socket import gethostname
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import func
from sqlalchemy import Column, Integer, String, create_engine, orm, ForeignKey, DateTime, Numeric
import hashlib
import utils
Base =  declarative_base()

class DAO(object):
    """This class provides some basic functionallity to all DAO classes."""

    def _setValues(self, dict, struct):
        """Set some values (normally attributes in columns) from a given utils.Struct."""

        #extract intersection of dictionary and current columns
        items = [ c.name for c in self.__table__.columns if c.name in dict]

        if values:
            #further specify the values (if not in items, it won't be even considered)
            items = [ i for i in items if i in values]

        #insert all the attributes as long as not in 'skip'
        for att in items:
            if att not in skip:
                self.__dict__[att] = dict[att]

    def __repr__(self):
        return "<" + self.__class__.__name__ + "(" + \
            ",".join(map(lambda k: k+"="+str(self.__dict__[k]), filter(lambda name: name[0] != '_', self.__dict__))) + \
            ")>"


class BenchmarkDAO(Base, DAO):
    __tablename__ = 'benchmark'
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.now)
    machine = Column(String, default=gethostname())
    inbound = Column(Numeric(precision=2))
    outbound = Column(Numeric(precision=2))

    def __init__(self, bench):
        self.inbound = bench.inbound
        self.outbound = bench.outbound

class BenchmarkDB(object):

    def __init__(self, db_url):

        self._db_url = db_url
        
        self._engine = create_engine(self._db_url)
        self._session = None

    def open(self):
        """Assure we have access to the DB and that it's properly created"""
        if not self._session:
            Base.metadata.create_all(self._engine)
            self._session = orm.scoped_session(orm.sessionmaker(self._engine, autoflush=False, autocommit=False, expire_on_commit=False))

    def add(self, benchmark):
        self.open()
        entry = BenchmarkDAO(benchmark)
        self._session.add(entry)
        self._session.commit()
        return entry

    def get(self, **filter):
        self.open()
        return self._session.query(ReplicaFileDAO).filter_by(**filter)
         
        
    def commit(self):
        self.open()
        self._session.commit()

    def close(self):
        if self._session:
            self._session.close()

if __name__=='__main__':
    import time
    print "Testing benchmark_db"

    db = BenchmarkDB('sqlite:///bench.db')
    bench = utils.Struct(inbound=1.23)
    e = db.add(bench)
    print e

    time.sleep(1)

    e = db.add(bench)
    print e

    db.close()  

