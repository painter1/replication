#!/usr/local/cdat/bin/python

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy import Column, Float, String 
import logging as log
from utils_db import DAO, DB, Base
import socket, re


_GEO_IP_HOST='ipinfodb.com'
_GEO_IP_PORT=80
_GEO_IP_REQ='GET /ip_locator.php?ip={0} HTTP/1.1\nHost: ipinfodb.com\n\n'
_GEO_IP_MARK='IP address :'
_GEO_IP_REGEX=re.compile('Latitude : (-?[0-9]*\.[0-9]*)<.*\n.*Longitude : (-?[0-9]*\.[0-9]*)', re.MULTILINE)

def ipFromName(name):
    """return the ip to which this name resolves (only 1!)"""
    try:
        return socket.gethostbyname(name)
    except socket.gaierror:
        log.warn('IP not found for %s', name)
    return None


def latlonFromName(name):
    """Tries to gather the most approximate location of the machine named here (name might also be IP)"""
    log.debug('Gathering location for %s', name)
    lat = lon = None
    ip = ipFromName(name)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((_GEO_IP_HOST, _GEO_IP_PORT))
    s.sendall(_GEO_IP_REQ.format(name))
    part = s.recv(1024)


    while part and (part.find(_GEO_IP_MARK) == -1 or part.find(ip) == -1):
        part = s.recv(1024)
    if part:
        part = part + s.recv(1024)
        mo = _GEO_IP_REGEX.search(part)
        if mo:
            lat, lon = float(mo.group(1)), float(mo.group(2))
        else:
            log.error('Regex failed')
            print part
    else:
        log.error('text position not found')
        s.close()
    return lat, lon


class GeoDAO(Base, DAO):
    __tablename__ = 'geo'

    name = Column(String, primary_key=True)
    dns = Column(String)
    lat = Column(Float)
    lon = Column(Float)


    def __init__(self, **kwargs):
        Base.__init__(self, **kwargs)


    def getLocationFromMachine(self, machine=None):
        """Try to find the location from the name of a machine (DNS), if nothing
        is given, it will be assumed the current name is the name of the machine."""
        if not machine: machine = self.dns

        if not (self.lat and self.lon):
            self.lat, self.lon = latlonFromName(self.dns)

        return (self.lat, self.lon)


class GeoDB(DB):
    @staticmethod
    def __update_function(db, new):
        if not (db.lat and db.lon):
            log.debug('Updating %s with %s', repr(db), repr(new))
            if new.lat and new.lon:
                db.lat = new.lat
                db.lon = new.lon
        
    def addAll(self, geos):
        if not geos: return []
        
        self.open()
        clazz = geos[0].__class__

        results = self._session.query(clazz).filter(clazz.name.in_([ g.name for g in geos])).all()

        new = []
        existing = []

        in_session = self._add_all(geos, db_objects=results, new=new, existing=existing, update_function=GeoDB.__update_function )
        
        return new + existing

    
def __testData():
    geo =   [GeoDAO(name='albedo2.dkrz.de'),
            GeoDAO(name='www.mpimet.mpg.de')]
    return geo
    
if __name__=='__main__':
    #configure logging
    log.basicConfig(level=log.DEBUG)
    
#    db = GeoDB('sqlite:///geo.db')
#    import getpass
#    password = getpass.getpass('DB password?')
#    db = GeoDB('postgres://plone:%s@plone.dkrz.de/thredds'.format(password))
    db = GeoDB('postgres://plone@plone.dkrz.de/thredds')

    db.open()
    import sys
    args = sys.argv[1:]
    if args:
        data = []
        for name in args: data.append(GeoDAO(name=name))
        data = db.addAll(data)

        for g in data:
            if g.dns: g.getLocationFromMachine()
            print repr(g)
        db._session.commit()
    else:
        for geo in db._session.query(GeoDAO):
            if geo.dns and not geo.lon:
                geo.lat = geo.lon = None
                geo.getLocationFromMachine()
                print "changed!", repr(geo), geo.lon
            else:   print geo, geo.lon
        db.commit()

    
    
