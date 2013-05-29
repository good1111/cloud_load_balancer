import os
import sys
import datetime

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),os.path.pardir))

from common.database import mysql_engine as engine
from common.database import Base, Session_Maker, Scoped_Session
from common.database import Column, DateTime, ForeignKey, Integer, String

class DNSRecord(Base):
    """
    table used for bind8 dns server
    """
    __tablename__ = "dns_records"
    
    id = Column(Integer,primary_key=True, autoincrement=True)
    zone = Column(String(255), nullable=False)
    host = Column(String(255), nullable=False)
    type = Column(String(255), nullable=False)
    data = Column(String(255), nullable=False)
    
    ttl = Column(Integer(11), nullable=True)
    
    mx_priority = Column(String(255), nullable=True)
    
    refresh = Column(Integer(11), nullable=True)
    retry = Column(Integer(11), nullable=False)
    expire = Column(Integer(11), nullable=True)
    minimum = Column(Integer(11), nullable=True)
    
    serial = Column(Integer(32), nullable=True)
    
    resp_person = Column(String(255), nullable=True)
    primary_ns = Column(String(255), nullable=True)
    
    def __init__(self, zone=None, host=None, type=None, data=None, ttl=None,
                 mx_priority=None, refresh=None, retry=None, expire=None,
                 minimum=None,serial=None,resp_person=None,primary_ns=None):
        self.zone = zone
        self.host = host
        self.type = type
        self.data = data
        self.ttl = ttl
        self.mx_priority = mx_priority
        self.refresh = refresh
        self.retry = retry
        self.expire = expire
        self.minimum = minimum
        self.serial = serial
        self.resp_person = resp_person
        self.primary_ns = primary_ns

    def __repr__(self):
        return "<DNSRecord('%s','%s','%s','%s')>" % (self.zone,self.host,self.type,self.data)


dnsrecord_table = DNSRecord.__table__

MetaData = Base.metadata
# in generaly Scoped_Session is for thread safety
# or you can just use Session_Maker only
Session = Scoped_Session(Session_Maker(bind=engine,autoflush=True))

if __name__ == '__main__':
    MetaData.create_all(engine)
