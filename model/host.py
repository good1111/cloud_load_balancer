import os
import sys
import datetime

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),os.path.pardir))

from common.database import mysql_engine as engine
from common.database import Base, Session_Maker, Scoped_Session
from common.database import Column, DateTime, ForeignKey, Integer, String

class Host(Base):
    """
    store loadbalance host information, logging informations is the main purpose.
    """
    __tablename__ = "host"
    
    id = Column(Integer,primary_key=True, autoincrement=True)
    agent = Column(String(128), nullable=True)
    created_time = Column(DateTime, default=datetime.datetime.now,
                          onupdate=datetime.datetime.now)
    lb_type = Column(String(128), nullable=True)
    server_name_port = Column(String(128), nullable=True)
    upstream = Column(String(1024), nullable=True)
    distribute = Column(String(128), nullable=True)
    
    def __init__(self, agent=None, lb_type=None, server_name_port=None,
                 upstream=None,distribute=None):
        self.agent = agent
        self.lb_type = lb_type
        self.server_name_port = server_name_port
        self.upstream = upstream
        self.distribute = distribute
    
    def __repr__(self):
        return "<Host('%s','%s','%s','%s')>" % (self.server_name_port,self.upstream,
                                                self.distribute,self.agent)

host_table = Host.__table__ # the Table object of Host

MetaData = Base.metadata
# in generaly Scoped_Session is for thread safety
# or you can just use Session_Maker only
Session = Scoped_Session(Session_Maker(bind=engine,autoflush=True)) 


if __name__ == '__main__':
    # create the table
    MetaData.create_all(engine)

