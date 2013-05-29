import os
import sys
import datetime

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),os.path.pardir))

from common.database import mysql_engine as engine
from common.database import Base, Session_Maker, Scoped_Session
from common.database import Column, DateTime, ForeignKey, Integer, String

class Agent(Base):
    """
    the table store status information for agent.
    """
    __tablename__ = 'agent'
    
    id = Column(Integer,primary_key=True, autoincrement=True)
    update_time = Column(DateTime,default=datetime.datetime.now,
                         onupdate=datetime.datetime.now)
    node_id = Column(String(128), nullable=True)
    http_host = Column(String(1024), nullable=True)
    tcp_host = Column(String(1024), nullable=True)
    sys_uptime = Column(Integer(32), nullable=True)
    self_uptime = Column(Integer(32), nullable=True)
    cpu_load_avg = Column(String(128), nullable=True)
    
    def __init__(self, node_id=None, http_host=None, tcp_host=None,
                 sys_uptime=None,self_uptime=None,cpu_load_avg=None):
        self.node_id = node_id
        self.http_host = http_host
        self.tcp_host = tcp_host
        self.sys_uptime = sys_uptime
        self.self_uptime = self_uptime
        self.cpu_load_avg = cpu_load_avg
    
    def __repr__(self):
        return "<Agent('%s','%d','%d','%s')>" % (self.node_id,self.sys_uptime,
                                                 self.self_uptime,self.cpu_load_avg)

agent_table = Agent.__table__

MetaData = Base.metadata
# in generaly Scoped_Session is for thread safety
# or you can just use Session_Maker only
Session = Scoped_Session(Session_Maker(bind=engine,autoflush=True)) 

if __name__ == '__main__':
    MetaData.create_all(engine)

