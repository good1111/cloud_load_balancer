import os
import sys
import datetime

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),os.path.pardir))

from common.database import mysql_engine as engine
from common.database import Base, Session_Maker, Scoped_Session
from common.database import Column, DateTime, ForeignKey, Integer, String

class UUID(Base):
    """
    uuid table, contains the loadbalance host for each loadbalance host.
    """
    __tablename__ = "uuid"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String(256), nullable=False)
    create_type = Column(String(128), nullable=False)
    lb_type = Column(String(32), nullable=False)
    src_port = Column(Integer(32), nullable=False)
    dst_port = Column(Integer(32), nullable=False)
    dns_name = Column(String(128), nullable=True)
    created_time = Column(DateTime, default=datetime.datetime.now,
                          onupdate=datetime.datetime.now)
    
    node_id = Column(String(128), nullable=True)
    result = Column(String(128), nullable=True)
    
    def __int__(self, uuid=None, create_type=None, lb_type=None, src_port=None,
                dst_port=None, dns_name=None):
        self.uuid = uuid
        self.create_type = create_type
        self.lb_type = lb_type
        self.src_port = src_port
        self.dst_port = dst_port
        self.dns_name = dns_name
    
    def __repr__(self):
        return "<UUID('%s','%s','%s','%s','%s','%s')>" % (self.uuid, self.create_type, self.lb_type,
                                                          self.src_port, self.dst_port, self.dns_name)


host_table = UUID.__table__ # the Table object of Host

MetaData = Base.metadata
# in generaly Scoped_Session is for thread safety
# or you can just use Session_Maker only
Session = Scoped_Session(Session_Maker(bind=engine,autoflush=True)) 


if __name__ == '__main__':
    # create the table
    MetaData.create_all(engine)