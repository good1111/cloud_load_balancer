import os
import sys
import datetime

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             os.path.pardir))

from common.database import mysql_engine as engine
from common.database import Base, Session_Maker, Scoped_Session
from common.database import Column, DateTime, ForeignKey, Integer, String


class LoadBalance(Base):
    """
    the main table that used for web user, who can init the loadbalance host.
    """
    __tablename__ = "loadbalance"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_name = Column(String(256), nullable=False)
    user_id = Column(Integer(32), nullable=False)
    uuid = Column(String(256), nullable=False)
    instance_name = Column(String(256), nullable=False)
    realserver = Column(String(4096), default="[]", nullable=True)
    created_time = Column(DateTime, default=datetime.datetime.now,
                          onupdate=datetime.datetime.now)
    
    def __int__(self, user_name=None, user_id=None,
                uuid=None, instance_name=None, realserver=None):
        self.user_name = user_name
        self.user_id = user_id
        self.uuid = uuid
        self.realserver = realserver
    
    def __repr__(self):
        return "<LoadBalance('%s','%s','%s')>" % (self.user_name,
                                                  self.uuid, self.realserver)


host_table = LoadBalance.__table__ # the Table object of Host

MetaData = Base.metadata
# in generaly Scoped_Session is for thread safety
# or you can just use Session_Maker only
Session = Scoped_Session(Session_Maker(bind=engine, autoflush=True)) 


if __name__ == '__main__':
    # create the table
    MetaData.create_all(engine)

    