import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),os.path.pardir))
import settings

from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine


mysql_engine = create_engine("mysql://%s:%s@%s:%s/%s" % (settings.mysql_username, settings.mysql_password,
                        settings.mysql_server, settings.mysql_port,
                        settings.mysql_database),
                        echo=settings.orm_debug,
                        convert_unicode=True, encoding='utf-8',
                        pool_size=settings.mysql_pool_size)

sqlite_engine = create_engine('sqlite:///%s' % settings.sqlite_database, echo=settings.orm_debug,
                              convert_unicode=True,encoding='utf-8')

Base = declarative_base()
Session_Maker = sessionmaker
Scoped_Session = scoped_session
