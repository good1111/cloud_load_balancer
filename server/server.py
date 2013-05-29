#!/usr/bin/env python
import sys
import os
import time
import threading
from threading import Lock
from threading import Condition
import uuid
import simplejson
import Queue
import shelve
import signal
import argparse

import pika
from pika.adapters import select_connection

import tornado.ioloop
import tornado.web
from tornado import template
from tornado.httpserver import HTTPServer

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             os.path.pardir))

from common.log import getlogger
from common.exceptions import *
from common.schduler import scheduler
from common.utils import gen_message_id
from common.queue import PulseQueue

###### import model ######
from model.host import Host
from model.host import Session as HostSession

from model.agent import Agent
from model.agent import Session as AgentSession

from model.dns import DNSRecord
from model.dns import Session as DNSSession

from instance import NovaInstance
from instance import instance_queue

from model.loadbalance import Session as LoadBalanceSession
from model.loadbalance import LoadBalance

from model.uuid import Session as UUIDSession
from model.uuid import UUID
###### end import ######

from common.utils import gen_random_dns_name

from settings import *

logger = getlogger("MasterServer")

lock = Lock()   # thread lock, used for singleton mode in threaded environment.
queue = PulseQueue()   # queue for send message
dns_queue = PulseQueue()   # queue for add domain name
node_map = {}           # node list used for web user
callback_map = {}       # callback map for tornado 
agent_max_host = 20     # max load balance host in agent
heartbeat_queue = Queue.Queue()   # deque for heartbeat message
host_queue = Queue.Queue()        # deque for host message
loadbalance_queue = Queue.Queue()    # loadbalnce instance queue
lbhost_queue = PulseQueue()         # lb host quuee

_DEBUG = False

if _DEBUG == True:
	import pdb
	pdb.set_trace()


class LoadBalanceHostORM(threading.Thread):
    def __init__(self, session, queue):
        super(LoadBalanceHostORM, self).__init__()
        self.daemon = True
        self.queue = queue
        self.session = session()
        self.query = self.session.query(UUID)
        self.callback_map = {}
    
    def run(self):
        while 1:
            record, callback_obj, original_msg_id = self.queue.get()
            if callback_obj:
                self.callback_map[original_msg_id] = callback_obj
            
            command = record['head']
            body = record['body']
            
            if command == "add":
                if not self.exists(body):
                    ret, msg_id = self.insert(body)
                else:
                    ret = "record_exists"
                    msg_id = body['msg_id']
            elif command == "del":
                if self.exists(body):
                    ret, msg_id = self.delete(body)
                else:
                    ret = "not_exists"
                    msg_id = body['msg_id']
            
            if msg_id in self.callback_map:
                obj = self.callback_map.pop(msg_id)
                if not obj.request.connection.stream.closed():
                    obj.write({'return': ret})
                    tornado.ioloop.IOLoop.instance().add_callback(obj.on_write)

    def exists(self, body):
        uuid = body.get('uuid', None)
        lb_type = body.get('lb_type', None)
        src_port = body.get('src_port', None)
        dst_port = body.get('dst_port', None)
        dns_name = body.get('dns_name', None)
        if lb_type == "http":
            count = self.query.filter_by(uuid=uuid).filter_by(src_port=src_port). \
                                filter_by(dst_port=dst_port).filter_by(dns_name=dns_name).count()
        elif lb_type == "tcp":
            count = self.query.filter_by(uuid=uuid).filter_by(src_port=src_port). \
                                filter_by(dst_port=dst_port).count()
        
        if count > 0:
            return True
        else:
            return False
    
    def insert(self, body):
        self.extra_body(body)
        try:
            self.session.add(UUID(
                    uuid=self.uuid,
                    create_type=self.create_type,
                    lb_type=self.lb_type,
                    src_port=self.src_port,
                    dst_port=self.dst_port,
                    dns_name=self.dns_name
                ))
            self.session.commit()
        except Exception, e:
            logger.exception(e)
            self.session.rollback()
            return False, self.msg_id
        else:
            return True, self.msg_id
    
    def delete(self, body):
        self.extra_body(body)
        obj = self.query.filter_by(uuid=self.uuid).filter_by(src_port=self.src_port). \
                            filter_by(dst_port=self.dst_port).filter_by(dns_name=self.dns_name)
        if obj.count() > 0:
            try:
                obj.delete()
                self.session.commit()
            except Exception, e:
                logger.exception(e)
                self.session.rollback()
                return False, self.msg_id
            else:
                return True, self.msg_id
    
    def extra_body(self, body):
        self.msg_id = body.get('msg_id', None)
        self.uuid = body.get('uuid', None)
        self.create_type = body.get('create_type', None)
        self.lb_type = body.get('lb_type', None)
        self.src_port = body.get('src_port', None)
        self.dst_port = body.get('dst_port', None)
        self.dns_name = body.get('dns_name', None)


class LoadBalanceInstanceORM(threading.Thread):
    def __init__(self, session, queue):
        super(LoadBalanceInstanceORM, self).__init__()
        self.daemon = True
        self.queue = queue
        self.session = session()
        self.query = self.session.query(LoadBalance)
        self.callback_map = {}
    
    def run(self):
        while 1:
            record, callback_obj, original_msg_id = self.queue.get()
            if callback_obj:
                self.callback_map[original_msg_id] = callback_obj
            
            command = record['head']
            body = record['body']
            
            if command == "add":
                if not self.exists(body):
                    ret, msg_id = self.insert(body)
                else:
                    ret = "record_exists"
                    msg_id = body['msg_id']
            
            elif command == "del":
                if self.exists(body):
                    ret, msg_id = self.delete(body)
                else:
                    ret = "not_exists"
                    msg_id = body['msg_id']
            
            if msg_id in self.callback_map:
                obj = self.callback_map.pop(msg_id)
                if not obj.request.connection.stream.closed():
                    obj.write({'return': ret})
                    tornado.ioloop.IOLoop.instance().add_callback(obj.on_write)
    
    def exists(self, body):
        uuid = body.get('uuid', None)
        if not isinstance(uuid, str) and not isinstance(uuid, unicode):
            try:
                uuid = str(uuid)
            except Exception, e:
                logger.error("Loadbalance need a string based object")
        count = self.query.filter_by(uuid=uuid).count()
        if count > 0:
            return True
        else:
            return False
    
    def insert(self, body):
        self.extra_body(body)
        try:
            self.session.add(LoadBalance(
                user_name = self.user_name,
                user_id = self.user_id,
                uuid = self.uuid,
                instance_name = self.instance_name
            ))
            self.session.commit()
        except Exception, e:
            self.session.rollback()
            logger.exception(e)
            return False, self.msg_id
        else:
            return True, self.msg_id
    
    def delete(self, body):
        self.extra_body(body)
        if not uuid:
            raise RuntimeError("Loadbalance need a string based object")
        obj = self.query.filter_by(uuid=self.uuid)
        try:
            if obj.count() > 0:
                obj.delete()
                self.session.commit()
        except Exception, e:
            self.session.rollback()
            logger.exception(e)
            return False, self.msg_id
        else:
            return True, self.msg_id
    
    def extra_body(self, body):
        self.msg_id = body.get('msg_id', None)
        self.user_name = body.get('user_name', None)
        self.user_id = body.get('user_id', None)
        self.uuid = body.get('uuid', None)
        self.instance_name = body.get('instance_name', None)


class DNSServer(threading.Thread):
    def __init__(self, session, queue):
        super(DNSServer, self).__init__()
        self.daemon = True
        self.queue = queue
        self.session = session()
        self.query = self.session.query(DNSRecord)
        self.callback_map = {}
    
    def run(self):
        while 1:
            record, callback_obj, original_msg_id = self.queue.get()
            if callback_obj:
                self.callback_map[original_msg_id] = callback_obj
            
            command = record['head']
            body = record['body']
            
            if command == "add":
                if not self.exists(body):
                    ret, msg_id = self.insert(body)
                else:
                    ret = "record_exists"
                    msg_id = body['msg_id']
                    
            elif command == "update":
                if self.exists(body):
                    ret, msg_id = self.update(body)
                else:
                    ret = "record_not_exists"
                    msg_id = body['msg_id']
                    
            elif command == "delete":
                ret, msg_id = self.delete(body)
                
            if msg_id in self.callback_map:
                obj = self.callback_map.pop(msg_id)
                invoke_type = getattr(obj, "invoke_type", None)
                if not invoke_type:
                    if not obj.request.connection.stream.closed():
                        obj.write(simplejson.dumps({'return': ret}))
                        tornado.ioloop.IOLoop.instance().add_callback(obj.on_write)
                elif invoke_type == "add_dns":
                    obj.delete_host(ret)
    
    def insert(self, record_body):
        self.extra_body(record_body)
        try:
            self.session.add(DNSRecord(
                zone = self.zone,
                host = self.host,
                type = self.type,
                data = self.data,
                ttl = self.ttl,
                mx_priority = self.mx_priority,
                refresh = self.refresh,
                retry = self.retry,
                expire = self.expire,
                minimum = self.minimum,
                serial = self.serial,
                resp_person = self.resp_person,
                primary_ns = self.primary_ns
            ))
            self.session.commit()
        except Exception, e:
            logger.exception(e)
            self.session.rollback()
            return False, self.msg_id
        else:
            return True, self.msg_id
    
    def update(self, record_body):
        self.extra_body(record_body)
        try:
            obj = self.query.filter_by(zone=self.zone).filter_by(host=self.host).filter_by(type=self.type)
            if obj.count() > 0:
                obj.update({
                    DNSRecord.zone: self.zone,
                    DNSRecord.host: self.host,
                    DNSRecord.type: self.type,
                    DNSRecord.data: self.data,
                })
                self.session.commit()
            else:
                raise RuntimeError("can not found any record.")
        except Exception, e:
            logger.exception(e)
            self.session.rollback()
            return False, self.msg_id
        else:
            return True, self.msg_id
    
    def delete(self, body):
        self.extra_body(body)
        if not self.zone or not self.host or not self.type:
            raise RuntimeError('parameters missing or incorrect.')
        try:
            obj = self.query.filter_by(zone=self.zone).filter_by(host=self.host).filter_by(type=self.type)
            if obj.count() > 0:
                obj.delete()
                self.session.commit()
            else:
                raise RuntimeError('can not found any record.')
        except Exception, e:
            logger.exception(e)
            self.session.rollback()
            return False, self.msg_id
        else:
            return True, self.msg_id
    
    def exists(self, body):
        zone = body.get('zone', None)
        host = body.get('host', None)
        type = body.get('type', None)
        if not zone or not host or not type:
            raise RuntimeError('dns parameters missing or incorrect.')
        ret = self.query.filter_by(zone=zone).filter_by(host=host).filter_by(type=type).count()
        if ret:
            return True
        else:
            return False

    def extra_body(self, body):
        self.msg_id = body.get('msg_id')
        self.zone = body.get('zone',None)
        self.host = body.get('host',None)
        self.type = body.get('type',None)
        self.data = body.get('data',None)
        self.ttl = body.get('ttl',None)
        self.mx_priority = body.get('mx_priority',None)
        self.refresh = body.get('refresh',None)
        self.retry = body.get('retry',None)
        self.expire = body.get('expire',None)
        self.minimum = body.get('minimum',None)
        self.serial = body.get('serial',None)
        self.resp_person = body.get('resp_person',None)
        self.primary_ns = body.get('primary_ns',None)


class TemplateManage(object):
    """
    template manager, render template with variables.
    """
    
    def __init__(self, path):
        self.path = path
    
    @property
    def get_lbconf_path(self):
        """
        get the template dir.
        """
        return os.path.join(LOCAL_PATH,self.path)

    def get_template(self, template_name):
        """
        load the template with name.
        """
        loader = template.Loader(self.get_lbconf_path)
        t = loader.load(template_name)
        return t
    
    def render_http_lb(self,**kwargs):
        """
        render the http load balance template
        """
        server_name = kwargs.get('server_name',None)
        upstream_server = kwargs.get('upstream_server',None)
        distrubute = kwargs.get('distrubute',None)
        t = self.get_template(HTTP_LOADBALANCE_CONF)
        if server_name and upstream_server:
            return t.generate(server_name=server_name, upstream_server=upstream_server, distrubute=distrubute)
        else:
            return False
    
    def render_tcp_lb(self, **kwargs):
        """
        render the tcp load balance template.
        """
        server_port = kwargs.get('server_port',None)
        upstream_server = kwargs.get('upstream_server',None)
        distrubute = kwargs.get('distrubute',None)
        t = self.get_template(TCP_LOADBALANCE_CONF)
        if server_port and upstream_server:
            return t.generate(server_port=server_port, upstream_server=upstream_server, distrubute=distrubute)
        else:
            return False


class Heartbeat(threading.Thread):
    def __init__(self,session,queue):
        super(Heartbeat,self).__init__()
        self.daemon = True
        self.session = session()
        self.query = self.session.query(Agent)
        self.queue = queue
    
    def run(self):
        while 1:
            body = self.queue.get()
            node_id = body['node_id']
            if self.exists(node_id):
                self.update(body)
            else:
                self.insert(body)
    
    def exists(self,node_id):
        if not isinstance(node_id,str) or isinstance(node_id,unicode):
            raise TypeError('The type of node_id was error, must be string or unicode.')
        if self.query.filter_by(node_id=node_id).count():
            return True
        else:
            return False
    
    def insert(self,body):
        self.extra_body(body)
        try:
            self.session.add(Agent(
                node_id = self.node_id,
                http_host = simplejson.dumps(self.http_host),
                tcp_host = simplejson.dumps(self.tcp_host),
                sys_uptime = self.sys_uptime,
                self_uptime = self.self_uptime,
                cpu_load_avg = simplejson.dumps(self.cpu_load_avg)
                ))
            self.session.commit()
        except Exception,e:
            logger.exception(e)
            self.session.rollback()
    
    def update(self,body):
        self.extra_body(body)
        try:
            query_obj = self.query.filter_by(node_id=self.node_id)
            query_obj.update({
                    Agent.node_id:self.node_id,
                    Agent.http_host:simplejson.dumps(self.http_host),
                    Agent.tcp_host:simplejson.dumps(self.tcp_host),
                    Agent.sys_uptime:self.sys_uptime,
                    Agent.self_uptime:self.self_uptime,
                    Agent.cpu_load_avg:simplejson.dumps(self.cpu_load_avg)
            })
            self.session.commit()
        except Exception,e:
            logger.exception(e)
            self.session.rollback()
    
    def extra_body(self,body):
        self.node_id = body['node_id']
        self.http_host = body['host_list']['http_host']
        self.tcp_host = body['host_list']['tcp_host']
        self.sys_uptime = body['sysinfo']['sys_uptime']
        self.self_uptime = body['sysinfo']['self_uptime']
        self.cpu_load_avg = body['sysinfo']['load_avg']


class LBHost(threading.Thread):
    def __init__(self,session,queue):
        super(LBHost,self).__init__()
        self.daemon = True
        self.session = session()
        self.query = self.session.query(Host)
        self.queue = queue
    
    def run(self):
        while 1:
            body = self.queue.get()
            command = body['command']
            failed = body['failed']
            content = body['content']
            lb_type = content['host_type']
            
            # if command is "addhost" and the result is ok
            if command == 'addhost' and failed == False:
                if lb_type == 'http':
                    parameter = content['server_name']
                elif lb_type == 'tcp':
                    parameter = content['server_port']
                if not self.exists(parameter):
                    self.insert(body)
                
            # if command is "delhost" and the result is ok        
            elif command == 'delhost' and failed == False:
                parameter = content['server']
                if self.exists(parameter):
                    self.delete(body)
    
    def exists(self,name_or_port):
        """
        check if the record is exists.
        """
        if isinstance(name_or_port,int) or isinstance(name_or_port,long):
            name_or_port = str(name_or_port)
        if not (isinstance(name_or_port,str) or isinstance(name_or_port,unicode)):
            raise TypeError('The type of node_id was error, must be string or unicode.')
        
        if self.query.filter_by(server_name_port=name_or_port).count():
            return True
        else:
            return False
    
    def insert(self,body):
        self.extra_body(body)
        try:
            self.session.add(Host(
                agent = self.agent,
                lb_type = self.lb_type,
                server_name_port = self.server_name_port,
                upstream = simplejson.dumps(self.upstream),
                distribute = self.distribute
            ))
            self.session.commit()
        except Exception,e:
            logger.exception(e)
            self.session.rollback()
    
    def delete(self,body):
        server_name_port = body['content']['server']
        try:
            self.query.filter_by(server_name_port=server_name_port).delete()
            self.session.commit()
        except Exception,e:
            logger.exception(e)
            self.session.rollback()
    
    def extra_body(self,body):
        self.agent = body['return_node_id']
        content = body['content']
        self.lb_type = content['host_type']
        if self.lb_type == 'tcp':
            self.server_name_port = content['server_port']
        elif self.lb_type == 'http':
            self.server_name_port = content['server_name']
        self.upstream = content['original']['upstream_server']
        self.distribute = content['original']['distrubute']


class AdminSender(threading.Thread):
    def __init__(self,queue):
        super(AdminSender,self).__init__()
        self.daemon = True
        self.queue = queue
        self.callback_map = callback_map
        self.connect()
    
    def connect(self):
        parameters = pika.ConnectionParameters(virtual_host=virtual_host,
                        credentials=pika.PlainCredentials(username,password),frame_max=frame_max_size,
                        host=rabbitmq_server)
        
        select_connection.POLLER_TYPE = 'epoll'
        self.connection_agent = select_connection.SelectConnection(parameters=parameters, on_open_callback=self.on_connected)
    
    def run(self):
        self.connection_agent.ioloop.start()
    
    def on_connected(self,connection):
        connection.set_backpressure_multiplier(20000)
        connection.channel(self.on_channel_open)
    
    def on_channel_open(self,channel):
        self.channel_agent = channel
        self.channel_agent.exchange_declare(exchange='loadbalance.agent',
                                 type='fanout',durable=True,
                                 callback=self.on_queue_declared)
    
    def on_queue_declared(self,frame):
        while 1:
            message,obj,msg_id = self.queue.get()
            
            if obj:    
                self.callback_map[msg_id] = obj
                
            self.channel_agent.basic_publish(exchange='loadbalance.agent',
                              routing_key='',
                              body=simplejson.dumps(message),
                              properties=pika.BasicProperties(
                              delivery_mode = 1 # make message nonpersistent
                        ))
                
            #logger.debug("send: %s" % msg_id)


class MonitorReceiver(threading.Thread):
    """
    class that receive alert from cloud monitor.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """singleton model"""
        if not cls._instance:
            MonitorReceiver._lock.acquire()
            cls._instance = super(MonitorReceiver, cls).__new__(
                cls, *args, **kwargs)
            MonitorReceiver._lock.release()
        return cls._instance
    
    def __init__(self, queue):
        super(MonitorReceiver, self).__init__()
        self.daemon = True
        self.instance_queue = queue
        self.connect()
    
    def connect(self):
        """
        connect to rabbitmq server and declare exchange and queue.
        """
        parameters = pika.ConnectionParameters(virtual_host=virtual_host,
                        credentials=pika.PlainCredentials(username,password),
                        frame_max=frame_max_size,
                        host=rabbitmq_server)
            
        select_connection.POLLER_TYPE = 'epoll'
        self.connection_monitor = select_connection.SelectConnection(parameters=parameters,
                                                        on_open_callback=self.on_connected)
        
    def run(self):
        """
        start event loop, and wait for message from monitor.
        """
        self.connection_monitor.ioloop.start()
    
    def on_connected(self, connection):
        connection.channel(self.on_channel_open)
    
    def on_channel_open(self, _channel):
        self.channel_monitor = _channel
        self.channel_monitor.exchange_declare(exchange='loadbalance.monitor',
                                              type='fanout', durable=True,
                                              callback=self.on_exchange_declared)

    def on_exchange_declared(self, _exchange):
        self.channel_monitor.queue_declare(durable=False, exclusive=True,
                                           callback=self.on_queue_declared)
    
    def on_queue_declared(self, _result):
        self.queue_name = _result.method.queue
        self.channel_monitor.queue_bind(exchange='loadbalance.monitor',
                                        queue=self.queue_name,
                                        callback=self.on_queue_bind)
    
    def on_queue_bind(self, _frame):
        self.channel_monitor.basic_consume(self.handle_delivery,
                                           queue=self.queue_name)
    
    def handle_delivery(self, ch, method, header, body):
        logger.debug("receive alert from cloud monitor: %s" % body)
        instance_queue.put_nowait(body)


class AdminReceiver(threading.Thread):
    """
    main class of server, singleton model.
    """
    
    _instance = None
    _lock = lock or threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """
        singleton model
        """
        AdminReceiver._lock.acquire()
        if not cls._instance:
            cls._instance = super(AdminReceiver,cls).__new__(
                cls, *args, **kwargs)
        AdminReceiver._lock.release()
        return cls._instance
        
    def __init__(self):
        super(AdminReceiver,self).__init__()
        self.daemon = True
        self.node_map = node_map
        self.callback_map = callback_map
        self.connect()
        
    def connect(self):
        """
        connect to rabbitmq server and declare exchange and queue.
        """
        parameters = pika.ConnectionParameters(virtual_host=virtual_host,
                        credentials=pika.PlainCredentials(username,password),
                        frame_max=frame_max_size,
                        host=rabbitmq_server)
            
        select_connection.POLLER_TYPE = 'epoll'
        self.connection_server = select_connection.SelectConnection(parameters=parameters,
                                                        on_open_callback=self.on_connected)

    def run(self):
        """
        start event loop, and wait for message from agent.
        """
        self.connection_server.ioloop.start()
        
    def on_connected(self,connection):
        connection.channel(self.on_channel_open)

    def on_channel_open(self,channel_):
        self.channel_server = channel_
        self.channel_server.exchange_declare(exchange='loadbalance.server',
                                             type='fanout',durable=True,
                                             callback=self.on_exchange_declared)

    def on_exchange_declared(self,exchange_):
        self.channel_server.queue_declare(durable=False, exclusive=True,
                                          callback=self.on_queue_declared)
        
    def on_queue_declared(self,result):
        self.queue_name = result.method.queue
        self.channel_server.queue_bind(exchange='loadbalance.server',
                                       queue=self.queue_name,
                                    callback=self.on_queue_bind)
        
    def on_queue_bind(self,frame):
        self.channel_server.basic_consume(self.handle_delivery,
                      queue=self.queue_name)
        
    def handle_delivery(self, ch, method, header, body):
        """
        callback function that will receive return message from remote agent.
        """
        body = simplejson.loads(body)
        message_type = body['message_type']
        
        if message_type == "heartbeat":
            global NodeList_Thread
            NodeList_Thread.process_online(body)
            heartbeat_queue.put(body)
            
        if message_type == "work_report":
            msg_id = body['message_id']
            host_queue.put_nowait(body)
            
            #logger.debug("receive: %s" % body)
            
            if msg_id in self.callback_map:
                obj = self.callback_map.pop(msg_id)
                invoke_type = getattr(obj,'invoke_type',None)
                if not invoke_type:
                    if not obj.request.connection.stream.closed():
                        obj.write(body)
                        tornado.ioloop.IOLoop.instance().add_callback(obj.on_write)
                elif invoke_type == "delete_host":
                    obj.add_host(body)
                elif invoke_type == "add_host":
                    obj.update_lbhost(body)
                
        ch.basic_ack(delivery_tag = method.delivery_tag)
        
    @property
    def get_node_map(self):
        """
        return nodes in a list.
        """
        nodelist = []
        for node_id,content in self.node_map.items():
            temp = {}
            temp['node_id'] = node_id
            temp['sysinfo'] = content['body']['sysinfo']
            temp['host_amount'] = content['body']['host_amount']
            temp['host_list'] = content['body']['host_list']
            nodelist.append(temp)
        return nodelist
        
    @property
    def get_node_ids(self):
        """
        return nodes with only its ids.
        """
        return [node_id for node_id,content in self.node_map.items()]
    
    @property
    def get_host_list(self):
        """
        return host list in agent.
        """
        hostlist = []
        for node_id,content in self.node_map.items():
            temp = {}
            temp[node_id] = content['body']['host_list']
            hostlist.append(temp)
        return hostlist


class CacheNodeList(threading.Thread):
    """
    thread used to cache node list, check if node online or offline.
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """
        singleton model
        """
        CacheNodeList._lock.acquire()
        if not cls._instance:
            cls._instance = super(CacheNodeList,cls).__new__(
                cls, *args, **kwargs)
        CacheNodeList._lock.release()
        return cls._instance
        
    def __init__(self, node_map):
        super(CacheNodeList,self).__init__()
        self.daemon = True
        self.node_map = node_map
        self.node_max_wait = 3
    
    def run(self):
        """
        cache a node list and update them immediately if node has changed.
        """
        while 1:
            for k,v in self.node_map.items():
                if int(time.time() - float(v['time'])) >= self.node_max_wait:
                    self.node_map.pop(k)
                    logger.debug("node %s has offline..." % k)
                else:
                    self.node_map[self.node_id]['time'] = self.ctime
                    self.node_map[self.node_id]['body'] = self.body
            time.sleep(0.01)
    
    def process_online(self,body):
        """
        receive message if that message type is heartbeat, and update node list.
        and update some variables at the same time.
        """
        node_id = body['node_id']
        ctime = body['time']
        
        self.node_id = node_id
        self.ctime = ctime
        self.body = body
                
        if node_id not in self.node_map:
            self.node_map[node_id] = {}
            self.node_map[node_id]['time'] = ctime
            self.node_map[node_id]['body'] = body
            logger.debug("node %s online now..." % node_id)


class RandomDnsName(object):
    """
    get random dns name from shelve object database.
    """
    
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            return super(RandomDnsName, cls).__new__(cls, *args, **kwargs)
        return cls._instance
    
    def __init__(self):
        super(RandomDnsName, self).__init__()
        self.daemon = True
        self.run()
        
    def run(self):
        self.db = shelve.open(DNS_NAME_POOL, flag='c', writeback=True)
        self.record = self.db['record']
    
    def get_name(self, callback=None):
        if self.record:
            name = self.record.pop()
        else:
            name = None
        self.db['record'] = self.record
        tornado.ioloop.IOLoop.instance().add_callback(callback)
        return str(name) + '.' + CLOUD_DOMAIN
    

def add_host_handler(lb_type=None, server_name=None, server_port=None,
                     upstream_server=None,node_id=None, distrubute=None):
    if lb_type == 'http':
            t = TemplateManage(TEMPLATE_PATH)
            kwargs = {
                'server_name': server_name,
                'upstream_server': upstream_server,
                'distrubute': distrubute,
            }
            
            content = {
                'server':t.render_http_lb(**kwargs),
                'server_name':server_name,
                'host_type':lb_type,
                'original':kwargs
            }
            
            msg_id = gen_message_id()
            imessage = {
                'message_id': msg_id,
                'task_node_id': node_id,
                'message_type': 'task',
                'command': 'addhost',
                'content': content
            }
            
    if lb_type == 'tcp':
            t = TemplateManage(TEMPLATE_PATH)
            kwargs = {
                'server_port': server_port,
                'upstream_server': upstream_server,
                'distrubute': distrubute,
            }
            
            content = {
                'server': t.render_tcp_lb(**kwargs),
                'server_port': server_port,
                'host_type': lb_type,
                'original': kwargs
            }
            
            msg_id = gen_message_id()
            imessage = {
                'message_id': msg_id,
                'task_node_id': node_id,
                'message_type': 'task',
                'command': 'addhost',
                'content': content
            }
            
    return imessage, msg_id


class AddHostHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        #message = self.get_argument("message",None)
        lb_type = 'tcp'
        
        if lb_type == 'http':
            t = TemplateManage(TEMPLATE_PATH)
            server_name = 'www.test.com'
            upstream_server = [{'127.0.0.1': 8888},{'127.0.0.1': 8889}]
            distrubute = 'ip_hash'
            kwargs = {
                'server_name': server_name,
                'upstream_server': upstream_server,
                'distrubute': distrubute,
            }
            
            message = {
                'server': t.render_http_lb(**kwargs),
                'server_name': server_name,
                'host_type': lb_type,
                'original': kwargs
            }
            
            msg_id = gen_message_id()
            node_id = 'Sunny'
            imessage = {
                'message_id':msg_id,
                'task_node_id':node_id,
                'message_type':'task',
                'command':'addhost',
                'content':message
            }
            
        if lb_type == 'tcp':
            t = TemplateManage(TEMPLATE_PATH)
            server_port = 81
            upstream_server = [{'127.0.0.1':8888},{'127.0.0.1':8889}]
            distrubute = 'ip_hash'
            kwargs = {
                'server_port':server_port,
                'upstream_server':upstream_server,
                'distrubute' : distrubute,
            }
            
            message = {
                'server':t.render_tcp_lb(**kwargs),
                'server_port':server_port,
                'host_type':lb_type,
                'original':kwargs
            }
            
            msg_id = gen_message_id()
            
            node_id = scheduler(AdminReceiver_Thread,agent_max_host)
            if not node_id:
                self.on_node_empty()
                
            imessage = {
                'message_id':msg_id,
                'task_node_id':node_id,
                'message_type':'task',
                'command':'addhost',
                'content':message
            }
            
        queue.put_nowait((imessage,self,msg_id))
        
    def on_write(self):
        return self.finish()
    
    def on_node_empty(self):
        self.write({'error':'node_map_empty'})
        return self.finish()


def delete_host_handler(lb_type=None, server_name=None,
                        server_port=None, node_id=None):
    if lb_type == 'http':
        message = {
            'host_type':lb_type,
            'server':server_name
        }
    elif lb_type == 'tcp':
        message = {
            'host_type':lb_type,
            'server':server_port
        }
    msg_id = gen_message_id()
    imessage = {
        'message_id':msg_id,
        'task_node_id':node_id,
        'message_type':'task',
        'command':'delhost',
        'content':message
    }
    return imessage, msg_id


class DelHostHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        #message = self.get_argument("message",None)
        
        host_type = 'tcp'
        #message = {
        #    'host_type':host_type,
        #    'server':'www.test.com'
        #}
        message = {
            'host_type':host_type,
            'server':81
        }
        
        msg_id = gen_message_id()
        node_id = 'Sunny'
        imessage = {
            'message_id':msg_id,
            'task_node_id':node_id,
            'message_type':'task',
            'command':'delhost',
            'content':message
        }
        
        queue.put_nowait((imessage, self, msg_id))
        
    def on_write(self):
        return self.finish()


class GetNodeList(tornado.web.RequestHandler):
    def get(self):
        nodelist = AdminReceiver_Thread.get_node_map
        self.write(simplejson.dumps(nodelist))


class GetNodeIDs(tornado.web.RequestHandler):
    def get(self):
        nodeids = AdminReceiver_Thread.get_node_ids
        self.write(simplejson.dumps(nodeids))


class RestartNode(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        msg_id = gen_message_id()
        node_id = 'Sunny'
        imessage = {
            'task_node_id':node_id,
            'message_id':msg_id,
            'message_type':'task',
            'command':'restart',
        }
        
        queue.put_nowait((imessage,self,msg_id))
        
    def on_write(self):
        return self.finish()


class ListHostHandler(tornado.web.RequestHandler):
    def get(self):
        hostlist = AdminReceiver_Thread.get_host_list
        self.write(simplejson.dumps(hostlist))


class HostStateHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        pass


class DNSAdd(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        msg_id = gen_message_id()
        body = {
            'zone':self.get_argument('zone'),
            'host':self.get_argument('host'),
            'type':self.get_argument('type'),
            'data':self.get_argument('data'),
            'ttl':self.get_argument('ttl',None),
            'mx_priority':self.get_argument('mx_priority',None),
            'refresh':self.get_argument('refresh',None),
            'retry':self.get_argument('retry',13),
            'expire':self.get_argument('expire',None),
            'minimum':self.get_argument('minimum',None),
            'serial':self.get_argument('serial',None),
            'resp_person':self.get_argument('resp_person',None),
            'primary_ns':self.get_argument('primary_ns',None),
            'msg_id':msg_id
        }
        dns_queue.put((
            {'head':'add','body':body},
            self,
            msg_id
            ))
    
    def on_write(self):
        return self.finish()


class DNSUpdate(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        msg_id = gen_message_id()
        body = {
            'zone':self.get_argument('zone'),
            'host':self.get_argument('host'),
            'type':self.get_argument('type'),
            'data':self.get_argument('data'),
            'msg_id':msg_id
        }
        dns_queue.put((
            {'head':'update','body':body},
            self,
            msg_id
            ))
    
    def on_write(self):
        return self.finish()


class DNSDel(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        msg_id = gen_message_id()
        body = {
            'zone':self.get_argument('zone'),
            'host':self.get_argument('host'),
            'type':self.get_argument('type'),
            'msg_id': msg_id
        }
        dns_queue.put((
            {'head':'delete','body':body},
            self,
            msg_id
            ))
    
    def on_write(self):
        return self.finish()


class ListInstanceUUID(tornado.web.RequestHandler):
    """ Just return the uuid of all instance in a list. """
    def get(self):
        lb_session = LoadBalanceSession()
        lb_query =lb_session.query(LoadBalance.uuid)
        ret = lb_query.all()
        instance_list = []
        for line in ret:
            instance_list.append(line.uuid)
        self.write(simplejson.dumps(instance_list))


class ListInstance(tornado.web.RequestHandler):
    def post(self):
        user_name = self.get_argument("user_name", default='ALL')
        uuid_session = UUIDSession()
        uuid_query = uuid_session.query(UUID)
        
        def get_host(uuid):
            temp_host = {}
            temp_host['http'] = []
            temp_host['tcp'] = []
            ret = uuid_query.filter_by(uuid=uuid)
            if ret.count() == 0:
                return temp_host
            for line in ret:
                if line.lb_type == "http":
                    temp_host['http'].append(line.dns_name)
                elif line.lb_type == "tcp":
                    temp_host['tcp'].append(line.src_port)
            return temp_host
        
        lb_session = LoadBalanceSession()
        lb_query = lb_session.query(LoadBalance)
        if user_name != "ALL":
            ret = lb_query.filter_by(user_name=user_name).all()
        else:
            ret = lb_query.all()
        main_content = []
        content = {}
        if len(ret) > 0:
            for line in ret:
                temp = {}
                temp['instance_name'] = line.instance_name
                temp['uuid'] = line.uuid
                temp['created_time'] = str(line.created_time)
                temp['realserver'] = line.realserver
                temp['host'] = get_host(line.uuid)
                main_content.append(temp)
        
            content['return'] = True
        else:
            content['return'] = "empty"
            
        content['content'] = main_content
        self.set_header('Content-Type','application/json')
        self.write(simplejson.dumps(content))


class GetRandomName(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        name = self.application.getname_handler.get_name(callback=self.on_write)
        self.write(name)
    
    def on_write(self):
        self.application.getname_handler.db.sync()
        self.finish()


class CreateInstance(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def post(self):
        message_id = gen_message_id()
        body = {
        'user_name': self.get_argument('user_name', default=None),
        'user_id': self.get_argument('user_id', default=None),
        'uuid': self.get_argument('uuid', default=None),
        'instance_name': self.get_argument('instance_name', default=None),
        'msg_id': message_id
        }
        loadbalance_queue.put((
            {'head': 'add', 'body': body},
            self,
            message_id
        ))
        
    def on_write(self):
        self.finish()


class DeleteInstance(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def post(self):
        message_id = gen_message_id()
        body = {
            'uuid': self.get_argument('uuid', default=None),
            'msg_id': message_id
        }
        loadbalance_queue.put((
            {'head': 'del', 'body': body},
            self,
            message_id
        ))
    
    def on_write(self):
        self.finish()


class ListInstanceHost(tornado.web.RequestHandler):
    def post(self):
        instance_id = self.get_argument('instance_id', None)
        session = UUIDSession()
        query = session.query(UUID)
        if instance_id:
            ret = query.filter_by(uuid=instance_id)
        else:
            self.write(simplejson.dumps({'return': False, 'content': 'invalid instance id'}))
            return
        if ret.count() == 0:
            self.write(simplejson.dumps({'return': False, 'content': 'empty'}))
            return
        
        ret = ret.all()
        host_list = []
        for line in ret:
            temp = {}
            temp['uuid'] = line.uuid
            temp['create_type'] = line.create_type
            temp['lb_type'] = line.lb_type
            temp['src_port'] = line.src_port
            temp['dst_port'] = line.dst_port
            temp['domain_name'] = line.dns_name
            temp['created_time'] = str(line.created_time)
            temp['node_id'] = line.node_id
            temp['result'] = line.result
            host_list.append(temp)
        self.write(simplejson.dumps({'return': True, 'content':host_list}))
        self.finish()


class CreateInstanceHost(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def post(self):
        message_id = gen_message_id()
        body = {
            'uuid': self.get_argument('uuid', default=None),
            'create_type': self.get_argument('create_type', default=None),
            'lb_type': self.get_argument('lb_type', None),
            'src_port': self.get_argument('src_port', default=True),
            'dst_port': self.get_argument('dst_port', default=None),
            'dns_name': self.get_argument('dns_name', default=None),
            'msg_id': message_id
        }
        lbhost_queue.put((
            {'head': 'add', 'body': body},
            self,
            message_id
           ))
        
    def on_write(self):
        self.finish()
        

class DeleteInstanceHost(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def post(self):
        message_id = gen_message_id()
        body = {
            'uuid': self.get_argument('uuid', default=None),
            'lb_type': self.get_argument('lb_type', None),
            'src_port': self.get_argument('src_port', default=True),
            'dst_port': self.get_argument('dst_port', default=None),
            'dns_name': self.get_argument('dns_name', default=None),
            'msg_id': message_id
        }
        lbhost_queue.put((
            {'head': 'del', 'body': body},
            self,
            message_id
           ))
        
    def on_write(self):
        self.finish()
   

class InstanceHostTCPPort(tornado.web.RequestHandler):
    def post(self):
        port = self.get_argument('port', default=None)
        try:
            tcp_port = int(port)
        except Exception, e:
            logger.exception(e)
            self.write(simplejson.dumps({'return': 'wrong port number'}))
            raise e
        session = UUIDSession()
        ports = session.query(UUID.src_port).filter_by(lb_type="tcp", src_port=tcp_port).all()
        if ports:
            self.write(simplejson.dumps({'return': True, 'content': 'exists'}))
            return
        self.write(simplejson.dumps({'return': True, 'content': 'ok'}))


settings = {'debug':True,
            'port':8888}

application = tornado.web.Application([
        # for loadbalance host.
        (r'/api/host/add', AddHostHandler),
        (r'/api/host/del', DelHostHandler),
        (r'/api/host/list', ListHostHandler),
        (r'/api/host', HostStateHandler),
        # for loadbalance node.
        (r'/api/node/list', GetNodeList),
        (r'/api/node/id', GetNodeIDs),
        (r'/api/node/restart', RestartNode),
        # for dns server.
        (r'/api/dns/add', DNSAdd),
        (r'/api/dns/update', DNSUpdate),
        (r'/api/dns/del', DNSDel),
        # for cloud monitor
        (r'/api/sys/name', GetRandomName),
        # for instance
        (r'/api/instance', ListInstance),
        (r'/api/instance/uuid', ListInstanceUUID),
        (r'/api/instance/create', CreateInstance),
        (r'/api/instance/delete', DeleteInstance),
        # for instance host
        (r'/api/instance/host', ListInstanceHost),
        (r'/api/instance/host/create', CreateInstanceHost),
        (r'/api/instance/host/delete', DeleteInstanceHost),
        (r'/api/instance/host/port', InstanceHostTCPPort),
    ], **settings)


class Watcher:   
    """this class solves two problems with multithreaded  
    programs in Python, (1) a signal might be delivered  
    to any thread (which is just a malfeature) and (2) if  
    the thread that gets the signal is waiting, the signal  
    is ignored (which is a bug).  
 
    The watcher is a concurrent process (not thread) that  
    waits for a signal and the process that contains the  
    threads.  See Appendix A of The Little Book of Semaphores.  
    http://greenteapress.com/semaphores/  
 
    I have only tested this on Linux.  I would expect it to  
    work on the Macintosh and not work on Windows.  
    """  
  
    def __init__(self):   
        """ Creates a child thread, which returns.  The parent  
            thread waits for a KeyboardInterrupt and then kills  
            the child thread.  
        """  
        self.child = os.fork()   
        if self.child == 0:   
            return  
        else:   
            self.watch()
            
    def watch(self):   
        try:   
            os.wait()   
        except (KeyboardInterrupt, SystemExit):   
            # I put the capital B in KeyBoardInterrupt so I can   
            # tell when the Watcher gets the SIGINT
            logger.debug("server exit at %s" % time.asctime())
            self.kill()   
        sys.exit()   
  
    def kill(self):   
        try:   
            os.kill(self.child, signal.SIGKILL)   
        except OSError: pass 


#make current process to daemon
def daemonize (stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
    # Do first fork.
    try: 
        pid = os.fork() 
        if pid > 0:
            sys.exit(0)   # Exit first parent.
    except OSError, e: 
        sys.stderr.write ("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror) )
        sys.exit(1)

    # Decouple from parent environment.
    os.chdir("/") 
    os.umask(0) 
    os.setsid() 

    # Do second fork.
    try: 
        pid = os.fork() 
        if pid > 0:
            sys.exit(0)   # Exit second parent.
    except OSError, e: 
        sys.stderr.write ("fork #2 failed: (%d) %s\n" % (e.errno, e.strerror) )
        sys.exit(1)

    # Now I am a daemon!
    
    # Redirect standard file descriptors.
    si = open(stdin, 'r')
    so = open(stdout, 'a+')
    se = open(stderr, 'a+', 0)
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())


def main():
    parser = argparse.ArgumentParser(description="cloud loadbalance server")
    exclusive_group = parser.add_mutually_exclusive_group(required=False)
    exclusive_group.add_argument('-r', '--dryrun', action='store_true',
                            dest='dryrun', default=False, help='just dry run')
    exclusive_group.add_argument('-d', '--daemon', action='store_true',
                            dest='daemon', default=False, help='make this process go background.')
    
    sysargs = sys.argv[1:]
    args = parser.parse_args(args=sysargs)
    if len(sysargs) < 1:
        parser.print_help()
        sys.exit(1)
    else:
        if args.dryrun:
           Watcher()
        elif args.daemon:
           logger.debug("we will disappear from console :)")
           daemonize()
    
    # thread for store dns record to db
    DNSServer_Thread = DNSServer(DNSSession, dns_queue)
    DNSServer_Thread.start()
    
    # thread for store nginx loadbalance host record to db
    LBHost_Thread = LBHost(HostSession,host_queue)
    LBHost_Thread.start()
    
    # thread for store heartbeat record to db
    Heartbeat_Thread = Heartbeat(AgentSession,heartbeat_queue)
    Heartbeat_Thread.start()
    
    # thread for add or delete loadbalance instance in db
    LoadBalance_Thread = LoadBalanceInstanceORM(LoadBalanceSession, loadbalance_queue)
    LoadBalance_Thread.start()
    
    LoadBalanceHost_Thread = LoadBalanceHostORM(UUIDSession, lbhost_queue)
    LoadBalanceHost_Thread.start()
    
    # thread for maintain node list
    global NodeList_Thread
    NodeList_Thread = CacheNodeList(node_map)
    NodeList_Thread.start()
    
    # Main thread that send payload to agent
    AdminSender_Thread = AdminSender(queue)
    if not AdminSender_Thread.is_alive():
        AdminSender_Thread.start()
    
    # thread which receive message from agent
    AdminReceiver_Thread = AdminReceiver()
    if not AdminReceiver_Thread.is_alive():
        AdminReceiver_Thread.start()
    
    # thread which create nova instance and etc...
    InstanceThread = NovaInstance(instance_queue, queue, dns_queue,
                                  AdminReceiver_Thread,
                                  add_host_handler, delete_host_handler,
                                  LoadBalanceSession, UUIDSession, logger)
    InstanceThread.start()
    
    # thread which receive from cloud monitor, and put them into instance queue.
    MonitorThread = MonitorReceiver(instance_queue)
    MonitorThread.start()
    
    random_name_thread = RandomDnsName()
    
    application.getname_handler = random_name_thread
    
    port = settings.get('port', 8888)
    if settings.get('debug', None) == False:
        # run tornado in multiple processing mode.
        # and this can not run in debug mode.
        server = HTTPServer(application)
        server.bind(port)
        server.start(0)
    else:
        application.listen(port)
  
    tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
