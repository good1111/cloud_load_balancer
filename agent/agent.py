#!/usr/bin/env python
import os
import sys
import time
import threading
from threading import Lock
import simplejson
import decimal
import Queue
import signal
import argparse

import pika
from pika.adapters import select_connection

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),os.path.pardir))
from common.log import getlogger
from common.exceptions import *
from common.driver import NginxDriver

import settings

node_id = "Sunny"
logger = getlogger(node_id)


class AgentSender(threading.Thread):
    def __init__(self, queue):
        super(AgentSender,self).__init__()
        self.queue = queue
        self.connect()
    
    def connect(self):
        parameters = pika.ConnectionParameters(virtual_host=settings.virtual_host,
                        credentials=pika.PlainCredentials(settings.username,settings.password),
                        frame_max=settings.frame_max_size,
                        host=settings.rabbitmq_server)
        
        select_connection.POLLER_TYPE = 'epoll'
        self.connection_server = select_connection.SelectConnection(parameters=parameters, on_open_callback=self.on_connected)
    
    def run(self):
        self.connection_server.ioloop.start()
    
    def on_connected(self,connection):
        connection.channel(self.on_channel_open)
    
    def on_channel_open(self,channel):
        self.channel_server = channel
        self.channel_server.exchange_declare(exchange='loadbalance.server',type='fanout',durable=True,callback=self.on_queue_declared)
    
    def on_queue_declared(self,frame):
        while 1:
            message = self.queue.get()
            self.channel_server.basic_publish(exchange='loadbalance.server',
                          routing_key='',
                          body=message,
                          properties=pika.BasicProperties(
                         delivery_mode = 1, # make message nonpersistent
                      ))


class AgentReceiver(threading.Thread):
    def __init__(self,queue):
        super(AgentReceiver,self).__init__()
        self.queue = queue
        self.driver = NginxDriver(logger=logger)
        self.node_id = node_id
        self.connect()
    
    def connect(self):
        parameters = pika.ConnectionParameters(virtual_host=settings.virtual_host,
                        credentials=pika.PlainCredentials(settings.username,settings.password),
                        frame_max=settings.frame_max_size,
                        host=settings.rabbitmq_server)
        
        select_connection.POLLER_TYPE = 'epoll'
        self.connection_agent = select_connection.SelectConnection(parameters=parameters, on_open_callback=self.on_connected)
    
    def run(self):
        self.connection_agent.ioloop.start()
    
    def on_connected(self,connection):
        connection.channel(self.on_channel_open)
    
    def on_channel_open(self,channel):
        self.channel_agent = channel
        self.channel_agent.exchange_declare(exchange='loadbalance.agent',type='fanout',durable=True,
                                            callback=self.on_exchange_declared)
        
    def on_exchange_declared(self,exchange_):
        self.channel_agent.queue_declare(durable=False, exclusive=True, callback=self.on_queue_declared)
        
    def on_queue_declared(self,result):
        self.queue_name = result.method.queue
        self.channel_agent.queue_bind(exchange='loadbalance.agent',queue=self.queue_name,
                                      callback=self.on_queue_bind)
    
    def on_queue_bind(self,frame):
        self.channel_agent.basic_consume(self.handle_delivery,
                              queue=self.queue_name)
    
    def handle_delivery(self, ch, method, header, body):
        body = simplejson.loads(body)
        
        task_node_id = body['task_node_id'] # task's node id
        message_type = body['message_type'] # message type
        command = body['command']           # command
        
        msg_id = body['message_id']
        #logger.debug("agent got:%s" % msg_id)
        
        # task message
        if message_type == "task":
            if task_node_id == self.node_id:
                body['message_type'] = "work_report"
                
                if command == "addhost":
                    ret,why = self.driver.add_host(body)
                    # remove server content from original message
                    body['content'].pop('server')
                    if ret:
                        body['failed'] = False
                    else:
                        body['failed'] = True
                        body['why'] = why
                
                if command == "delhost":
                    ret,why = self.driver.delete_host(body)
                    if ret:
                        body['failed'] = False
                    else:
                        body['failed'] = True
                        body['why'] = why
                
                if command == 'restart':
                    ret,why = self.driver.restart_node()
                    if ret:
                        body['failed'] = False
                    else:
                        body['failed'] = True
                        body['why'] = why
                
        # broadcast message
        if message_type == "cast":
            if command == "host_amount":
                pass
        
        ch.basic_ack(delivery_tag = method.delivery_tag)
        
        body['return_node_id'] = self.node_id
        self.queue.put_nowait(simplejson.dumps(body))


class AgentHreatbeat(threading.Thread):
    """
    send hreatbeat message to server with system information and the information of agent ieself.
    """
    
    def __init__(self,queue):
        super(AgentHreatbeat,self).__init__()
        self.daemon = False
        self.start_time = time.time()
        self.queue = queue
    
    def run(self):
        """
        start the thead, the message will be send in every second.
        """
        while 1:
            try:
                http_host = self._get_host_amount(settings.http_conf_path)
                tcp_host = self._get_host_amount(settings.tcp_conf_path)
            except Exception,e:
                logger.exception(e)
                http_host_amount = tcp_host_amount = 0
                http_host_list = tcp_host_list = 0
            else:
                http_host_amount = http_host[0]
                http_host_list = http_host[1]
                tcp_host_amount = tcp_host[0]
                tcp_host_list = tcp_host[1]
                
            heartbeat = {
                'message_type':'heartbeat',
                'node_id':node_id,
                'time':time.time(),
                'sysinfo':self._getsysinfo,
                'host_amount':{
                    'http_host':http_host_amount,
                    'tcp_host':tcp_host_amount
                },
                'host_list':{
                    'http_host':http_host_list,
                    'tcp_host':tcp_host_list
                }
            }
            self.queue.put_nowait(simplejson.dumps(heartbeat))
            time.sleep(1)
   
    @property    
    def _getsysinfo(self):
        """
        get system information.
        """
        sysinfo = {
            'self_uptime':self._cal_uptime,
            'sys_uptime':self._get_sysuptime,
            'load_avg':os.getloadavg()
        }
        return sysinfo

    @property    
    def _cal_uptime(self):
        """
        get update time of the agent itself.
        """
        uptime = time.time() - self.start_time
        return int(uptime)
   
    @property    
    def _get_sysuptime(self):
        """
        get the system update time.
        """
        uptime_file = '/proc/uptime'
        try:
            fd = open(uptime_file,'r')
            up_seconds = int(decimal.Decimal((fd.read()).split()[0]))
        except Exception,e:
            logger.exception(e)
            return 0
        else:
            return up_seconds
    
    def _get_host_amount(self,conf_path):
        def listdir_onerror(error): # on error callback function, just raise the error
            raise error
        host_amount = 0
        host_list = []
        try:
            for root,dirs,files in os.walk(conf_path, onerror=listdir_onerror):  # the dirs will be an empty list, because the depth is 1.
                for file in files:
                    if os.path.isfile(os.path.join(root,file)):
                        host_amount += 1
                        host_list.append(file.split(settings.conf_suffix)[0])
        except Exception,e:
            logger.exception(e)
            return None
        else:
            return host_amount,host_list

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
            logger.debug("agent exit at %s" % time.asctime())
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="cloud loadbalance agent")
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

    sender_queue = Queue.Queue()
    
    AgentSenderThread = AgentSender(sender_queue)
    AgentSenderThread.start()
    
    AgentReceiverThread = AgentReceiver(sender_queue)
    AgentReceiverThread.start()
    
    AgentHreatbeatThread = AgentHreatbeat(sender_queue)
    AgentHreatbeatThread.start()

