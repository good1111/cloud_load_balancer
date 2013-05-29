import sys
import os
import time
import threading
import Queue
import simplejson

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             os.path.pardir))

import settings

from common.openstack import keystone as keystone_api
from common.openstack import nova as nova_api
from common.sortdict import SortedDict
from common.ipy import IP
from common.utils import gen_message_id
from common.utils import gen_random_dns_name
from common.schduler import scheduler
from common.exceptions import *

from model.loadbalance import LoadBalance
from model.uuid import UUID

instance_queue = Queue.Queue()


def _extra_instance(request, instance, detailed=False, full_flavors=None):
    # to get size
    if full_flavors is None:
        flavors = nova_api.flavor_list(request)
        full_flavors = SortedDict([(str(flavor.id), flavor) for flavor in flavors])
    instance.full_flavor = full_flavors[instance.flavor['id']]
    instance.is_ajax_stating = False  # flag to tell state updating by AJAX, default as True
    
    if detailed:
        instance.power_state = settings.POWER_STATES.get(getattr(instance, "OS-EXT-STS:power_state", 0), '')
        instance.task = getattr(instance, 'OS-EXT-STS:task_state', '')
        instance.physical_host = getattr(instance,'OS-EXT-SRV-ATTR:host','')
        instance.is_transitionary = instance.status and instance.status.upper() not in settings.FINAL_STATUS
        instance.loading_status = instance.task or instance.is_transitionary
        if instance.created: # it is not datetime type :(
            time_str = instance.created
            instance.created = time_str.replace('T', ' ').replace('Z', '')
        
    return instance


class User(object):
    """
        This is Openstack's user, which is managed by Keystone.
    """
    def __init__(self, id=None, account_id=None, token=None, name=None, tenant_id=None,
                    service_catalog=None, tenant_name=None, roles=None, authorized_tenants=None):
        self.id = id
        self.account_id = account_id
        self.token = token
        self.name = name
        self.tenant_id = tenant_id
        self.tenant_name = tenant_name
        self.service_catalog = service_catalog
        self.roles = roles or []
        self.authorized_tenants = authorized_tenants
        
    @property
    def has_login_openstack(self):
        return self.tenant_id

    def is_authenticated(self):
        # TODO: deal with token expiration
        return self.token

    def is_admin(self):
        for role in self.roles:
            if role['name'].lower() == 'admin':
                return True
        return False


class Request(object):
    def __init__(self):
        pass
    
    def __setattr__(self, name, value):
        object.__setattr__(self, name, value)


def _set_request_data(request, token):
    request.session = dict()
    request.session['serviceCatalog'] = token.serviceCatalog
    request.session['tenant'] = token.tenant['name']
    request.session['tenant_id'] = token.tenant['id']
    request.session['token'] = token.id
    request.session['user_id'] = token.user['id']
    request.session['roles'] = token.user['roles']
    request.session['user_name'] = 'admin'


class NovaMain(object):
    """
    Receive item from cloud monitor, and create nova instance.
    If the loadbalance type is http, will create dns name in auto.
    Create the nginx load balance host at last.
    """
    def __init__(self, instance_id, lb_queue, dns_queue,
                 receiver_thread, add_host_handler, delete_host_handler,
                 lb_session, uuid_session, logger):
        self.instance_id = instance_id
        self.lb_queue = lb_queue
        self.dns_queue = dns_queue
        self.receiver_thread = receiver_thread
        self.add_host_handler = add_host_handler
        self.delete_host_handler = delete_host_handler
        self.lb_session = lb_session()
        self.uuid_session = uuid_session()
        self.lb_query = self.lb_session.query(LoadBalance)
        self.uuid_query = self.uuid_session.query(UUID)
        self.logger = logger
        self.run()

    def run(self):
        """
        Get image and check the src server of the image.
        if it equal the self.instance_id, will be create another instance
        and get the ip address of them.
        If the type of load balance host is http, will create a dns name random.
        """
        # check username and userid of instance id will start here!
        self.original_instance = self.lb_query.filter_by(uuid=self.instance_id)
        
        try:
            count = self.original_instance.count()
            if count > 1:
                raise InstanceNotUniqueError
            elif count == 0:
                raise NoInstanceError
        except (InstanceNotUniqueError, NoInstanceError),e:
            self.logger.exception(e)
            return
        else:
            self.instance = self.original_instance[0]
        
        self.username = self.instance.user_name
        self.userid = self.instance.user_id
        try:
            self.realserver = simplejson.loads(self.instance.realserver)
        except simplejson.JSONDecodeError:
            self.logger.error("The realserver data in table are error!")
            return
        
        # get loadbalance host list from uuid table
        self.loadbalance_host = self.uuid_query.filter_by(uuid=self.instance_id)
        lb_count = self.loadbalance_host.count()
        
        request = Request()
        
        # login to openstack
        token = keystone_api.simple_auth(self.username,settings.ADMIN_PASSWORD(self.userid))
        
        _set_request_data(request, token)
        request.ouser = User(id=request.session['user_id'],
                            token=request.session['token'],
                            name=request.session['user_name'],
                            tenant_id=request.session['tenant_id'],
                            tenant_name=request.session['tenant'],
                            service_catalog=request.session['serviceCatalog'],
                            roles=request.session['roles'])
        
        # get snapshot_list and check the server id of per snapshot
        snapshots = nova_api.snapshot_list(request, detailed=True)
        for snapshot in snapshots:
            src_server_id = getattr(snapshot, 'server', {}).get('id', None)
            if src_server_id == self.instance_id:
                image_id = snapshot.id
                break
        
        PRIVATE_NETWORKS = IP(settings.PRIVATE_NETWORKS)
        
        # if we got server id of the snapshot
        if src_server_id:
            primary_instance = nova_api.server_get(request, src_server_id)
            primary_private_net = primary_instance.networks.get('private',None)
            
            primary_status = primary_instance.status
            
            # get private ip address of primary instance
            for net_name, ip_list in primary_private_net.items():
                if ip_list[0] in PRIVATE_NETWORKS:
                    self.primary_private_ip = ip
                    break
            
            # flavor id for create new instance
            flavor_id = primary_instance.flavor.get('id')
            name = primary_instance.name + '-lb'
            
            # if the amount of loadbalance host more than one 
            if lb_count > 0:
                second_private_net = None
                second_instance = nova_api.server_create(request, name, image_id,
                                                         flavor_id, key_name=None, user_data=None)
                # get private ip in a loop
                while not second_private_net:
                    second_instance = nova_api.server_get(request, second_instance.id)
                    second_private_net = second_instance.networks.get('private', None)
                    time.sleep(1)
                
                self.second_id = second_instance.id
                second_status = second_instance.status
                
                # get private ip address of second instance
                for ip in second_private_net:
                    if ip in PRIVATE_NETWORKS:
                        self.second_private_ip = ip
                        break
                
                # if private ip of primary and second instance has prepared allready.
                if self.primary_private_ip and self.second_private_ip:
                    self.update_realserver({self.second_id: self.second_private_ip})
                    self.realserver.append({self.second_id: self.second_private_ip})
                    self.realserver.insert(0, {self.instance_id: self.primary_private_ip})
                    
                    # if one uuid has more loadbalance need to be create.
                    for host in self.loadbalance_host:
                        # we just only create loadbalance host which the type is automatically
                        if host.create_type == "automatically":
                            if host.node_id:
                                self.node_id = host.node_id
                                self.logger.debug("Got node_id: %s from DB!" % self.node_id)
                            else:
                                self.node_id = scheduler(self.receiver_thread)
                                self.logger.debug("Got node_id: %s from scheduler!" % self.node_id)
                                
                            if not self.node_id:
                                self.logger.error("Can not found any loadbalance node for host: %s!" % self.host.id)
                                return
                            
                            self.node_ip = settings.LB_NODE_NETWORK[self.node_id]
                            
                            msg_id = gen_message_id()
                            handler = AddLBHandler(host=host, node_ip=self.node_ip,
                                                        add_host_handler=self.add_host_handler,
                                                        delete_host_handler=self.delete_host_handler,
                                                        dns_queue=self.dns_queue, lb_queue=self.lb_queue,
                                                        message_id=msg_id,
                                                        realserver=self.realserver, node_id=self.node_id,
                                                        logger=self.logger, uuid_session=self.uuid_session)
                            handler.run()
        else:
            self.logger.debug("can not found snapshot for %s" % self.instance_id)
    
    def update_realserver(self,realserver):
        try:
            original_realserver = simplejson.loads(self.instance.realserver)
            original_realserver.append(realserver)
            self.original_instance.update({
                LoadBalance.realserver:simplejson.dumps(original_realserver)
            })
            self.lb_session.commit()
        except Exception,e:
            self.logger.exception(e)
            self.lb_session.rollback()


class AddLBHandler(object):
    """
    This class will be called by NovaMain, for add loadbalance host to LB host.
    """
    def __init__(self, host=None, node_ip=None, dns_queue=None,
                 lb_queue=None, message_id=None, realserver=None,
                 node_id=None, add_host_handler=None, delete_host_handler=None,
                 logger=None, uuid_session=None):
        self.host = host
        self.node_ip = node_ip
        self.dns_queue = dns_queue
        self.lb_queue = lb_queue
        self.message_id = message_id
        self.realserver = realserver
        self.node_id = node_id
        self.add_host_handler = add_host_handler
        self.delete_host_handler = delete_host_handler
        self.logger = logger
        self.uuid_session = uuid_session
    
    def run(self):
        """
        if the type of loadbalance host is http, will add dns name record to dns server.
        """
        self.invoke_type = "add_dns"
        if self.host.lb_type == 'http':
            body = {
                'zone':settings.CLOUD_DOMAIN,
                'host':self.host.dns_name.split('.')[0],
                'type':settings.DEFAULT_NS_TYPE,
                'data':self.node_ip,
                'ttl':None,
                'mx_priority':None,
                'refresh':None,
                'retry':13,
                'expire':None,
                'minimum':None,
                'serial':None,
                'resp_person':None,
                'primary_ns':None,
                'msg_id':self.message_id
                }        
            self.dns_queue.put((
                {'head':'add','body':body},
                self,
                self.message_id
                ))
        else:
            self.delete_host(True)
    
    def delete_host(self, ret):
        if ret == True:
            # if host.node_id is not empty, delete the host config file in nginx first.
            if self.host.node_id:   
                self.invoke_type = "delete_host"
                if self.host.lb_type == 'http':
                    server_name = self.host.dns_name
                    message,msg_id = self.delete_host_handler(
                        lb_type = self.host.lb_type,
                        server_name = server_name,
                        node_id = self.node_id
                    )
                elif self.host.lb_type == 'tcp':
                    server_port = self.host.src_port
                    message,msg_id = self.delete_host_handler(
                        lb_type = self.host.lb_type,
                        server_port = server_port,
                        node_id = self.node_id
                    )
                    
                self.lb_queue.put((
                        message,
                        self,
                        msg_id
                        ))
            else:
                ret = {'failed': False}
                self.add_host(ret)
        else:
            self.logger.error("Add dns record failed: %s" % ret)
    
    def add_host(self, ret):
        if not ret.get('failed',None):    
            self.invoke_type = "add_host"
            if self.host.lb_type == 'http':
                self.server_name = self.host.dns_name
                
                upstream_server = []
                for server in self.realserver:
                    for uuid,ip in server.items():
                        upstream_server.append({ip:self.host.dst_port})
                        
                node_id = self.node_id
                distrubute = settings.LB_DEFAULT_DIS
                message,msg_id = self.add_host_handler(lb_type=self.host.lb_type, server_name=self.server_name,
                                           upstream_server=upstream_server, node_id=self.node_id,
                                           distrubute=distrubute)
                self.lb_queue.put((
                    message,
                    self,
                    msg_id
                    ))
                
            elif self.host.lb_type == 'tcp':
                server_port = self.host.src_port
                upstream_server = []
                for server in self.realserver:
                    for uuid,ip in server.items():
                        upstream_server.append({ip:self.host.dst_port})
                node_id = self.node_id
                distrubute = settings.LB_DEFAULT_DIS
                message,msg_id = self.add_host_handler(lb_type=self.host.lb_type, server_port=server_port,
                                           upstream_server=upstream_server, node_id=self.node_id,
                                           distrubute=distrubute)
                self.lb_queue.put((
                    message,
                    self,
                    msg_id
                    ))
        else:
            self.logger.error('Delete nginx loadbalance host failed')
            self.logger.error(simplejson.dumps(ret))
    
    def update_lbhost(self, ret):
        """
        at last we update the state of singel loadbalance host.
        """
        if not ret.get('failed',None):
            try:
                uuid_obj = self.uuid_session.query(UUID).filter_by(id=self.host.id)
                uuid_obj.update({
                    UUID.node_id:self.node_id,
                    UUID.result:'0'
                    })
                self.uuid_session.commit()
            except Exception,e:
                self.logger.exception(e)
                self.uuid_session.rollback()
        else:
            self.logger.error("Add nginx loadbalance host failed!")
            self.logger.error(simplejson.dumps(ret))


class NovaInstance(threading.Thread):
    """
    Receive item from cloud monitor through the instance_queue, if no item in queue it will be block.
    Then start a new thread to finish work if got item from the queue.
    """
    def __init__(self,instance_queue, lb_queue, dns_queue,
                 main_receiver_thread, add_host_handler, delete_host_handler,
                 loadbalance_session, uuid_session, logger):
        super(NovaInstance,self).__init__()
        self.instance_queue = instance_queue
        self.lb_queue = lb_queue
        self.dns_queue = dns_queue
        self.receiver_thread = main_receiver_thread
        self.add_host_handler = add_host_handler
        self.delete_host_handler = delete_host_handler
        self.lb_session = loadbalance_session
        self.uuid_session = uuid_session
        self.logger = logger

    def run(self):
        while True:
            uuid = self.instance_queue.get()
            nova_thread = threading.Thread(target=NovaMain,
                                           args=(uuid, self.lb_queue,self.dns_queue,
                                                                 self.receiver_thread,self.add_host_handler,
                                                                 self.delete_host_handler,
                                                                 self.lb_session,self.uuid_session, self.logger))
            nova_thread.start()

