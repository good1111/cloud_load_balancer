import os
from hashlib import sha224

# the top dir of this project
LOCAL_PATH = os.path.dirname(os.path.realpath(__file__))

# configuration for RabbitMQ Server
rabbitmq_server = "127.0.0.1"
username = "guest"
password = "guest"
virtual_host = "/"
frame_max_size = 131072 # 128 KB

# debug option for orm
orm_debug = False

# configuration for MySQL Server
mysql_server = '127.0.0.1'
mysql_port = 3306
mysql_username = 'root'
mysql_password = 'root'
mysql_database = 'loadbalance'
mysql_pool_size = 100

# configuration for sqlite
sqlite_database = os.path.join(LOCAL_PATH,'loadbalance.db')

# configuration for template
TEMPLATE_PATH = 'templates'
HTTP_LOADBALANCE_CONF = 'http_lb.conf'
TCP_LOADBALANCE_CONF = 'tcp_lb.conf'


# main configuration directory of nginx
main_conf_path = '/usr/local/nginx/conf'

# configuration directory http load balance host
http_conf_path = os.path.join(main_conf_path,'http.conf.d')

# configuration directory tcp load balance host
tcp_conf_path = os.path.join(main_conf_path,'tcp.conf.d')

# binary executable file of nginx
nginx_bin_path = '/usr/local/nginx/sbin/nginx'

# the suffix of configuration file
conf_suffix = '.conf'


# configuration for openstack
OPENSTACK_HOST = '127.0.0.1'
OPENSTACK_KEYSTONE_URL = 'http://%s:5000/v2.0' % OPENSTACK_HOST
OPENSTACK_KEYSTONE_DEFAULT_ROLE = 'members'

# TODO: Encryption service token
# user this to fake admin token, for user maintenance
SERVICE_ENDPOINT = 'http://%s:35357/v2.0' % OPENSTACK_HOST
SERVICE_TOKEN = 'openstack'

## for admin
ADMIN_USER_NAME = 'admin'
#ADMIN_PASSWORD = 'openstack'
ADMIN_PASSWORD = lambda id_str: sha224('_!@$#&^_%s' % (id_str)).hexdigest()[:10]
ADMIN_TENANT = 'admin'

POWER_STATES = {
    0: "NO STATE",
    1: "RUNNING",
    2: "BLOCKED",
    3: "PAUSED",
    4: "SHUTDOWN",
    5: "SHUTOFF",
    6: "CRASHED",
    7: "SUSPENDED",
    8: "FAILED",
    9: "BUILDING",
}

PAUSE = 0
UNPAUSE = 1
SUSPEND = 0
RESUME = 1

# status from nova server
INSTANCE_STATUS_ACTIVE = 'ACTIVE'

FINAL_STATUS = ('ACTIVE', 'ERROR', 'SUSPENDED', 'DELETED', 'SHUTOFF')

PRIVATE_NETWORKS = "10.0.0.0/8"

CLOUD_DOMAIN = "test.com"
DEFAULT_NS_TYPE = "a"
DNS_NAME_POOL = os.path.join(LOCAL_PATH,"name_pool.dat")

# import local configuration file
try:
    from local_conf import *
except:
    pass