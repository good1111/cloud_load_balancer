import os
import sys
import uuid
import shelve
import random

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),os.path.pardir))
import settings


# return a uuid in hex string
def gen_message_id():
    return uuid.uuid4().get_hex()

# generate some random integer number 
def gen_random_dns_name():
    db = shelve.open(settings.DNS_NAME_POOL,flag='c')
    record = db['record']
    name = record.pop()
    db['record'] = record
    db.close()
    return str(name)