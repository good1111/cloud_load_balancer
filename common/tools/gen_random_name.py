#!/usr/bin/env python
import os
import sys
import shelve

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             os.path.join(os.path.pardir,os.path.pardir)))
import settings

ranges = [
        {1:100000},
        {2:200000},
        {3:300000},
        {4:400000},
        {5:500000},
        {6:600000},
        {7:700000},
        {8:800000},
        {9:900000}
    ]


def new_db():
    db = shelve.open(settings.DNS_NAME_POOL,flag='c')
    try:
        db['head']
    except:
        db['head'] = []
    try:
        db['record']
    except:
        db['record'] = []
    return db


def usage():
    db = new_db()
    head = db['head']
    real_ranges = ranges[:]
    key_list = []
    
    for item in real_ranges:
        for key,value in item.items():
            if key in head:
                ranges.remove(item)
    db.close()
    
    for item in ranges:
        for key,value in item.items():
            key_list.append(key)   
    
    min_key = min(key_list)
    max_key = max(key_list)
    
    print
    print """Usage: python %s [gen %d-%d] | [head]
    """ % (os.path.realpath(__file__), min_key, max_key)
    for i in ranges:
        print i
    print


def gen(number_range):
    db = new_db()
    number_range = int(number_range)
    record = db['record']
    
    head = db['head']
    real_ranges = ranges[:]
    
    for item in head:
        if number_range == item:
            print "Head [%s] was existed!" % item
            sys.exit(1)
    
    for item in real_ranges:
        for key,value in item.items():
            if key in head:
                real_ranges.remove(item)
    
    end_range = number_range - 1
    for item in real_ranges:
        for key,value in item.items():
            if number_range == key:
                start_value = value
                important_key = key
                
    for item in ranges:
        for key,value in item.items():
            if number_range > 1:
                if end_range == key:
                    end_value = value
            else:
                end_value = settings.DNS_START
    
    for i in xrange(end_value,start_value):
        record.append(i)
    db['record'] = record
    
    head = db['head']
    head.append(important_key)
    db['head'] = head
    db.close()


def listdb():
    db = new_db()
    try:
        head = db['head']
        if len(head) >0:
            print "Head: %s" % head
        else:
            print "Head is empty."
    except:
        print "Head is empty."
    db.close()


if __name__ == '__main__':
    if len(sys.argv) <= 1:
        usage()
    else:
        try:
            cmd = sys.argv[1]
        except KeyError:
            usage()
        else:
            if cmd == "head":
                listdb()
            if cmd == "gen":
                try:
                    number_range = sys.argv[2]
                except KeyError:
                    usage()
                else:
                    gen(number_range)

