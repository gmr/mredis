#!/usr/bin/env python

# Include our src dir
import sys
sys.path.append('../src')

import mredis
import time

ports = [6379, 6380]

servers = []
for port in ports:
    servers.append({'host': 'localhost', 'port': port, 'db': 0})

mr = mredis.MRedis(servers)

# Destructive test of the database
#print mr.flushall()
#print mr.flushdb()
print mr.ping()

# Build a set of keys for operations
keys = set()
for x in xrange(0, 100):
    key = 'key:%.8f' % time.time()
    keys.add(key)

for key in keys:
    mr.set(key, time.time())

fetched = mr.keys('key:*')
results = []
for server in fetched:
    temp = fetched[server].split(' ')
    for key in temp:
        results.append('%s->%s' % (key, mr.get(key)))
print '%i keys fetched' % len(results)

print mr.bgrewriteaof()
print mr.dbsize()
print mr.lastsave()
#print mr.info()
print mr.randomkey()
