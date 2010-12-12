#!/usr/bin/env python

import mredis
import time

ports = [6379, 6380]

servers = []
for port in ports:
    servers.append({'host': 'localhost', 'port': port, 'db': 0})

mr = mredis.MRedis(servers)

# Append some keys
for x in xrange(0, 100):
    mr.lpush('test1', time.time())

# Time a pipeline test
start = time.time()
p = mr.pipeline('test1')
p.lrange('test1', 0, 10).ltrim('test1', 10, -1)
x = p.execute()

print 'Keys returned:'
print x[0]
print 'Pipeline  : %.8f' % (time.time() - start)

start = time.time()
a = []
for x in xrange(0, 10):
    a.append(mr.lpop('test1'))

print 'Range LPop: %.8f' % (time.time() - start)
