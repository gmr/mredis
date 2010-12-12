About
=====

MRedis is a multi-servwe wapper for the Python Redis client at https://github.com/andymccurdy/redis-py.

While the redis-py module supports multiple connections it does not provide any type of server hashing to automatically distribute keys across the connection
pool.

The goal was to provide a drop in replacement/enhancement of the redis-py client. As such, the function signatures are the same wherever possible.


Requirements
============

* redis-py


Example
=======

    import mredis
    import time

    servers = [{'host': 'localhost', 'port': 6379, 'db': 0},
               {'host': 'localhost', 'port': 6380, 'db': 0}]

    mr = mredis.MRedis(servers)

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


Purposefully omitted functionality
==================================

* Redis.mget
* Redis.mset
* Redis.msetnx
