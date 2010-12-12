About
=====

MRedis is a multi-servwe wapper for the Python Redis client at https://github.com/andymccurdy/redis-py.

While the redis-py module supports multiple connections it does not provide any type of server hashing to automatically distribute keys across the connection
pool.

The goal was to provide a drop in replacement/enhancement of the redis-py client. As such, the function signatures are the same wherever possible.

Requirements
============

* redis-py

Installation
============

Use pip, easy_install or run "python setup.py install"

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

    # Set these keys across the two Redis servers
    for key in keys:
        mr.set(key, time.time())

    # Get a list of keys from all servers as a dictionary by server
    fetched = mr.keys('key:*')

    # Loop through the servers and build the key list per server
    results = []
    for server in fetched:

        # Get the list of keys
        temp = fetched[server].split(' ')

        # Loop through the keys retrieving them from the servers
        for key in temp:
            results.append('%s->%s' % (key, mr.get(key)))

    # Demonstrate we fetched as many keys as we set
    print '%i keys fetched' % len(results)

Pipelining
==========

While the goal is to make the client behave as closely as possible to redis-py, due to the nature of distributing keys across multiple servers, pipelining can
not be handled the same way.

Because of this, pipeline objects are added per server and should only be used for the same key.

Using multiple keys in a pipeline may and most likely will result in data being stored or used in the wrong server and will yield unpredictable results.

When you request a pipeline object, you pass in a key which will return the proper server for the pipeline.

Using pipelining:


    servers = [{'host': 'localhost', 'port': 6379, 'db': 0},
               {'host': 'localhost', 'port': 6380, 'db': 0}]

    mr = mredis.MRedis(servers)

    pipeline = mr.pipeline(key)
    pipeline.lpush(key, value).lpop(key, value).llen(key)
    pipeline.execute()


Purposefully omitted functionality
==================================

The following functions are currently unimplemented due to complexity in keeping the same behavior with multiple servers.

* Redis.mget
* Redis.mset
* Redis.msetnx
* Redis.rename
* Redis.renamenx
* Redis.blpop
* Redis.brpop
* Redis.sdiff
* Redis.sdiffstore
* Redis.sinter
* Redis.sinterstore
* Redis.smove
* Redis.sunion
* Redis.sunionstore
* Redis.zinterstore
* Redis.zunionstore
* Redis._zaggregate

All Hash and Channel related functionality.

Any function listed as deprecated in the redis-py code is not implemented in mredis.

Notes
=====

Redis.sort will not store currently, needs to be extended to manually store to the correct server.

Currently the only hash_method supported is "standard" which is a crc32 value of the key mod the number of servers.
