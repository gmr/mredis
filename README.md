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

Purposefully omitted functionality
==================================

* Redis.mget
* Redis.mset
* Redis.msetnx

Any function listed as deprecated in the redis-py code.

Notes
=====

Redis.sort will not store currently, needs to be extended to manually store to the correct server.

Currently the only hash_method supported is "standard" which is a crc32 value of the key mod the number of servers.
