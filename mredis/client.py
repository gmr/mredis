"mredis is wrapper for adding multiple server hashing to the redis client."

from binascii import crc32
import redis


class MRedis:

    def __init__(self, config, hash_method='standard'):
        """
        Expects a list of dictionaries containing host, port, db:

        servers = [{'host': 'localhost', 'port': 6379, 'db': 0},
                   {'host': 'localhost', 'port': 6380, 'db': 0}]

        mr = mredis.MRedis(servers)
        """

        self.pool = redis.ConnectionPool()
        self.servers = []

        if hash_method not in ['standard']:
            raise mredis.exceptions.InvalidHashMethod

        self.hash_method = hash_method

        for server in config:

            self.servers.append(redis.Redis(host=server['host'],
                                            port=server['port'],
                                            db=server['db'],
                                            connection_pool=self.pool))

    ### MRedis Specific Parts ###
    def get_node_offset(self, key):
        "Return the redis node list offset to use"

        if self.hash_method == 'standard':
            c = crc32(key) >> 16 & 0x7fff
            return c % len(self.servers)

        raise InvalidHashMethod

    def get_server_key(self, server):
        "Return a string of server:port:db"

        return "%s:%i:%i" % (server.host, server.port, server.db)

    #### SERVER INFORMATION ####
    def bgrewriteaof(self):
        """
        Run the bgrewriteoaf command for each server returning success indexed
        in a dictionary by server
        """

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.bgrewriteaof()
        return response

    def bgsave(self):
        """
        Run the bgsave command for each server returning success indexed in a
        dictionary by server
        """

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.bgsave()
        return response

    def dbsize(self):
        "Return the size of the database in a dictionary keyed by server"

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.dbsize()
        return response

    def flushall(self):
        """
        Flushes all databases for each redis server returning success indexed
        in a dictionary by server
        """

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.flushall()
        return response

    def flushdb(self):
        """
        Flushes the selected db for each redis server returning success indexed
        in a dictionary by server
        """

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.flushdb()
        return response

    def info(self):
        "Returns a dictionary keyed by Redis server of info command output"

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.info()
        return response

    def lastsave(self):
        "Returns a dictionary keyed by Redis server of lastsave command output"

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.lastsave()
        return response

    def ping(self):
        "Returns a dictionary keyed by Redis server of ping command output"

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.lastsave()
        return response

    def save(self):
        "Returns a dictionary keyed by Redis server of save command output"

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.save()
        return response

    ### Basic Key Commands
    def append(self, key, value):
        """
        Appends the string ``value`` to the value at ``key``. If ``key``
        doesn't already exist, create it with a value of ``value``.
        Returns the new length of the value at ``key``.
        """

        offset = self.get_node_offset(key)
        return self.servers[offset].append(key, value)

    def decr(self, key, amount=1):
        """
        Decrements the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as 0 - ``amount``
        """

        offset = self.get_node_offset(key)
        return self.servers[offset].decr(key, amount)

    def delete(self, *key):
        "Delete one or more keys specified by ``key``"

        for temp in key:
            offset = self.get_node_offset(temp)
            if not self.servers[offset].delete(temp):
                return False
        return True


    def exists(self, key):
        "Returns a boolean indicating whether ``key`` exists"

        offset = self.get_node_offset(key)
        return self.servers[offset].exists(key)

    def expire(self, key, time):
        "Set an expire flag on ``key`` for ``time`` seconds"
        offset = self.get_node_offset(key)
        return self.servers[offset].exists(key, time)

    def expireat(self, key, when):
        """
        Set an expire flag on ``key``. ``when`` can be represented
        as an integer indicating unix time or a Python datetime object.
        """

        offset = self.get_node_offset(key)
        return self.servers[offset].exists(key, when)

    def get(self, key):
        """
        Return the value at ``key``, or None of the key doesn't exist
        """

        offset = self.get_node_offset(key)
        return self.servers[offset].get(key)

    def getset(self, key, value):
        """
        Set the value at ``key`` to ``value`` if key doesn't exist
        Return the value at key ``name`` atomically
        """

        offset = self.get_node_offset(key)
        return self.servers[offset].getset(key, value)

    def incr(self, key, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """

        offset = self.get_node_offset(key)
        return self.servers[offset].incr(key, amount)

    def keys(self, pattern="*"):
        """
        Returns a list of keys matching ``pattern`` in a dictionary keyed by
        server
        """

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.keys(pattern)
        return response

    def mget(self):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise mredis.exceptions.UnextendedRedisCommand

    def move(self):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise mredis.exceptions.UnextendedRedisCommand

    def mset(self):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise mredis.exceptions.UnextendedRedisCommand

    def msetnx(self):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise mredis.exceptions.UnextendedRedisCommand

    def randomkey(self):
        "Returns the name of a random key from each server in a dictionary"

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.randomkey()
        return response

    def rename(self, key):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise mredis.exceptions.UnextendedRedisCommand

    def renamenx(self, key):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise mredis.exceptions.UnextendedRedisCommand

    def set(self, key, value):
        """
        Set the value at ``key`` to ``value``

        * The following flags have been deprecated *
        If ``preserve`` is True, set the value only if key doesn't already
        exist
        If ``getset`` is True, set the value only if key doesn't already exist
        and return the resulting value of key
        """

        offset = self.get_node_offset(key)
        return self.servers[offset].set(key, value)

    def setex(self, key, value, time):
        """
        Set the value of ``key`` to ``value``
        that expires in ``time`` seconds
        """

        offset = self.get_node_offset(key)
        return self.servers[offset].setex(key, value, time)

    def substr(self, key, start, end=-1):
        """
        Return a substring of the string at ``key``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.
        """

        offset = self.get_node_offset(key)
        return self.servers[offset].substr(key, start, end)

    def ttl(self, key):
        "Returns the number of seconds until the ``key`` will expire"

        offset = self.get_node_offset(key)
        return self.servers[offset].ttl(key)

    def type(self, key):
        "Returns the type of ``key``"
        offset = self.get_node_offset(key)
        return self.servers[offset].type(key)

    def watch(self, key):
        "Watches the value at ``key``, or None of the key doesn't exist"
        offset = self.get_node_offset(key)
        return self.servers[offset].watch(key)

    def unwatch(self, key):
        "Unwatches the value at ``key``, or None of the key doesn't exist"
        offset = self.get_node_offset(key)
        return self.servers[offset].unwatch(key)

    ### List Commands ###
    def blpop(self, keys, timeout=0):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise mredis.exceptions.UnextendedRedisCommand

    def brpop(self, keys, timeout=0):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise mredis.exceptions.UnextendedRedisCommand

    def lindex(self, key, index):

        offset = self.get_node_offset(key)
        return self.servers[offset].lindex(key, index)

    def linsert(self, key, where, refvalue, value):

        offset = self.get_node_offset(key)
        return self.servers[offset].linsert(key, where, refvalue, value)

    def llen(self, key):

        offset = self.get_node_offset(key)
        return self.servers[offset].llen(key)

    def lpop(self, key):

        offset = self.get_node_offset(key)
        return self.servers[offset].lpop(key)

    def lpush(self, key, value):

        offset = self.get_node_offset(key)
        return self.servers[offset].lpush(key, value)

    def lpushx(self, key, value):

        offset = self.get_node_offset(key)
        return self.servers[offset].lpushx(key, value)

    def lrange(self, key, start, end):

        offset = self.get_node_offset(key)
        return self.servers[offset].lrange(key, start, end)

    def lrem(self, key, value, num=0):

        offset = self.get_node_offset(key)
        return self.servers[offset].lrem(key, value, num)

    def lset(self, key, index, value):

        offset = self.get_node_offset(key)
        return self.servers[offset].set(key, index, value)

    def ltrim(self, key, start, end):

        offset = self.get_node_offset(key)
        return self.servers[offset].ltrim(key, start, end)

    def rpop(self, key):

        offset = self.get_node_offset(key)
        return self.servers[offset].rpop(key)

    def rpush(self, key, value):

        offset = self.get_node_offset(key)
        return self.servers[offset].rpush(key, value)

    def rpushx(self, key, value):

        offset = self.get_node_offset(key)
        return self.servers[offset].rpushx(key, value)

    def sort(self, key, start=None, num=None, by=None, get=None,
             desc=False, alpha=False, store=None):

        offset = self.get_node_offset(key)
        return self.servers[offset].store(key, start, num, by, get, desc,
                                          alpha, None)

    #### SET COMMANDS ####
    def sadd(self, key, value):
        "Add ``value`` to set ``key``"

        offset = self.get_node_offset(key)
        return self.servers[offset].sadd(key, value)

    def scard(self, key):
        "Return the number of elements in set ``key``"

        offset = self.get_node_offset(key)
        return self.servers[offset].scard(key)

    def sdiff(self, keys, *args):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise mredis.exceptions.UnextendedRedisCommand

    def sdiffstore(self, dest, keys, *args):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise mredis.exceptions.UnextendedRedisCommand

    def sinter(self, keys, *args):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise mredis.exceptions.UnextendedRedisCommand

    def sinterstore(self, dest, keys, *args):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise mredis.exceptions.UnextendedRedisCommand

    def sismember(self, key, value):
        "Return a boolean indicating if ``value`` is a member of set ``key``"

        offset = self.get_node_offset(key)
        return self.servers[offset].sismember(key, value)

    def smembers(self, key):
        "Return all members of the set ``key``"

        offset = self.get_node_offset(key)
        return self.servers[offset].smembers(key)

    def smove(self, src, dst, value):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise mredis.exceptions.UnextendedRedisCommand

    def spop(self, key):
        "Remove and return a random member of set ``key``"

        offset = self.get_node_offset(key)
        return self.servers[offset].spop(key)

    def srandmember(self, key):
        "Return a random member of set ``key``"

        offset = self.get_node_offset(key)
        return self.servers[offset].srandmember(key)

    def srem(self, key, value):
        "Remove ``value`` from set ``key``"

        offset = self.get_node_offset(key)
        return self.servers[offset].srem(key, value)

    def sunion(self, keys, *args):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise mredis.exceptions.UnextendedRedisCommand

    def sunionstore(self, dest, keys, *args):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise mredis.exceptions.UnextendedRedisCommand

    #### SORTED SET COMMANDS ####
    def zadd(self, key, value, score):
        "Add member ``value`` with score ``score`` to sorted set ``key``"

        offset = self.get_node_offset(key)
        return self.servers[offset].zadd(key, value, score)

    def zcard(self, key):
        "Return the number of elements in the sorted set ``key``"

        offset = self.get_node_offset(key)
        return self.servers[offset].zcard(key)

    def zcount(self, key, min, max):

        offset = self.get_node_offset(key)
        return self.servers[offset].zcount(key, min, max)

    def zincrby(self, key, value, amount=1):
        "Increment the score of ``value`` in sorted set ``key`` by ``amount``"

        offset = self.get_node_offset(key)
        return self.servers[offset].zadd(key, amount, value)

    def zinterstore(self, dest, keys, aggregate=None):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise mredis.exceptions.UnextendedRedisCommand

    def zrange(self, key, start, end, desc=False, withscores=False):
        """
        Return a range of values from sorted set ``key`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` indicates to sort in descending order.

        ``withscores`` indicates to return the scores along with the values.
            The return type is a list of (value, score) pairs
        """

        offset = self.get_node_offset(key)
        return self.servers[offset].zrange(key, start, end, desc, withscores)

    def zrangebyscore(self, key, min, max,
            start=None, num=None, withscores=False):
        """
        Return a range of values from the sorted set ``key`` with scores
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice of the range.

        ``withscores`` indicates to return the scores along with the values.
            The return type is a list of (value, score) pairs
        """

        offset = self.get_node_offset(key)
        return self.servers[offset].zrangebyscore(key, min, max, start, num,
                                                  withscores)

    def zrank(self, key, value):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``key``
        """

        offset = self.get_node_offset(key)
        return self.servers[offset].zrank(key, value)

    def zrem(self, key, value):
        "Remove member ``value`` from sorted set ``key``"

        offset = self.get_node_offset(key)
        return self.servers[offset].zrem(key, value)

    def zremrangebyrank(self, key, min, max):
        """
        Remove all elements in the sorted set ``key`` with ranks between
        ``min`` and ``max``. Values are 0-based, ordered from smallest score
        to largest. Values can be negative indicating the highest scores.
        Returns the number of elements removed
        """

        offset = self.get_node_offset(key)
        return self.servers[offset].zremrangebyrank(key, min, max)

    def zremrangebyscore(self, key, min, max):
        """
        Remove all elements in the sorted set ``key`` with scores
        between ``min`` and ``max``. Returns the number of elements removed.
        """

        offset = self.get_node_offset(key)
        return self.servers[offset].zremrangebyscore(key, min, max)

    def zrevrange(self, key, start, num, withscores=False):
        """
        Return a range of values from sorted set ``key`` between
        ``start`` and ``num`` sorted in descending order.

        ``start`` and ``num`` can be negative, indicating the end of the range.

        ``withscores`` indicates to return the scores along with the values
            as a dictionary of value => score
        """

        offset = self.get_node_offset(key)
        return self.servers[offset].zrevrange(key, start, num, withscores)

    def zrevrank(self, key, value):
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``key``
        """

        offset = self.get_node_offset(key)
        return self.servers[offset].zrevrank(key, value)

    def zscore(self, key, value):
        "Return the score of element ``value`` in sorted set ``key``"

        offset = self.get_node_offset(key)
        return self.servers[offset].zscore(key, value)


    def zunionstore(self, dest, keys, aggregate=None):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise mredis.exceptions.UnextendedRedisCommand

    def _zaggregate(self, command, dest, keys, aggregate=None):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise mredis.exceptions.UnextendedRedisCommand

    ### Pipeline Function ###
    def pipeline(self, key):

        offset = self.get_node_offset(key)
        return self.servers[offset].pipeline()
