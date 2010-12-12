"""
mredis is wrapper for adding multiple server hashing to the redis client.

Purposefully ommitted commands:

  redis.move
  redis.mget
  redis.mset
  redis.msetnx
"""

from binascii import crc32
import redis


class InvalidHashMethod(Exception):
    pass


class UnextendedRedisCommand(Exception):
    pass


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
        self.pipeline = []

        if hash_method not in ['standard']:
            raise InvalidHashMethod

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

        offset = self.get_node_offset(key)
        return self.servers[offset].append(key, value)

    def delete(self, key):

        offset = self.get_node_offset(key)
        return self.servers[offset].delete(key)

    def decr(self, key, amount=1):

        offset = self.get_node_offset(key)
        return self.servers[offset].decr(key, amount)

    def exists(self, key):

        offset = self.get_node_offset(key)
        return self.servers[offset].exists(key)

    def expire(self, key, time):

        offset = self.get_node_offset(key)
        return self.servers[offset].exists(key, time)

    def expireat(self, key, when):

        offset = self.get_node_offset(key)
        return self.servers[offset].exists(key, when)

    def get(self, key):

        offset = self.get_node_offset(key)
        return self.servers[offset].get(key)

    def getset(self, key, value):

        offset = self.get_node_offset(key)
        return self.servers[offset].getset(key, value)

    def incr(self, key, amount=1):

        offset = self.get_node_offset(key)
        return self.servers[offset].incr(key, amount)

    def keys(self, pattern="*"):

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

        raise UnextendedRedisCommand

    def move(self):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise UnextendedRedisCommand

    def mset(self):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise UnextendedRedisCommand

    def msetnx(self):
        """
        Currently unimplemented due to complexity of perserving this behavior
        properly with multiple servers.
        """

        raise UnextendedRedisCommand

    def randomkey(self):

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.randomkey()
        return response

    def rename(self, key):

        # @TODO Write this function, will have to manually perform functions
        raise UnextendedRedisCommand

    def renamenx(self, key):

        # @TODO Write this function, will have to manually perform functions
        raise UnextendedRedisCommand

    def set(self, key, value):

        offset = self.get_node_offset(key)
        return self.servers[offset].set(key, value)

    def setex(self, key, value, time):

        offset = self.get_node_offset(key)
        return self.servers[offset].setex(key, value, time)

    def substr(self, key, start, end=-1):

        offset = self.get_node_offset(key)
        return self.servers[offset].substr(key, start, end)

    def ttl(self, key):

        offset = self.get_node_offset(key)
        return self.servers[offset].ttl(key)

    def type(self, key):

        offset = self.get_node_offset(key)
        return self.servers[offset].type(key)

    def watch(self, key):

        offset = self.get_node_offset(key)
        return self.servers[offset].watch(key)

    def unwatch(self, key):

        offset = self.get_node_offset(key)
        return self.servers[offset].unwatch(key)

    ### List Commands ###
    def blpop(self, keys, timeout=0):
        # @TODO Write this function, will have to manually perform functions

        raise UnextendedRedisCommand

    def brpop(self, keys, timeout=0):
        # @TODO Write this function, will have to manually perform functions

        raise UnextendedRedisCommand

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
