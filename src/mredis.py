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

class UnextendedRedisCommand(Exception):
    pass

class MRedis:

    def __init__(self, config):
        """
        Expects a list of dictionaries containing host, port, db:

        servers = [{'host': 'localhost', 'port': 6379, 'db': 0},
                   {'host': 'localhost', 'port': 6380, 'db': 0}]

        mr = mredis.MRedis(servers)

        """

        self.pool = redis.ConnectionPool()
        self.servers = []
        self.pipeline = []

        for server in config:

            self.servers.append(redis.Redis(host=server['host'],
                                            port=server['port'],
                                            db=server['db'],
                                            connection_pool=self.pool))


    ### MRedis Specific Parts ###
    def get_node_offset(self, key):
        "Return the redis node list offset to use"
        c = crc32(key) >> 16 & 0x7fff
        return c % len(self.servers)


    def get_server_key(self, server):
        "Return a string of server:port:db"

        return "%s:%i:%i" % (server.host, server.port, server.db)


    #### SERVER INFORMATION ####
    def bgrewriteaof(self):

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.bgrewriteaof()
        return response

    def bgsave(self):

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.bgsave()
        return response


    def dbsize(self):

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.dbsize()
        return response


    def flushall(self):

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.flushall()
        return response


    def flushdb(self):

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.flushdb()
        return response


    def info(self):

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.info()
        return response


    def lastsave(self):

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.lastsave()
        return response


    def ping(self):

        response = {}
        for server in self.servers:
            key = self.get_server_key(server)
            response[key] = server.lastsave()
        return response


    def save(self):

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
        raise UnextendedRedisCommand


    def move(self):

        raise UnextendedRedisCommand


    def mset(self):
        raise UnextendedRedisCommand


    def msetnx(self):
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

