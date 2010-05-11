'''
Created on 2010/03/15

@author: Masahiro Fukuda
'''

import sys
import memc
import memc.basic
import socket

from threading import Lock
from Queue import Queue
#from multiprocessing import Queue
from memc.basic import SocketError, KeyNotFoundError


RETRY_NUM = 2


class Client(memc.basic.Client):
    def __init__(self, servers):
        super(Client, self).__init__(servers[0])
        
        self._servers = servers
        self.mc = None
        
        try:
            self._connect2()
        except:
            pass
    
    def connect(self, force=False):
        pass
    
    def _connect2(self):
        for server in self._servers:
            try:
                self._server = memc.conn2tuple(server)
                super(Client, self).connect(True)
                return
            except socket.error:
                continue
    
    def _get(self, cmd, keys, use_cas=False):
        for a in xrange(RETRY_NUM):
            try:
                return super(Client, self)._get(cmd, keys, use_cas)
            except socket.error:
                self._error_log("Can't connect to %s:%d, will attempt next one." % self._server)
                self._connect2()
        raise SocketError("Can't connect servers.")
                
    def _error_log(self, msg):
        sys.stderr.write("memc-flare: %s\n" % msg)

    def _set(self, cmd, key, value, kwargs={}):
        for a in xrange(RETRY_NUM):
            try:
                return super(Client, self)._set(cmd, key, value, kwargs)
            except socket.error:
                self._error_log("Can't connect to %s:%d, will attempt next one." % self._server)
                self._connect2()

        raise SocketError("Can't connect servers.")
                
    def _incr_decr(self, cmd, key, value, kwargs={}):
        for a in xrange(RETRY_NUM):
            try:
                return super(Client, self)._incr_decr(cmd, key, value, kwargs)
            except socket.error:
                self._error_log("Can't connect to %s:%d, will attempt next one." % self._server)
                self._connect2()
                
        raise SocketError("Can't connect servers.")

    def _delete(self, key, kwargs={}):
        for a in xrange(RETRY_NUM):
            try:
                return super(Client, self)._delete(key, kwargs)
            except socket.error:
                self._error_log("Can't connect to %s:%d, will attempt next one." % self._server)
                self._connect2()
            
        raise SocketError("Can't connect servers.")



class Pool(object):
    def __init__(self, servers, max_pool = 5):
        self._max_pool = max_pool
        self._servers = servers
        self._queue = Queue()
        self._conns = []

        for i in xrange(self._max_pool):
            self._connect()
        
    def _connect(self):
        fl = Client(self._servers)
        fl.connect()
        self._queue.put(fl)

    def _get(self, cmd, keys, use_cas=False):
        try:
            fl = self._queue.get()
            result =  fl._get(cmd, keys, use_cas)
            return result
        except:
            raise
        finally:
            self._queue.put(fl)
        
    def _set(self, cmd, key, value, kwargs={}):
        try:
            fl = self._queue.get()
            result =  fl._set(cmd, key, value, kwargs)
            return result
        except:
            raise
        finally:
            self._queue.put(fl)

    def _incr_decr(self, cmd, key, value, kwargs={}):
        try:
            fl = self._queue.get()
            result =  fl._incr_decr(cmd, key, value, kwargs)
            return result
        except:
            raise
        finally:
            self._queue.put(fl)
        
    def _delete(self, key, kwargs={}):
        try:
            fl = self._queue.get()
            result =  fl._delete(key, kwargs)
            return result
        except:
            raise
        finally:
            self._queue.put(fl)


    def delete(self, key, **kwargs):
        return self._delete(key, kwargs)

    def set(self, key, value, **kwargs):
        return self._set('set', key, value, kwargs)

    def add(self, key, value, **kwargs):
        return self._set('add', key, value, kwargs)

    def replace(self, key, value, **kwargs):
        return self._set('replace', key, value, kwargs)

    def append(self, key, value, **kwargs):
        return self._set('append', key, value, kwargs)

    def prepend(self, key, value, **kwargs):
        return self._set('prepend', key, value, kwargs)

    def cas(self, key, value, cas, **kwargs):
        kwargs[memc.basic.OPT_CAS] = cas
        return self._set('cas', key, value, kwargs)

    def get(self, key):
        return self.raw_get(key)[0]

    def raw_get(self, key):
        result = self._get('get', [key])
        
        if result.has_key(key):
            return result[key]
        
        raise KeyNotFoundError("Key:%s is not found." % key)

    def raw_gets(self, key):
        result = self._get('gets', [key], True)
        
        if result.has_key(key):
            return result[key]
        
        raise KeyNotFoundError("Key:%s is not found." % key)
       
    def raw_mget(self, keys):
        return self._get("get", keys)

    def raw_mgets(self, keys):
        return self._get("gets", keys, True)

    def incr(self, key, value, **kwargs):
        return self._incr_decr('incr', key, value, kwargs)

    def decr(self, key, value, **kwargs):
        return self._incr_decr('decr', key, value, kwargs)

        
if __name__ == "__main__":
    pass
