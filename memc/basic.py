'''
Created on 2010/03/15

@author: Masahiro Fukuda
'''

import memc
import socket

class Error(memc.Error):
    pass

class StoreError(memc.StoreError):
    pass

class KeyNotFoundError(memc.KeyNotFoundError):
    pass

class SocketError(socket.error):
    pass

BUF_LEN = 40960
TIME_OUT = 10

# memcached:250
MAX_KEY_LEN = 250

LINE_DELIMITER = "\r\n"
DELIMITER_LEN = len(LINE_DELIMITER)
TERMINATOR = "END\r\n"
TERMINATOR_LEN = len(TERMINATOR)

OPT_FLAG    = 'flag'
OPT_EXPIRE  = 'expire'
OPT_NOREPLY = 'noreply'
OPT_SYNC    = 'sync'
OPT_CAS     = 'cas'

class Client(object):
    def __init__(self, server, debug=False):
        self._debug = debug
        self._server = memc.conn2tuple(server)
        self._sock = None
        self._buf = str()
        
    def connect(self, force=False):
        if self._sock == None or force:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 0) 
            self._sock.settimeout(TIME_OUT)
            self._sock.connect(self._server)
            
        self.version()

    def _send_cmd(self, cmd):
        self._send_cmds([cmd, LINE_DELIMITER])

    def _send_cmds(self, cmds):
        self._sock.send("".join(cmds))

    def _recv(self):
        buf = self._sock.recv(BUF_LEN)
        
        # This is adhoc code.
        # We should find propery TCP flags or something.
        if not buf:
            raise SocketError('No data received.')
        
        self._buf += buf
        # print "len:%d " % len(buf)
        

    def _readline(self):
        while(True):
            pos = self._buf.find(LINE_DELIMITER)
            if(pos >= 0):
                break
            self._recv()

        line = self._buf[:pos]
        self._buf = self._buf[pos + DELIMITER_LEN:]
        
        return line
        
    def _read(self, size):
        while(True):
            if(len(self._buf) >= size + DELIMITER_LEN):
                break
            self._recv()
            
        result = self._buf[:size]
        self._buf = self._buf[size + DELIMITER_LEN:]
        
        return result
    
    def _send_readline(self, buf):
        self._send_cmd(buf)
        return self._readline()

    def _check_key(self, key):
        if len(key) > MAX_KEY_LEN:
            raise Error("Too long key: %s" % key)
        
        for c in key:
            h = ord(c)
            if h <= 0x20 or h == 0x7f:
                raise Error("Key must never include white spaces:%s" % key)
        
        return

    def stats(self, arg=""):
        stats = {}

        cmdline = "stats %s" % (arg)
        
        self._send_cmd(cmdline)

        while(True):
            line = self._readline()
        
            if line.startswith('STAT '):
                pass
            
            elif line == 'END':
                break

            else:
                raise Error("Argument error: stats %s" % arg)
        
            (cmd, k, v) = line.split(None, 2)
            stats[k] = v
         
        return stats   


    def version(self):  
        return self._send_readline('version')

    def close(self):
        self._send_cmd('quit')
        

    def _set(self, cmd, key, value, kwargs={}):
        expire = 0
        flag = 0
        opt = []
        cas = None
        noreply = False
        
        self._check_key(key)
        
        if kwargs.has_key(OPT_EXPIRE):
            expire = kwargs[OPT_EXPIRE]
            
        if kwargs.has_key(OPT_FLAG):
            flag = kwargs[OPT_FLAG]

        if kwargs.has_key(OPT_NOREPLY) and kwargs[OPT_NOREPLY]:
            opt.append(OPT_NOREPLY)
            noreply = True
        
        if kwargs.has_key(OPT_SYNC) and kwargs[OPT_SYNC]:
            opt.append(OPT_SYNC)
        
        if kwargs.has_key(OPT_CAS):
            cas = kwargs[OPT_CAS]

        if type(value) != str:
            value = str(value)

        if cmd == 'cas':
            cmdline = "%s %s %d %d %d %d %s\r\n%s" % \
                       (cmd, key, flag, expire, len(value), cas, " ".join(opt), value)
        else:
            cmdline = "%s %s %d %d %d %s\r\n%s" % \
                       (cmd, key, flag, expire, len(value), " ".join(opt), value)
        
        if noreply:
            self._send_cmd(cmdline)
            return
        else:
            result = self._send_readline(cmdline)
        
        if result == "STORED":
            pass

        elif result == "NOT_STORED":
            raise StoreError("store error:%s" % key)

        else:
            raise Error("Unknown error:%s" % result)
        
        return
        
        
    def _get(self, cmd, keys, use_cas=False):
        results = {}
        
        for key in keys:
            self._check_key(key)

        cmdline = "%s %s\r\n" % (cmd, " ".join(keys))
        
        self._sock.send(cmdline)

        while(True):
            line = self._readline()

            if line == "END":
                break
            
            elif not line.startswith("VALUE "):
                raise Error("Unknown error: %s - %s" % (cmdline, line))
                
            if use_cas:
                (cmd, key, flags, bytes, cas) = line.split()
            else:
                (cmd, key, flags, bytes) = line.split()
                cas = None
            
            bytes = int(bytes)
            flags = int(flags)
            if cas != None:
                cas = int(cas)

            results[key] = (self._read(bytes), key, flags, bytes, cas)

        return results

    def _incr_decr(self, cmd, key, value, kwargs={}):
        opt = []
        noreply = False
        
        self._check_key(key)
        
        if not 0 <= value:
            raise Error("Value must be integer.") 

        
        if kwargs.has_key(OPT_NOREPLY) and kwargs[OPT_NOREPLY]:
            opt.append(OPT_NOREPLY)
            noreply = True
        
        if kwargs.has_key(OPT_SYNC) and kwargs[OPT_SYNC]:
            opt.append(OPT_SYNC)

        cmdline = "%s %s %d %s" % \
                    (cmd, key, value, " ".join(opt))

        if noreply:
            self._send_cmd(cmdline)
            return
        else:
            result = self._send_readline(cmdline)
        
        if result.isdigit():
            value = int(result)
            
        elif result == "NOT_FOUND":
            raise KeyNotFoundError("key:%s is not found." % key)
        
        elif result.startswith('CLIENT_ERROR'):
            raise Error(result)
        
        else:
            raise Error("Unknown error:%s" % result)
        
        return value
        

    def _delete(self, key, kwargs={}):
        opt = []
        noreply = False
        
        self._check_key(key)

        if kwargs.has_key(OPT_NOREPLY) and kwargs[OPT_NOREPLY]:
            opt.append(OPT_NOREPLY)
            noreply = True

        cmdline = "delete %s %s" % (key, " ".join(opt))
        
        if noreply:
            self._send_cmd(cmdline)
            return
        else:
            result = self._send_readline(cmdline)
        
        if result == 'DELETED':
            pass
        
        elif result ==  "NOT_FOUND":
            raise KeyNotFoundError("key:%s is not found." % key)
        
        else:
            raise Error("Unknown error:%s" % result)
        
        return
    
    
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
        kwargs[OPT_CAS] = cas
        return self._set('cas', key, value, kwargs)

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
        result = self._get("get", keys)
        lst = []
        for key in keys:
            if result.has_key(key):
                lst.append(result[key][0])
            else:
                lst.append(None)
        return lst

    def raw_mgets(self, keys):
        return self._get("gets", keys, True)

    def incr(self, key, value, **kwargs):
        return self._incr_decr('incr', key, value, kwargs)

    def decr(self, key, value, **kwargs):
        return self._incr_decr('decr', key, value, kwargs)

    def get(self, key):
        return self.raw_get(key)[0]

    def mget(self, keys):
        return self.raw_mget(keys)
        
if __name__ == "__main__":
    pass
