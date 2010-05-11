'''
Created on 2010/03/08

@author: Masahiro Fukuda
'''

import re

class Error(Exception):
    """ Base class for errors in the email package. """
    
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
    
class StoreError(Error):
    pass

class KeyNotFoundError(Error):
    pass

    
_reg_server = re.compile("^(?P<host>[a-z0-9\-\_\.]+)(?::(?P<port>[0-9]+))?$")
def conn2tuple(cons):
    if type(cons) == tuple:
        return cons

    m = _reg_server.match(cons)
    if not m:
        raise Error("connection string is invalid:%s" % cons)

    return (m.group('host'), int(m.group('port')))
