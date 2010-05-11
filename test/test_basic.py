'''
Created on 2010/03/12

@author: Masahiro Fukuda
'''

import unittest
import socket
import memc.basic


class TestBasic(unittest.TestCase):
    def setUp(self):
        self.server = ('127.0.0.1', 11211)
        self.dummy_server = ('_no_existent_host_', 11211)
        
        self.key = '_hoge'
        self.data = self.key * 2000
        
        self.mc = memc.basic.Client(self.server)
        self.mc.connect()

        #self.mc._set_buf_len(20)
        #memc.basic.BUF_LEN = 30

    def tearDown(self):
        self.mc.close()
    
    
    def test_version(self):
        v = self.mc.version()
        #self.assertEqual(v, 'VERSION 1.4.4')
        self.assertEqual(type(v), str, v)
        
    def test_stats(self):
        s = self.mc.stats()
        self.assertTrue(type(s) == dict and len(s) > 0)
        
    def test_set_get(self):
        flag = 0x11
        
        self.assertEqual(self.mc.set(self.key, self.data, flag=flag), None)
        result = self.mc.raw_get(self.key)
        self.assertEqual(self.data, result[0])
        self.assertEqual(flag, result[2])
        self.assertEqual(len(self.data), result[3])
        self.mc.delete(self.key, noreply=True)

    def test_add(self):
        def func():
            return self.mc.add(self.key, self.data)
        
        self.mc.delete(self.key, noreply=True)
        self.assertEqual(func(), None)
        self.assertRaises(memc.basic.StoreError, func)
        self.mc.delete(self.key, noreply=True)
        
    def test_replace(self):
        data = 'abc'
        
        def func():
            return self.mc.replace(self.key, self.data)
        
        self.mc.delete(self.key, noreply=True)
        self.assertRaises(memc.basic.StoreError, func)
        
        self.assertEqual(self.mc.add(self.key, self.data), None)
        self.assertEqual(self.mc.replace(self.key, data), None)
        
        result = self.mc.raw_get(self.key)
        self.assertEqual(data, result[0])
        
        
    def test_append_prepend(self):
        append  = '__append' * 1000
        prepend = '__prepend' * 1000
        
        self.mc.delete(self.key, noreply=True) 
        self.mc.set(self.key, self.data)
        
        self.mc.append(self.key, append)
        self.mc.prepend(self.key, prepend)
        
        result = self.mc.raw_get(self.key)
        
        self.assertEqual("".join((prepend, self.data, append)), result[0])

    def test_append_error(self):
        def func():
            self.mc.delete(self.key, noreply=True)
            self.mc.append(self.key, self.data)
        
        self.assertRaises(memc.basic.StoreError, func)
        
    def test_prepend_error(self):
        def func():
            self.mc.delete(self.key, noreply=True)
            self.mc.prepend(self.key, self.data)
        
        self.assertRaises(memc.basic.StoreError, func)
        
    def test_get_error(self):
        def func():
            self.mc.delete(self.key, noreply=True)
            self.mc.raw_get(self.key)
            
        self.assertRaises(memc.basic.KeyNotFoundError, func)
        
    
    def test_gets_cas(self):
        self.mc.delete(self.key, noreply=True)
        self.mc.set(self.key, '')
        result = self.mc.raw_gets(self.key)
        self.mc.cas(self.key, self.data, result[4])
        result = self.mc.raw_get(self.key)
        
        self.assertEqual(self.data, result[0])
        
    def test_buf_len(self):
        self.mc.set(self.key, self.data)
        
        for s in xrange(1, len(self.data) * 2):
            memc.basic.BUF_LEN = s
            result = self.mc.raw_get(self.key)
            self.assertEqual(self.data, result[0])
            
            
    def test_key_len(self):
        for l in xrange(1, 250):
            k = 'a' * l
            self.mc.set(k, self.data)
            result = self.mc.raw_get(k)
            self.assertEqual(self.data, result[0])
            
    def test_key_check(self):
        def t1():
            k = "a" * 251
            self.mc.set(k, self.data) 

        def t2():
            k = "a b"
            self.mc.set(k, self.data) 

        def t3():
            k = "   "
            self.mc.set(k, self.data) 
            
        self.assertRaises(memc.basic.Error, t1)
        self.assertRaises(memc.basic.Error, t2)
        self.assertRaises(memc.basic.Error, t3)


        k = str()
        for k1 in xrange(0x21, 0x7e):
            k += chr(k1)
        
        self.mc.set(k, self.data)
        result = self.mc.raw_get(k)
        self.assertEqual(self.data, result[0])


    def test_connect_close(self):
        def func():
            mc = memc.basic.Client(self.server)
            mc.connect()
            mc.close()
            
        for i in xrange(100000):
            func()
            
 
    def test_incr_decr(self):
        def t1():
            self.mc.set(self.key, 'a')
            self.mc.incr(self.key, 1)

        def t2():
            self.mc.delete(self.key, noreply=True)
            self.mc.incr(self.key, 1)
            
        self.assertRaises(memc.basic.KeyNotFoundError, t2)
        
        a = 10
        b = 5
        self.mc.set(self.key, str(a))
        self.assertEqual(self.mc.incr(self.key, b), a + b)
        
        amount = 0
        n = 10000
        self.mc.set(self.key, '0')
        for i in xrange(n):
            amount += i
            self.assertEqual(self.mc.incr(self.key, i), amount)
        
        for i in xrange(n):
            amount -= i
            self.assertEqual(self.mc.decr(self.key, i), amount)
        
        
        self.mc.set(self.key, '10')
        self.assertEqual(self.mc.decr(self.key, 11), 0)

    def test_incr_decr2(self):
        ver = self.mc.version()
        
        num = 0xFFFFFFFFFFFFFFFF # 64bit-max

        def t1():
            self.mc.set(self.key, str(num + 1))
            self.mc.decr(self.key, 1)

        if ver.startswith('VERSION 1.0.'): # if flare
            return

        self.assertRaises(memc.basic.Error, t1)
    
        self.mc.set(self.key, str(num))
        self.assertEqual(self.mc.incr(self.key, 1), 0)
    
    
    def test_mget_mgets(self):
        num = 1000
        keys = []
        data = 'D'
        
        for a in xrange(num):
            keys.append(str(a))
            self.mc.set(str(a), data * a)
        
        result = self.mc.raw_mgets(keys)
        
        for a in xrange(num):
            self.assertEqual(result[str(a)][0], data * a)
            del(result[str(a)])
            
        self.assertEqual(result, {})
        
        
        # -- mgets --
        
        result = self.mc.raw_mgets(keys)
        
        for a in xrange(num):
            self.mc.cas(str(a), data, result[str(a)][4])
        
    
    def test_connect(self):
        def func():
            mc = memc.basic.Client(self.dummy_server)
            mc.connect()
            
        self.assertRaises(socket.error, func)
    
    def test_close(self):
        def func():
            mc = memc.basic.Client(self.server)
            mc.connect()
            mc.close()
            mc.version()
            
        self.assertRaises(socket.error, func)
    
    
if __name__ == '__main__':
    #unittest.main()
    suite = unittest.TestLoader().loadTestsFromTestCase(TestBasic)
    unittest.TextTestRunner(verbosity=2).run(suite)
    
    
