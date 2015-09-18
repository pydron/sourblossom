'''
Created on 14.09.2015

@author: stefan
'''

import unittest
from . import pool
from twisted.internet import defer
import twistit

class TestPool(unittest.TestCase):
    
    def setUp(self):
        
        self.disposed = set()
        
        self.next_v = 0
        
        def factory():
            v = self.next_v
            self.next_v += 1
            return defer.succeed(v)
            
        def disposer(v):
            self.disposed.add(v)
            return defer.succeed(None)
        
        self.target = pool.Pool(factory, disposer)

    def test_acquire(self):
        self.assertEqual(0, twistit.extract(self.target.acquire()))
        
    def test_acquire_two(self):
        self.assertEqual(0, twistit.extract(self.target.acquire()))
        self.assertEqual(1, twistit.extract(self.target.acquire()))
        
    def test_release(self):
        v = twistit.extract(self.target.acquire())
        self.target.release(v)
        
    def test_a_r_a(self):
        v = twistit.extract(self.target.acquire())
        self.target.release(v)
        w = twistit.extract(self.target.acquire())
        self.assertEqual(v, w)
        
    def test_release_other_obj(self):
        self.assertRaises(ValueError, self.target.release, "hello")
        
    def test_dispose(self):
        v1 = twistit.extract(self.target.acquire())
        self.target.dispose(v1)
        v2 = twistit.extract(self.target.acquire())
        self.assertEqual(1, v2)
        self.assertEqual({0}, self.disposed)
        
    def test_dispose_all(self):
        v1 = twistit.extract(self.target.acquire())
        v2 = twistit.extract(self.target.acquire())
        self.target.release(v1)
        self.target.release(v2)
        self.target.dispose_all()
        self.assertEqual({v1,v2}, self.disposed)
        
class TestKeyedPool(unittest.TestCase):
    
    def setUp(self):
        
        self.disposed = set()
        
        self.next_v = 0
        
        def factory(key):
            v = self.next_v
            self.next_v += 1
            return defer.succeed((key,v))
            
        def disposer(v):
            self.disposed.add(v)
            return defer.succeed(None)
        
        self.target = pool.KeyedPool(factory, disposer)
        
    def test_acquire(self):
        self.assertEqual(('a',0), twistit.extract(self.target.acquire('a')))
        
    def test_acquire_differentkeys(self):
        self.assertEqual(('a',0), twistit.extract(self.target.acquire('a')))
        self.assertEqual(('b',1), twistit.extract(self.target.acquire('b')))
        
    def test_dont_share(self):
        v1 = twistit.extract(self.target.acquire('a'))
        self.target.release(v1)
        self.assertEqual(('b',1), twistit.extract(self.target.acquire('b')))
        
    def test_a_r_a(self):
        v = twistit.extract(self.target.acquire('a'))
        self.target.release(v)
        w = twistit.extract(self.target.acquire('a'))
        self.assertEqual(v, w)
        
    def test_dispose(self):
        twistit.extract(self.target.acquire('a'))
        v2 = twistit.extract(self.target.acquire('a'))
        twistit.extract(self.target.acquire('a'))
        self.target.dispose(v2)
        self.assertEqual({v2}, self.disposed)
        
    def test_dispose_all(self):
        v1 = twistit.extract(self.target.acquire('a'))
        v2 = twistit.extract(self.target.acquire('b'))
        self.target.release(v1)
        self.target.release(v2)
        self.target.dispose_all()
        self.assertEqual({v1, v2}, self.disposed)