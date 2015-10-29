'''
Created on 14.09.2015

@author: stefan
'''

from twisted.internet import defer
import collections

class Pool(object):
    """
    Async object pool that uses a factory to create objects as needed.
    """
    
    def __init__(self, factory, disposer):
        """
        :param factory: Callable, no args, returns a deferred for the
            created object.
        :param disposer: Callable, takes the object as the single arg.
            Returns a deferred to report completion.
        """
        self.factory = factory
        self.disposer = disposer
        self._pool = []
        self._pending_factory_calls = []
        self._inuse = []
        
    def add(self, value):
        self._pool.append(value)
        
    def acquire(self):
        """
        Get an element from the pool or create a new one if the pool is
        emtpy.
        
        Returns a deferred element.
        """
        if self._pool:
            v = self._pool.pop()
            self._inuse.append(v)
            return defer.succeed(v)
        else:
            d = self.factory()
            self._pending_factory_calls.append(d)
            
            def created(v):
                self._inuse.append(v)
                self._pending_factory_calls.remove(d)
                return v
            
            def failed(fail):
                self._pending_factory_calls.remove(d)
                return fail
            
            d.addCallbacks(created, failed)
            return d
    
    def release(self, v):
        """
        Hand back a previously acquired element to the pool.
        
        Returns None.
        """
        self._inuse.remove(v)
        self._pool.append(v)
        
    def dispose(self, v):
        """
        Dispose of a previously acquired element.
        
        Returns a deferred from the disposer.
        """
        self._inuse.remove(v)
        return self.disposer(v)
        
    def dispose_all(self):
        """
        Disposes all elements in the pool. Will fail if there
        are elements not yet returned.
        """
        if (self._inuse):
            raise ValueError("Not all elements have been released.")
        for d in self._pending_factory_calls:
            d.cancel()
        self._pending_factory_calls = []
        
        ds = [self.disposer(v) for v in self._pool]
        return defer.DeferredList(ds, fireOnOneErrback=True)
            
    
class KeyedPool(object):
    """
    Async object pool that uses a factory to create objects as needed.
    There is one pool per key.
    """
    
    def __init__(self, factory, disposer):
        """
        :param factory: Callable, taking key as arg, returns a deferred for the
            created object.
        :param disposer: Callable, takes the object as the single arg.
            Returns a deferred to report completion.
        """
        self.factory = factory
        self.disposer = disposer
        self._value_to_key = {}
        
        def make_pool(key):
            def factory():
                return self.factory(key)
            return Pool(factory, self.disposer)
        
        self._pools = KeyedDefaultdict(make_pool)
        
    def add(self, key, value):
        """
        Add additional value.
        """
        pool = self._pools[key]
        pool.add(value)
        
    def acquire(self, key):
        """
        Acquire a value for the given key.
        """
        pool = self._pools[key]
        d = pool.acquire()
        def success(v):
            self._value_to_key[v] = key
            return v
        d.addCallback(success)
        return d
    
    def release(self, v):
        """
        Hand back a previously acquired value.
        """
        key = self._value_to_key.pop(v)
        pool = self._pools[key]
        return pool.release(v)
    
    def dispose(self, v):
        """
        Dispose a previously acquired value.
        """
        key = self._value_to_key.pop(v)
        pool = self._pools[key]
        return pool.dispose(v)
    
    def dispose_all(self):
        """
        Disposes all elements in the pool. Will fail if there
        are elements not yet returned.
        """
        ds = [pool.dispose_all() for pool in self._pools.values()]
        return defer.DeferredList(ds, fireOnOneErrback=True)
    
    
class KeyedDefaultdict(collections.defaultdict):
    def __missing__(self, key):
        if self.default_factory:
            dict.__setitem__(self, key, self.default_factory(key))
            return self[key]
        else:
            collections.defaultdict.__missing__(self, key)
        