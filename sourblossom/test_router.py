'''
Created on 14.09.2015

@author: stefan
'''
import unittest
import utwist

from twisted.internet import defer

from . import router, blob
import twistit

class TestRouter(unittest.TestCase):
    
    @utwist.with_reactor    
    @twistit.yieldefer
    def test(self):
        
        a = StringMsgRouter(4000)
        b = StringMsgRouter(4001)
        yield a.listen()
        yield b.listen()
        
        yield a.send(("localhost", 4001), 42, blob.StringBlob("hello"))
        
        frameid, data = yield b.next();
        
        self.assertEqual(42, frameid)
        self.assertEqual("hello", data)
        
        yield a.shutdown()
        yield b.shutdown()


class StringMsgRouter(router.MsgRouter):
    
    def __init__(self, port):
        router.MsgRouter.__init__(self, ("me", port))
        self.queue = defer.DeferredQueue()
    
    def frame_received(self, frameid, blob):
        d = blob.read_all()
        def got_all(data):
            self.queue.put((frameid, data))
        d.addCallback(got_all)
        
    def next(self):
        return self.queue.get()