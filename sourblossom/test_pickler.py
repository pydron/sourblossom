'''
Created on 21.09.2015

@author: stefan
'''

import unittest
import utwist
import twistit
from twisted.internet import reactor, threads
from sourblossom import tools, pickler
import pickle
import numpy

class TestPickler(unittest.TestCase):
    
    @utwist.with_reactor
    @twistit.yieldefer
    def test_str(self):
        obj = "test"
        target = pickler.PickleDumpBlob(obj)
        expected = pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)
        actual = yield target.read_all()
        self.assertEqual(expected, actual)
        
    @utwist.with_reactor
    @twistit.yieldefer
    def test_numpy(self):
        obj = numpy.zeros(10)
        target = pickler.PickleDumpBlob(obj)
        expected = pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)
        actual = yield target.read_all()
        self.assertEqual(expected, actual)
        

class TestFilePushProducer(unittest.TestCase):
    
    def setUp(self):
        self.consumer = self.DummyConsumer()
    
    @utwist.with_reactor
    @twistit.yieldefer
    def test_empty(self):
        target = pickler.FilePushProducer(0, self.consumer, reactor)
        
        def thread():
            target.close()
        yield threads.deferToThread(thread)
        
        self.assertEqual([], self.consumer.parts)
        
    @utwist.with_reactor
    @twistit.yieldefer
    def test_one_write(self):
        target = pickler.FilePushProducer(5, self.consumer, reactor)
        
        def thread():
            target.write("Hello")
            target.close()
        yield threads.deferToThread(thread)
        
        self.assertEqual(["Hello"], self.consumer.parts)
            
    @utwist.with_reactor
    @twistit.yieldefer
    def test_two_writes(self):
        target = pickler.FilePushProducer(12, self.consumer, reactor)
        
        def thread():
            target.write("Hello")
            target.write(" World!")
            target.close()
        yield threads.deferToThread(thread)
        
        self.assertEqual("Hello World!", "".join(self.consumer.parts))
        
    @utwist.with_reactor
    @twistit.yieldefer
    def test_write_too_little(self):
        target = pickler.FilePushProducer(13, self.consumer, reactor)
        
        def thread():
            target.write("Hello")
            target.write(" World!")
            target.close()
        d = threads.deferToThread(thread)
        try:
            yield d
            self.fail("Expected IOError")
        except IOError:
            pass
        
    @utwist.with_reactor
    @twistit.yieldefer
    def test_write_too_much(self):
        target = pickler.FilePushProducer(11, self.consumer, reactor)
        
        def thread():
            target.write("Hello")
            target.write(" World!")
            target.close()
        d = threads.deferToThread(thread)
        try:
            yield d
            self.fail("Expected IOError")
        except IOError:
            pass
        
        
    class DummyConsumer(tools.AbstractConsumer):
        def __init__(self):
            tools.AbstractConsumer.__init__(self)
            self.parts = []
        def write(self, data):
            self.parts.append(data)