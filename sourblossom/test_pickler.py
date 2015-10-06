'''
Created on 21.09.2015

@author: stefan
'''

import unittest
import utwist
import twistit
from twisted.internet import reactor, threads
from sourblossom import tools, pickler, blob
import pickle
import numpy
import time

class TestPickler(unittest.TestCase):
    
    @utwist.with_reactor
    @twistit.yieldefer
    def test_str_small(self):
        obj = "test"
        target = pickler.PickleDumpBlob(obj)
        expected = pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)
        actual = yield target.read_all()
        self.assertEqual(expected, actual)
        
    @utwist.with_reactor
    @twistit.yieldefer
    def test_numpy_small(self):
        obj = numpy.zeros(10)
        target = pickler.PickleDumpBlob(obj)
        expected = pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)
        actual = yield target.read_all()
        self.assertEqual(expected, actual)
        
    @utwist.with_reactor
    @twistit.yieldefer
    def test_large(self):
        obj = ["test"] * 100000
        target = pickler.PickleDumpBlob(obj)
        expected = pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)
        actual = yield target.read_all()
        self.assertEqual(expected, actual)
        
    @utwist.with_reactor
    @twistit.yieldefer
    def test_large_numpy(self):
        obj = numpy.zeros(10*1024*1024)
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
          
        
class TestLoadsFromBlob(unittest.TestCase):
    
    @utwist.with_reactor
    @twistit.yieldefer
    def test_str(self):
        src = blob.StringBlob(pickle.dumps("test", pickle.HIGHEST_PROTOCOL))
        actual = yield pickler.loads_from_blob(src)
        self.assertEqual("test", actual)
        
    @utwist.with_reactor
    @twistit.yieldefer
    def test_list(self):
        src = blob.StringBlob(pickle.dumps(["test",42], pickle.HIGHEST_PROTOCOL))
        actual = yield pickler.loads_from_blob(src)
        self.assertEqual(["test",42], actual)
        
    @utwist.with_reactor
    @twistit.yieldefer
    def test_roundtrip(self):
        obj= numpy.zeros(10)
        src = pickler.PickleDumpBlob(obj)
        actual = yield pickler.loads_from_blob(src)
        self.assertTrue(numpy.array_equal(obj, actual))
 
        
class TestBlockingConsumer(unittest.TestCase):
    
    def setUp(self):
        self.target = pickler.BlockingConsumer()
        self.target.min_queue = 0
        self.target.max_queue = 10
        
        class Producer(tools.DummyProducer):
            def __init__(self):
                self.paused = False
            def pauseProducing(self):
                self.paused = True
            def resumeProducing(self):
                self.paused = False
            def stopProducing(self):
                pass
            
        self.producer = Producer()
        self.target.registerProducer(self.producer, True)
        
    @utwist.with_reactor
    @twistit.yieldefer
    def test_wr(self):
        self.target.write("hello")
        actual = yield threads.deferToThread(self.target.read)
        self.assertEqual("hello", actual)
        
    @utwist.with_reactor
    @twistit.yieldefer
    def test_rw(self):
        d = threads.deferToThread(self.target.read)
        time.sleep(0.05)
        self.target.write("hello")
        actual = yield d
        self.assertEqual("hello", actual)
        
    def test_pause(self):
        self.target.write("."*5)
        self.target.write("."*5)
        self.assertTrue(self.target.producer.paused)
        
    def test_pause_onlyonce(self):
        self.target.write("."*5)
        self.target.write("."*5)
        self.target.write("."*5)
        self.target.write("."*5)
        self.assertTrue(self.target.producer.paused)
        
    @utwist.with_reactor
    @twistit.yieldefer
    def test_resume(self):
        self.target.write("."*5)
        self.target.write("."*5)
        self.target.write("."*5)
        yield threads.deferToThread(self.target.read)
        yield threads.deferToThread(self.target.read)
        yield threads.deferToThread(self.target.read)
        self.assertFalse(self.target.producer.paused)
        
class TestBufferedReader(unittest.TestCase):
    def setUp(self):
        me = self
        class Src(object):
            def read(self):
                return me.data.pop(0)
        self.target = pickler.BufferedReader(Src())
        self.data = []

    def test_read_equal(self):
        self.data = ["hello"]
        self.assertEqual("hello", self.target.read(5))
        
    def test_read_small(self):
        self.data = ["hello"]
        self.assertEqual("h", self.target.read(1))
        self.assertEqual("e", self.target.read(1))
        self.assertEqual("l", self.target.read(1))
        self.assertEqual("l", self.target.read(1))
        self.assertEqual("o", self.target.read(1))
        
    def test_merge(self):
        self.data = ["abc", "123"]
        self.assertEqual("abc123", self.target.read(6))
    
    def test_leftovers(self):
        self.data = ["abc", "123"]
        self.assertEqual("abc1", self.target.read(4))
        self.assertEqual("23", self.target.read(2))
        
    def test_split(self):
        self.data = ["abc", "123"]
        self.assertEqual("ab", self.target.read(2))
        self.assertEqual("c1", self.target.read(2))
        self.assertEqual("23", self.target.read(2))
    
    def test_readline(self):
        self.data = ["abc\n"]
        self.assertEqual("abc\n", self.target.readline())
        
    def test_twolines(self):
        self.data = ["abc\n123\n"]
        self.assertEqual("abc\n", self.target.readline())
        self.assertEqual("123\n", self.target.readline())
    
    def test_late_newline(self):
        self.data = ["abc", "123\n"]
        self.assertEqual("abc123\n", self.target.readline())
        
    def test_afterline(self):
        self.data = ["abc", "123\nxyz"]
        self.assertEqual("abc123\n", self.target.readline())
        self.assertEqual("xyz", self.target.read(3))
        
    def test_startline(self):
        self.data = ["abc\n123\nx"]
        self.target.read(4)
        self.assertEqual("123\n", self.target.readline())
        
    