import unittest
from sourblossom import blob, tools
import twistit
from twisted.python import failure


class TestPassthroughBlob(unittest.TestCase):
    
    def setUp(self):
        testcase = self
        
        self.paused = False
        self.stopped = False
        self.producer = None
        self.received = []

        class DummyProducer(object):
            
            def pauseProducing(self):
                testcase.paused = True
                
            def resumeProducing(self):
                testcase.paused = False
                
            def stopProducing(self):
                testcase.stopped = True
            
        class DummyConsumer(object):
            
            def write(self, data):
                testcase.received.append(data)
            
            def registerProducer(self, producer, streaming):
                assert streaming is True
                testcase.producer = producer
            
            def  unregisterProducer(self):
                testcase.producer = None
            
        self.target = blob.PassthroughBlob(100)
        self.target.registerProducer(DummyProducer(), True)
        self.consumer = DummyConsumer()
        
    
    def test_early_write_pauses(self):
        self.target.write("abc")
        self.target.write("def")
        self.target.write("123")
        self.assertTrue(self.paused)
        
    def test_early_write_resumes(self):
        self.target.write("abc")
        self.target.write("def")
        self.target.write("123")
        self.target.write_to(self.consumer)
        self.assertFalse(self.paused)
        
    def test_buffer_sent(self):
        self.target.write("abc")
        self.target.write_to(self.consumer)
        self.assertEqual(["abc"], self.received)
        
    def test_forward(self):
        self.target.write_to(self.consumer)
        self.target.write("abc")
        self.assertEqual(["abc"], self.received)
        
    def test_consumer_pauses(self):
        self.target.write_to(self.consumer)
        self.producer.pauseProducing()
        self.assertTrue(self.paused)
        
    def test_consumer_resumes(self):
        self.target.write_to(self.consumer)
        self.producer.pauseProducing()
        self.producer.resumeProducing()
        self.assertFalse(self.paused)
        
    def test_consumer_stop(self):
        self.target.write_to(self.consumer)
        self.producer.stopProducing()
        self.assertTrue(self.stopped)
        
    def test_done(self):
        self.target._size = 0
        self.target.write_to(self.consumer)
        self.target.done()
        self.assertIsNone(self.producer)
        
    def test_done_early(self):
        self.target._size = 0
        self.target.done()
        self.target.write_to(self.consumer)
        
    def test_defer_callsback(self):
        self.target._size = 3
        self.target.write("abc")
        d = self.target.write_to(self.consumer)
        self.target.done()
        twistit.extract(d)
        
    def test_defer_callsback_early(self):
        self.target._size = 3
        self.target.write("abc")
        self.target.done()
        d = self.target.write_to(self.consumer)
        twistit.extract(d)
        
    def test_read_all(self):
        self.target._size = 6
        self.target.write("abc")
        d = self.target.read_all()
        self.target.write("123")
        self.target.done()
        actual = twistit.extract(d)
        self.assertEqual("abc123", actual)
        
    def test_oversized(self):
        self.target._size = 2
        self.assertRaises(ValueError, self.target.write, "abc")
        
    def test_undersized(self):
        self.target._size = 3
        self.target.write("ab")
        self.assertRaises(ValueError, self.target.done)
        
    def test_fail_late_write_to(self):
        self.target.write("abc")
        self.target.fail(failure.Failure(IOError()))
        d = self.target.write_to(self.consumer)
        self.assertRaises(IOError, twistit.extract, d)
        
    def test_fail_early_write_to(self):
        d = self.target.write_to(self.consumer)
        self.target.write("abc")
        self.target.fail(failure.Failure(IOError()))
        self.assertRaises(IOError, twistit.extract, d)
        
    def test_concat_two_blobs(self):
        target = blob.PassthroughBlob(11, "target")
        d = target.read_all()
        
        a = blob.StringBlob("hello")
        a.write_to(target)
        b = blob.PassthroughBlob(6, "b")
        b.write_to(target)
        b.write("abc")
        b.write("def")
        b.done()
        target.done()
        
        actual = twistit.extract(d)
        self.assertEqual("helloabcdef", actual)
        
    def test_concat_string_blob(self):
        target = blob.PassthroughBlob(11, "target")
        d = target.read_all()
        target.write("hello")
        b = blob.PassthroughBlob(6, "b")
        b.write_to(target)
        b.write("abc")
        b.write("def")
        b.done()
        target.done()
        
        actual = twistit.extract(d)
        self.assertEqual("helloabcdef", actual)
        

class TestStringListBlob(unittest.TestCase):
    
    def test_data(self):
        target = blob.StringListBlob(["Hello", " ", "World", "!"])
        actual = twistit.extract(target.read_all())
        self.assertEqual("Hello World!", actual)
        
    def test_pause_resume(self):
        
        class Consumer(tools.AbstractConsumer):
            
            def __init__(self):
                tools.AbstractConsumer.__init__(self)
                self.data = None
            
            def write(self, data):
                if self.data is not None:
                    raise ValueError("Write before next()")
                self.data = data
                self._pause()
                
            def next(self):
                data = self.data
                self.data = None
                self._resume()
                return data
            
        target = blob.StringListBlob(["Hello", " ", "World", "!"])
        consumer = Consumer()
        target.write_to(consumer)
        
        self.assertEqual("Hello", consumer.next())
        self.assertEqual(" ", consumer.next())
        self.assertEqual("World", consumer.next())
        self.assertEqual("!", consumer.next())
    
    
class TestThrottleBlob(unittest.TestCase):
    
    class Consumer(tools.AbstractConsumer):
        
        def __init__(self):
            tools.AbstractConsumer.__init__(self)
            self.received = []
        
        def write(self, data):
            self.received.append(data)
            
    def setUp(self):
        self.datablocks = ["."*10]*10
        self.clock = twistit.TimeMock()
        self.src = blob.StringListBlob(self.datablocks)
        self.target = blob.ThrottleBlob(self.src, 2, self.clock)
        self.dest = self.Consumer()
        
    def test_waits_after_first_write(self):
        self.target.write_to(self.dest)
        self.assertEqual(1, len(self.dest.received))
        
    def test_not_too_fast(self):
        self.target.write_to(self.dest)
        self.clock.advanceTime(4.99)
        self.assertEqual(1, len(self.dest.received))
        
    def test_not_too_slow(self):
        self.target.write_to(self.dest)
        self.clock.advanceTime(5.01)
        self.assertEqual(2, len(self.dest.received))
        
    def test_not_too_fast2(self):
        self.target.write_to(self.dest)
        self.clock.advanceTime(9.99)
        self.assertEqual(2, len(self.dest.received))
        
    def test_not_too_slow2(self):
        self.target.write_to(self.dest)
        self.clock.advanceTime(10.01)
        self.assertEqual(3, len(self.dest.received))
        
    def test_done(self):
        d = self.target.write_to(self.dest)
        self.clock.advanceTime(5*9)
        self.assertTrue(d.called)
        
    def test_not_done(self):
        d = self.target.write_to(self.dest)
        self.clock.advanceTime(5*9-0.01)
        self.assertFalse(d.called)
        
    def test_buffer(self):
        self.target.write_to(self.dest)
        self.clock.advanceTime(5*9)
        self.assertLessEqual(self.target.max_buffer_size(), 10)
        