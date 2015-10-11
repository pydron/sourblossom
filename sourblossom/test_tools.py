import unittest

from . import tools
import collections

class TestSplitter(unittest.TestCase):
    
    def setUp(self):
        self.target = tools.Splitter()
        self.received = []
        self.requests = None
        
        def handler(data):
            self.received.append(data)
            if self.requests:
                min_count, max_count = self.requests.popleft()
                self.target.request(handler, min_count, max_count)
        self.handler = handler

    def request(self, min_count, max_count):
        if self.requests is not None:
            self.requests.append((min_count, max_count))
        else:
            self.requests = collections.deque()
            self.target.request(self.handler, min_count, max_count)
    
    def test_single_bytes(self):
        self.request(1, 1)
        self.target.write("abc")
        self.assertEqual(['a', 'b', 'c'], self.received)
        
    def test_change_request(self):
        self.request(1, 1)
        self.request(2, 2)
        self.target.write("abc")
        self.assertEqual(['a', 'bc'], self.received)
        
    def test_combine(self):
        self.request(4, 4)
        self.target.write("ab")
        self.target.write("cd")
        self.assertEqual(["abcd"], self.received)
        
    def test_match(self):
        self.request(4, 4)
        self.target.write("abcd")
        self.assertEqual(["abcd"], self.received)
        
    def test_minimal(self):
        self.request(4, 8)
        self.target.write("abcd")
        self.target.write("1234")
        self.assertEqual(["abcd", "1234"], self.received)
        
    def test_maximal(self):
        self.request(4, 8)
        self.target.write("abcd1234")
        self.assertEqual(["abcd1234"], self.received)
        
    def test_too_much(self):
        self.request(1,4)
        self.target.write("abcd12")
        self.assertEqual(["abcd", "12"], self.received)


class AbstractConsumer(unittest.TestCase):
    
    def stopProducing(self):
        self.assertFalse(self.stopped, "Already stopped")
        self.stopped = True
    
    def pauseProducing(self):
        self.assertFalse(self.paused, "Already paused")
        self.paused = True
    
    def resumeProducing(self):
        self.assertTrue(self.paused, "Not paused")
        self.paused = False
    
    def setUp(self):
        self.stopped = False
        self.paused = False
        self.target = tools.AbstractConsumer()
    
    def test_pause(self):
        self.target.registerProducer(self, True)
        self.target._pause()
        self.assertTrue(self.paused)
        
    def test_resume(self):
        self.target.registerProducer(self, True)
        self.target._pause()
        self.target._resume()
        self.assertFalse(self.paused)
        
    def test_pause_before_reg(self):
        self.target._pause()
        self.target.registerProducer(self, True)
        self.assertTrue(self.paused)
        
    def test_unreg_unpauses(self):
        self.target.registerProducer(self, True)
        self.target._pause()
        self.target.unregisterProducer()
        self.assertFalse(self.paused)
    
#     def test_double_pause(self):
#         self.target.registerProducer(self, True)
#         self.target._pause()
#         self.target._pause()
#         self.assertTrue(self.paused)
#         
#     def test_double_pause_single_resume(self):
#         self.target.registerProducer(self, True)
#         self.target._pause()
#         self.target._pause()
#         self.target._resume()
#         self.assertTrue(self.paused)
#         
#     def test_double_pause_double_resume(self):
#         self.target.registerProducer(self, True)
#         self.target._pause()
#         self.target._pause()
#         self.target._resume()
#         self.target._resume()
#         self.assertFalse(self.paused)
#         
#     def test_double_pause_tripple_resume(self):
#         self.target.registerProducer(self, True)
#         self.target._pause()
#         self.target._pause()
#         self.target._resume()
#         self.target._resume()
#         self.assertRaises(ValueError,  self.target._resume)
        
    def test_register_twice(self):
        self.target.registerProducer(tools.DummyProducer(), True)
        self.assertRaises(ValueError, 
                          self.target.registerProducer, 
                          tools.DummyProducer(), 
                          True)
        
    def test_unregister_unregistered(self):
        self.assertRaises(ValueError, self.target.unregisterProducer)
        
    def test_unregister_twice(self):
        self.target.registerProducer(tools.DummyProducer(), True)
        self.target.unregisterProducer()
        self.assertRaises(ValueError, self.target.unregisterProducer)
        