
import unittest

from . import framing, tools, blob
import twistit

class TestMergeFrames(unittest.TestCase):
    
    def setUp(self):
        me = self
        self.data = []
        class Consumer(tools.AbstractConsumer):
            def write(self, data):
                me.data.append(data)
        self.target = framing.MergeFrames(Consumer())
        self.header = framing._header

    def test_write_empty(self):
        self.target.write_blob(11, blob.StringBlob(""))
        self.assertEqual(self.header.pack(11, 0) + "", "".join(self.data))
        
    def test_write_one(self):
        self.target.write_blob(11, blob.StringBlob("abc"))
        self.assertEqual(self.header.pack(11, 3) + "abc", "".join(self.data))
        
    def test_write_two(self):
        self.target.write_blob(11, blob.StringBlob("abc"))
        self.target.write_blob(12, blob.StringBlob("1234"))
        
        msg1 = self.header.pack(11, 3) + "abc"
        msg2 = self.header.pack(12, 4) + "1234"
        
        self.assertEqual(msg1 + msg2, "".join(self.data))
class TestSplitFrames(unittest.TestCase):
    
    def setUp(self):
        self.target = framing.SplitFrames()
        self.target.registerProducer(tools.DummyProducer(), True)
        self.header = framing._header
    
    def test_empty_frame(self):
        self.target.write(self.header.pack(10, 0))
        fid, blob = twistit.extract(self.target.next_frame())
        self.assertEqual(10, fid)
        self.assertEqual("", twistit.extract(blob.read_all()))
        
    def test_small(self):
        self.target.write(self.header.pack(10, 4))
        self.target.write("abcd")
        _, blob = twistit.extract(self.target.next_frame())
        self.assertEqual("abcd", twistit.extract(blob.read_all()))
        
    def test_large(self):
        self.target.write(self.header.pack(10, 8))
        self.target.write("abcd")
        self.target.write("1234")
        _, blob = twistit.extract(self.target.next_frame())
        self.assertEqual("abcd1234", twistit.extract(blob.read_all()))

    def test_two_frames(self):
        self.target.write(self.header.pack(10, 4))
        self.target.write("abcd")
        self.target.write(self.header.pack(20, 4))
        self.target.write("1234")
        
        fid, blob = twistit.extract(self.target.next_frame())
        self.assertEqual(10, fid)
        self.assertEqual("abcd", twistit.extract(blob.read_all()))
        
        fid, blob = twistit.extract(self.target.next_frame())
        self.assertEqual(20, fid)
        self.assertEqual("1234", twistit.extract(blob.read_all()))
        
    