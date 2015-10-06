'''
Created on 17.09.2015

@author: stefan
'''

import unittest
from sourblossom import rpc, blob
import pickle
import twistit
import utwist
import logging
logging.basicConfig(level=logging.DEBUG)

class TestRPC(unittest.TestCase):
        
    @twistit.yieldefer
    def twisted_setup(self):
        self.rpcA = yield rpc.RPCSystem.listen(("localhost", 4000))
        self.rpcB = yield rpc.RPCSystem.listen(("localhost", 4001))
        
    @twistit.yieldefer
    def twisted_teardown(self):
        yield self.rpcA.shutdown()
        yield self.rpcB.shutdown()
        
    @utwist.with_reactor
    def test_start_stop(self):
        pass
    
    @utwist.with_reactor
    @twistit.yieldefer
    def test_local_call(self):
        stub = self.rpcA.register(self.dummy)
        actual = yield stub()
        self.assertEqual(42, actual)
        
    @utwist.with_reactor
    @twistit.yieldefer
    def test_call(self):
        with rpc.context(self.rpcA):
            stub = rpc.register(self.dummy)
            stub_dump = pickle.dumps(stub, pickle.HIGHEST_PROTOCOL)
            
        with rpc.context(self.rpcB):
            stub = pickle.loads(stub_dump)
            actual = yield stub()
        self.assertEqual(42, actual)
        
    @utwist.with_reactor
    @twistit.yieldefer
    def test_args(self):
        with rpc.context(self.rpcA):
            stub = rpc.register(self.dummy_arg)
            stub_dump = pickle.dumps(stub, pickle.HIGHEST_PROTOCOL)
            
        with rpc.context(self.rpcB):
            stub = pickle.loads(stub_dump)
            actual = yield stub("Hello")
        self.assertEqual("Hello", actual)
        
    @utwist.with_reactor
    @twistit.yieldefer
    def test_kwargs(self):
        with rpc.context(self.rpcA):
            stub = rpc.register(self.dummy_arg)
            stub_dump = pickle.dumps(stub, pickle.HIGHEST_PROTOCOL)
            
        with rpc.context(self.rpcB):
            stub = pickle.loads(stub_dump)
            actual = yield stub(arg="Hello")
        self.assertEqual("Hello", actual)
           
    @utwist.with_reactor
    @twistit.yieldefer
    def test_blob(self):
        with rpc.context(self.rpcA):
            stub = rpc.register(self.dummy_readblob)
            stub_dump = pickle.dumps(stub, pickle.HIGHEST_PROTOCOL)
            
        with rpc.context(self.rpcB):
            stub = pickle.loads(stub_dump)
            actual = yield stub(blob=blob.StringBlob("Hello World!"))
        self.assertEqual("Hello World!", actual)
        
    @utwist.with_reactor
    @twistit.yieldefer
    def test_return_blob(self):
        with rpc.context(self.rpcA):
            stub = rpc.register(self.dummy_writeblob)
            stub_dump = pickle.dumps(stub, pickle.HIGHEST_PROTOCOL)
            
        with rpc.context(self.rpcB):
            stub = pickle.loads(stub_dump)
            returned_blob = yield stub("Hello World!")
            
        actual = yield returned_blob.read_all()
        self.assertEqual("Hello World!", actual)
        
        
    @utwist.with_reactor
    @twistit.yieldefer
    def test_echo_blob(self):
        datalist = ["Hello World!"]*(100*1024)
        
        with rpc.context(self.rpcA):
            stub = rpc.register(self.dummy_echoblob)
            stub_dump = pickle.dumps(stub, pickle.HIGHEST_PROTOCOL)
            
        with rpc.context(self.rpcB):
            stub = pickle.loads(stub_dump)
            returned_blob = yield stub(blob=blob.StringListBlob(datalist))
            
        actual = yield returned_blob.read_all()
        self.assertEqual("Hello World!"*(100*1024), actual)
        
        
    @utwist.with_reactor
    @twistit.yieldefer
    def test_slow_sender(self):
        datablocks = ["."*(8*1024)]*1024
        
        src = blob.StringListBlob(datablocks)
        src = blob.ThrottleBlob(src, 2*1024*1024)
        
        with rpc.context(self.rpcA):
            stub = rpc.register(self.dummy_readblob)
            stub_dump = pickle.dumps(stub, pickle.HIGHEST_PROTOCOL)
        with rpc.context(self.rpcB):
            stub = pickle.loads(stub_dump)
            
        actual = yield stub(blob=src)
        self.assertEqual('.'*(8*1024*1024), actual)
        self.assertTrue(src.max_buffer_size() <= 8*1024)
    
    @utwist.with_reactor
    @twistit.yieldefer  
    def test_slow_receiver(self):
        datablocks = ["."*(8*1024)]*1024
        
        src = blob.StringListBlob(datablocks)
        src = blob.MeasureBlob(src)
        
        
        with rpc.context(self.rpcA):
            stub = rpc.register(self.dummy_echoblob)
            stub_dump = pickle.dumps(stub, pickle.HIGHEST_PROTOCOL)
        with rpc.context(self.rpcB):
            stub = pickle.loads(stub_dump)
            
        returned_blob = yield stub(blob=src)
        slow_blob = blob.ThrottleBlob(returned_blob, 2*1024*1024)
        actual = yield slow_blob.read_all()
        
        self.assertTrue(src.throughput < 4*1024*1024,
                         "Sender's throughput not throttled: %s Mbs" % 
                         (src.throughput/1024.0/1024.0))
        
        self.assertEqual('.'*(8*1024*1024), actual)
        
    @utwist.with_reactor
    @twistit.yieldefer  
    def test_exception(self):
        with rpc.context(self.rpcA):
            stub = pickle.dumps(rpc.register(self.dummy_throw))
        with rpc.context(self.rpcB):
            stub = pickle.loads(stub)
        try:
            yield stub()
        except:
            pass # expected
        else:
            self.fail("Expected exception")
            
    @utwist.with_reactor
    @twistit.yieldefer  
    def test_retval_picklefail(self):
        with rpc.context(self.rpcA):
            stub = pickle.dumps(rpc.register(self.dummy_picklefail))
        with rpc.context(self.rpcB):
            stub = pickle.loads(stub)
        try:
            yield stub()
        except:
            pass # expected
        else:
            self.fail("Expected exception")
            
    @utwist.with_reactor
    @twistit.yieldefer  
    def test_param_picklefail(self):
        with rpc.context(self.rpcA):
            stub = pickle.dumps(rpc.register(self.dummy_devnull))
        with rpc.context(self.rpcB):
            stub = pickle.loads(stub)
        try:
            yield stub(type(None))
        except:
            pass # expected
        else:
            self.fail("Expected exception")
            
    @utwist.with_reactor
    @twistit.yieldefer              
    def test_reregister(self):
        with rpc.context(self.rpcA):
            stubA = pickle.dumps(rpc.register(self.dummy))
        with rpc.context(self.rpcA):
            stubA = pickle.loads(stubA)
            stubB = rpc.register(stubA)
        self.assertIs(stubA, stubB)
                      
    def dummy(self):
        return 42
    
    def dummy_devnull(self, arg):
        pass
    
    def dummy_arg(self, arg):
        return arg
    
    def dummy_readblob(self, blob):
        return blob.read_all()
    
    def dummy_writeblob(self, data):
        return blob.StringBlob(data)
    
    def dummy_echoblob(self, blob):
        return blob
            
    def dummy_throw(self):
        raise ValueError("dummy")
    
    def dummy_picklefail(self):
        return type(None)
        
class TestSerialize(unittest.TestCase):
    
    def test_noblob(self):
        msg = Msg("hello", None)
        actual = rpc.serialize(msg)
        actual = actual.read_all()
        actual = twistit.extract(actual)
        
        pickleout = pickle.dumps(msg, pickle.HIGHEST_PROTOCOL)
        header = rpc.HEADER.pack(len(pickleout), 0)
        expected = header + pickleout
        self.assertEqual(expected, actual)
        
    def test_blob(self):
        msg = Msg("hello", blob.StringBlob("world"))
        actual = rpc.serialize(msg)
        actual = actual.read_all()
        actual = twistit.extract(actual)
        
        pickleout = pickle.dumps(msg, pickle.HIGHEST_PROTOCOL)
        header = rpc.HEADER.pack(len(pickleout), 5)
        expected = header + pickleout + "world"
        self.assertEqual(expected, actual)
        
    def test_blob_stream(self):
        b = blob.PassthroughBlob(6)
        msg = Msg("hello", b)
        actual = rpc.serialize(msg)
        actual = actual.read_all()
        
        b.write("world!")
        b.done()
        
        actual = twistit.extract(actual)
        
        pickleout = pickle.dumps(msg, pickle.HIGHEST_PROTOCOL)
        header = rpc.HEADER.pack(len(pickleout), 6)
        expected = header + pickleout + "world!"
        self.assertEqual(expected, actual)
        
    
        
class TestDeserialize(unittest.TestCase):
    
    def test_noblob(self):
        expected = Msg("hello", None)
        
        pickleout = pickle.dumps(expected, pickle.HIGHEST_PROTOCOL)
        header = rpc.HEADER.pack(len(pickleout), 0)
        data = header + pickleout
        
        src = blob.StringBlob(data)
        
        d = rpc.deserialize(src)
        actual = twistit.extract(d)
        
        self.assertEqual(expected, actual)
        
    def test_blob(self):
        expected = Msg("hello", None)
        
        pickleout = pickle.dumps(expected, pickle.HIGHEST_PROTOCOL)
        header = rpc.HEADER.pack(len(pickleout), 6)
        data = header + pickleout + "World!"
        
        src = blob.StringBlob(data)
        
        d = rpc.deserialize(src)
        actual = twistit.extract(d)
        
        payload = twistit.extract(actual.blob.read_all())
        
        self.assertEqual(expected, actual)
        self.assertEqual("World!", payload)
        
    def test_blob_stream(self):
        expected = Msg("hello", None)
        
        pickleout = pickle.dumps(expected, pickle.HIGHEST_PROTOCOL)
        header = rpc.HEADER.pack(len(pickleout), 6)
        data = header + pickleout
        
        src = blob.PassthroughBlob(len(data)+6)
        d = rpc.deserialize(src)
        
        src.write(data)
        
        actual = twistit.extract(d)
        
        self.assertEqual(expected, actual)
        
        src.write("World!")
        src.done()
        
        payload = twistit.extract(actual.blob.read_all())
        
        self.assertEqual("World!", payload)
        
class Msg(object):
    def __init__(self, data, blob):
        self.data = data
        self.blob = blob
        
    def __repr__(self):
        return "Msg(%r)" % self.data

    def __eq__(self, other):
        return self.data == other.data
    
    def __ne__(self, other):
        return not (self == other)
    
    