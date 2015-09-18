from . import tools, blob
import struct
from twisted.internet import defer

_header = struct.Struct("<iI")

class MergeFrames(object):
    
    def __init__(self, consumer):
        self._consumer = consumer
        self._writing = False
    
    def write_blob(self, frameid, blob):
        if self._writing:
            raise ValueError("Another blob is writing currently.")
        self._writing = True
        
        header = _header.pack(frameid, blob.size())
        
        self._consumer.write(header)
        d = blob.write_to(self._consumer)
        
        def done(result):
            self._writing = False
            return result
        d.addBoth(done)
        
        return d

class SplitFrames(tools.AbstractConsumer):
    
    
    def __init__(self):
        tools.AbstractConsumer.__init__(self)        
        self._splitter = tools.Splitter()
    
        self._splitter.request(self._header_received, 
                              _header.size, 
                              _header.size)
        
        self._payload = None
        self._failure = None
        self._remaining_payload = 0
        self._frame_queue = defer.DeferredQueue()
        
    def next_frame(self):
        """
        Returns a deferred `frameid, blob` tuple.
        """
        return self._frame_queue.get()
    
    def write(self, data):
        self._splitter.write(data)
        
    def fail(self, fail):
        """
        If we are currently providing data to a blob, fail that operation.
        """
        self._failure = fail
        if self._payload:
            self._payload.unregisterProducer()
            self._payload.fail(self._failure)
            self._payload = None
        
    def frame_received(self, frameid, blob):
        self._frame_queue.put((frameid,blob))
    
    def _header_received(self, data):
        frameid, length = _header.unpack(data)

        if self.producer is None:
            raise ValueError("Producer has not been registered.")
        
        self._payload = blob.PassthroughBlob(length)
        self._payload.registerProducer(self.forward_producer, True)
        self.frame_received(frameid, self._payload)

        self._remaining_payload = length
        self._continue()
        
    def _payload_received(self, data):
        self._payload.write(data)
        
        self._remaining_payload -= len(data)
        self._continue()
            
    def _continue(self):
        if self._remaining_payload > 0:
            self._splitter.request(self._payload_received, 
                                  1, 
                                  self._remaining_payload)
        else:
            self._payload.done()
            self._payload.unregisterProducer()
            self._payload = None
            self._splitter.request(self._header_received, 
                                  _header.size, 
                                  _header.size)
