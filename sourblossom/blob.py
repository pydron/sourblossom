import collections
import StringIO as stringio
import logging
from twisted.internet import defer, reactor

from . import tools

logger = logging.getLogger(__name__)

class Blob(object):
    """
    A finite sequence of bytes.
    
    A blob can be very large as its content must not be stored in memory
    completely. A typical implementation will produce the content of 
    the blob on-demand.
    
    Some blobs can only be read once.
    
    Since the blob might receive its data directly from an underlying
    communication link (like a TCP/IP connection) it is possible that some
    resources are bound until the blob has been read. The documentation
    of the origin of the blob should contain some hints regarding that.
    
    The content of a blob is accessed by passing a `IConsumer` 
    to :meth:`writeInto`. That consumer will then receive the blob's content.
    """
    
    def write_to(self, consumer):
        """
        Write the data of this `Blob` into the given
        consumer.
        
        A `IPushProducer` is registered in the consumer for the duration of the
        operation. This producer can be used to pause or stop this blob in 
        writing its content to the provided consumer.

        Returns a deferred that calls back once the blob has finished
        writing data to the provided consumer. Canceling the
        deferred has the same effect as telling the producer to stop.
        
        The returned deferred will fail if the blob was unable to write
        all the data to the consumer.
        """
        
    def size(self):
        """
        Number of bytes in this blob.
        """
      
    def read_all(self):
        """
        Return the complete content of this blob in one string.
        
        This is not particularly efficient and is mostly for testing purposes.
        """
        s = stringio.StringIO()
        class Consumer(tools.AbstractConsumer):
            def write(self, data):
                s.write(data)
        d = self.write_to(Consumer())
        def done(_):
            return s.getvalue()
        d.addCallback(done)
        return d
    
    def __getstate__(self):
        raise ValueError("Not serializable")
      
class PassthroughBlob(Blob, tools.AbstractConsumer):
    """
    A blob who's content comes from an external source that writes it
    into it.
    
    Instances of this blob can only be read once.
    
    Besides the methods from the :class.`Blob` interface, this class has
    two additional methods to inject data into the blob:
    
    .. method:: write(data)
    
        Put data into the blob. If the blob is read, the consumer will
        receive the data in the same order as written.
        
        This method can be invoked even if no :meth:`write_to` operation
        is in progress. The data will be buffered as required (the blob
        will also attempt to pause production to avoid the buffer getting
        too large).
        
    .. method:: done()
    
        All data has been written. :meth:`write` won't be called again.
        
    .. method:: fail(failure)
    
        Something went wrong in the producer and no more data can be delivered
        to :meth:`write`. The failure is delivered to the caller
        of :meth:`write_to`.
        
        
    This implementation needs some arguments, mostly to control
    the in-flow of data:
    
    :param int size: Number of bytes in the blob.
    
    :param producer: Implementation of `IPushProducer` to inform
        if this blob (or its consumer) would like to suspend calls
        to :meth:`write` or if no more data is required.
    """
    
    def __init__(self, size, name=None):
        tools.AbstractConsumer.__init__(self)
        assert size >= 0
        self._size = size     
        self._bytes_written = 0   
        self.name = name

        #: Consumer of the active `write_into`. Cleared once the deferred
        #: calls back.
        self._consumer = None
        
        #: Flag to check if `write_into` has been called multiple times
        self._consumed = False
        
        #: If `done` has been invoked
        self._done = False
        
        #: Failure in case of :meth:`fail`.
        self._failure = None
        
        #: Buffer to store incoming data. If is set to `None` then incoming
        #: data can be forwarded directly.
        self._buffer = collections.deque()
        
        #: Number of bytes in _buffer
        self._data_in_buffer = 0;
        
        #: The deferred returned from `write_into`.
        self._write_into_deferred = None
        
        #: If we have called `pause` while waiting for the `write_into` call.
        self._paused = False

    def write_to(self, consumer):
        if self._consumed:
            raise ValueError("Can only be consumed once")
        self._consumed = True
        
        self._consumer = consumer
        self._write_into_deferred = defer.Deferred(
                      lambda _:self._stop())
                
        self._consumer.registerProducer(self.forward_producer, True)
        
        while self._buffer:
            data = self._buffer.popleft()
            self._data_in_buffer -= len(data)
            self._consumer.write(data)
        self._buffer = None
        
        if self._paused:
            self._paused = False
            self._resume()

        if self._done:
            self._consumer.unregisterProducer()
            
            if self._failure is None:
                self._write_into_deferred.callback(None)
            else:
                self._write_into_deferred.errback(self._failure)
            
        return self._write_into_deferred
            
    def size(self):
        return self._size
      
    def write(self, data):
        if self._done:
            raise ValueError("write() after done()")
        self._bytes_written += len(data)
        if self._bytes_written > self._size:
            raise ValueError("Write beyond size of blob")
        if self._buffer is not None:
            if not self._paused:
                self._paused = True
                self._pause()
            self._buffer.append(data)
            self._data_in_buffer += len(data)
            if self._data_in_buffer > 1024*1024:
                logger.warn("More than 1MB in PassthroughBlob buffer")
        else:
            self._consumer.write(data)
            
    def done(self):
        if self._done:
            return
        if self._bytes_written != self.size():
            raise ValueError("Not enough data written")
        self._done = True
        if self._buffer is None:
            self._consumer.unregisterProducer()
            self._write_into_deferred.callback(None)
                
    def fail(self, failure):
        if self._done:
            return
        self._done = True
        self._failure = failure
        if self._buffer is None:
            self._consumer.unregisterProducer()
            self._write_into_deferred.errback(failure)
            
    def __repr__(self):
        return "PassthroughBlob(%r, %r)" % (self._size, self.name)
                
class StringBlob(Blob):
    """
    Blob that gets its data from a single string.
    This is handy for testing.
    """
    
    def __init__(self, data):
        self._data = data
        
    def write_to(self, consumer):
        consumer.write(self._data)
        return defer.succeed(None)
        
    def size(self):
        return len(self._data)
                
                
class StringListBlob(Blob):
    """
    Blob that gets its data from a list of strings.
    """
    
    def __init__(self, datalist):
        self._datalist = datalist
        self._size = sum(len(d) for d in datalist)
        
    def write_to(self, consumer):
        p = self.Producer(self._datalist, consumer)
        p.run()
        return p.done
        
    def size(self):
        return self._size
                
    class Producer(object):
        
        def __init__(self, datalist, consumer):
            self.datalist = collections.deque(datalist)
            self.consumer = consumer
            self.paused = False
            self.stopped = False
            self.done = defer.Deferred(lambda _:self.stopProducing())
            self.consumer.registerProducer(self, True)
        
        def run(self):
            while self.datalist and not self.paused and not self.stopped:
                data = self.datalist.popleft()
                self.consumer.write(data)
            if self.stopped or not self.datalist:
                consumer = self.consumer
                self.consumer = None
                if consumer is not None:
                    consumer.unregisterProducer()
                    self.done.callback(None)
                
        def stopProducing(self):
            self.stopped = True
        
        def pauseProducing(self):
            if self.paused:
                raise ValueError("already paused")
            self.paused = True
        
        def resumeProducing(self):
            if not self.paused:
                raise ValueError("not paused")
            self.paused = False
            self.run()

  
class ThrottleBlob(Blob):
    """
    Wrapper for a blob that limits the speed of data written to a consumer.
    """
    
    def __init__(self, source_blob, throughput, clock=None):
        """
        :param source_blob: This blob will have the same data as this blob.
        :param throughput: Maximal throughput in bytes per second.
        :param clock: Reactor for timing
        """
        self.source_blob = source_blob
        self.throughput = throughput
        self.clock = clock
        self._max_buffer = 0
        
    def size(self):
        return self.source_blob.size()
    
    def max_buffer_size(self):
        """
        Maximal number of bytes that were buffered at one time due
        to a mismatch of producer and consumer speed. If this number is
        high, this is a sign that the source blob does not pause correctly.
        """
        return self._max_buffer
    
    def write_to(self, consumer):
        t = tools.ThrottleConsumerProducer(consumer, 
                                           self.throughput, 
                                           self.clock)
        d = self.source_blob.write_to(t)
        def done(_):
            self._max_buffer = max(t.max_bytes_in_queue, self._max_buffer)
            return t.done()
        d.addCallback(done)
        return d
    
class MeasureBlob(Blob):
    """
    Wrapper for a blob that measures the throughput.
    
    .. attribute: throughput
        
        Throughput in bytes per second of the previous :meth:`write_to`
        operation.
    """
    
    def __init__(self, source_blob, clock=None):
        """
        :param source_blob: This blob will have the same data as this blob.
        :param clock: Reactor for timing
        """
        self.source_blob = source_blob
        self.clock = clock
        if self.clock == None:
            self.clock = reactor
        self.throughput = None
        
    def size(self):
        return self.source_blob.size()
    
    def write_to(self, consumer):
        starttime = self.clock.seconds()
        
        d = self.source_blob.write_to(consumer)
        
        def done(_):
            endtime = self.clock.seconds()
            self.throughput = float(self.size()) / (endtime - starttime)
        d.addCallback(done)
        
        return d