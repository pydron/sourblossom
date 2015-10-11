import collections
from twisted.internet import reactor, defer
import logging

logger = logging.getLogger(__name__)

class Splitter():
    """
    Consumers have a common problem: The data is received 
    in arbitrarily sized junks that make it hard to parse
    the data. This is a helper that can combine and split
    the incoming data into junks of a requested size.
    
    To use it, call :meth:`request` to tell us the minimum
    and maximum size of the junks that you'd like to receive.
    It also takes the method that will be invoked with the
    data. 
    
    The request can be changed at any time, in particular
    within the handler passed to the previous call 
    to :meth:`request`. It will take effect immediately.
    
    For example, to receive a 32-bit length prefixed message,
    first `request(handle_prefix, 4, 4)` and then, within
    `handle_prefix(encode_msg_length)` you
    `request(handle_msg, msg_length, msg_length)`. For
    better performance when receiving large messages you
    could `request(handle_msg_part, 1, remaining_msg_length)`
    and update the request every time more data has been received
    to avoid reading too much.
    
    The :meth:`write` method takes arbitrarily sized junks
    and processes them according to the current request.
    
    For optimal performance, the minimum requested size
    should be one, and the maximum requested size
    should be larger than the package size of the underlying
    medium (>=64kB is ok for TCP/IP).
    """
    
    def __init__(self):
        self._buffer = b""
        self._request_min = 1
        self._request_max = 1024*1024*1024
        self._request_handler = self._default_handler
    
    def write(self, data):
        
        while data:
        
            # Buffer the data if it isn't enough yet.
            # We assume that `_request_min` is a small number and that
            # string concatenation is therefore not a performance problem.
            if len(self._buffer) + len(data) < self._request_min:
                self._buffer += data
                return
            
            # We have sufficient data.
            
            # Number of bytes of `data` that we are going to pass on
            use_count = min(len(data), self._request_max - len(self._buffer))
            
            # Some operations that look slow here are actually fast
            # since the interpreter is smart enough not to copy the string:
            # * Concatenation with an empty string
            # * Empty slice of a string
            # * Slice of a string that contains the complete string
            request_data = self._buffer + data[:use_count]
            self._buffer = b""
            data = data[use_count:]
            
            self._request_handler(request_data)
        
    def _default_handler(self, data):
        raise ValueError("No handler configured")
    
    def request(self, handler, min_bytes, max_bytes):
        """
        Request that future calls to :meth:`request_received` must
        receive between `min_bytes` and `max_bytes` bytes. For maximal 
        throughput set `min_bytes` to one and `max_bytes` to a value larger
        than the delivery size of the underlying protocol.
        """
        assert min_bytes > 0 and min_bytes <= max_bytes
        self._request_min = min_bytes
        self._request_max = max_bytes
        self._request_handler = handler


class AbstractConsumer(object):
    """
    Base class for consumers. 
    
    Implements the register and unregister functions.
    
    .. attribute:: producer
    
        Producer currently registered.
    """
    
    class ForwardProducer(object):
        def __init__(self, consumer):
            self.paused = False
            self.consumer = consumer
            
        def stopProducing(self):
            self.consumer._stop()
        
        def pauseProducing(self):
            self.paused = True
            self.consumer._update_pause()
        
        def resumeProducing(self):
            self.paused = False
            self.consumer._update_pause()
                
    def __init__(self):
        self.producer = None
        self.__impl_paused = False
        self.__paused = False
        self.forward_producer = self.ForwardProducer(self)
    
    def write(self, data):
        raise NotImplementedError("abstract")
    
    def registerProducer(self, producer, streaming):
        if not streaming:
            raise ValueError("Only IPushProducers are supported")
        if self.producer is not None:
            raise ValueError("A producer is already registered")
        self.producer = producer
        if self.__paused:
            self.producer.pauseProducing()
    
    def unregisterProducer(self):
        if self.producer is None:
            raise ValueError("No producer is registered")
        producer = self.producer
        self.producer = None
        if self.__paused:
            producer.resumeProducing()
        
    def _pause(self):
        assert not self.__impl_paused
        self.__impl_paused = True
        self._update_pause()
        
    def _resume(self):
        assert self.__impl_paused
        self.__impl_paused = False
        self._update_pause()
            
    def _update_pause(self):
        shouldbe_paused = self.__impl_paused or self.forward_producer.paused
        is_paused = self.__paused
        self.__paused = shouldbe_paused
        
        if self.producer is None:
            return
        
        if shouldbe_paused and not is_paused:
            self.producer.pauseProducing()
        elif not shouldbe_paused and is_paused:
            self.producer.resumeProducing()
            
    def _stop(self):
        if self.producer is not None:
            self.producer.stopProducing()
        
        
class DummyProducer(object):
    
    def stopProducing(self):
        pass
    
    def pauseProducing(self):
        pass
    
    def resumeProducing(self):
        pass
    
    
class ThrottleConsumerProducer(AbstractConsumer):
    """
    
    Consumer that forwards data to another consumer `destination` with
    a limited throughput.
    
    It will pause/resume the producer to limit data in-flow. But even
    if the producer does not react, it will still throttle the out-flow
    by buffering the data internally.
    
    The size of the buffer is kept track of. Unit-tests use this to
    check if the producer actually did react to pause command, by checking how 
    large the buffer became.
    
    .. attribute: max_bytes_in_queue
        
        Maximal number of bytes that were queued up at one point.
        
    """
    
    def __init__(self, destination, bandwidth, clock = None):
        """
        :param bandwidth: Bytes per second
        """
        AbstractConsumer.__init__(self)
        
        self._destination = destination
        self._bandwidth = float(bandwidth)
        
        self._clock = clock
        if self._clock is None:
            self._clock = reactor
            
        self._queue = collections.deque()
        self._bytes_in_queue = 0
        self.max_bytes_in_queue = 0
        self._throttling = False
        self._done = False
        self._done_ds = []
        
        self._destination.registerProducer(self, True)
    
    def write(self, data):
        self._queue.append(data)
        self._bytes_in_queue += len(data)
        self.max_bytes_in_queue = max(self.max_bytes_in_queue, 
                                      self._bytes_in_queue)
        if not self._throttling:
            self._forward_next()
            
    def done(self):
        self._done = True
        d = defer.Deferred()
        self._done_ds.append(d)
        
        if not self._throttling:
            self._forward_next()
            
        return d
            
    def _forward_next(self):
        if self._queue:
            data = self._queue.popleft()
            self._bytes_in_queue -= len(data)
            duration = len(data) / self._bandwidth;
            
            if not self._throttling:
                self._throttling = True
                self._pause()
                
            self._destination.write(data)
                
            self._clock.callLater(duration, self._forward_next)
        else:
            
            if self._done:
                self._destination.unregisterProducer()
                for d in self._done_ds:
                    d.callback(None)
            
            if self._throttling:
                self._throttling = False
                self._resume()
            
    def stopProducing(self):
        self._stop()
    
    def pauseProducing(self):
        self._pause()
    
    def resumeProducing(self):
        self._resume()
  