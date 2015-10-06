'''
Created on 18.09.2015

@author: stefan
'''
from sourblossom import blob, tools
import picklesize
import pickle
import threading
from twisted.internet import defer, threads
from twisted.internet import reactor as default_reactor
import collections

class PickleDumpBlob(blob.Blob):
    """
    A blob that gets its data from pickling an object.
    
    For larger objects, pickling happens in a background thread and
    is paused / resumed as requested by the consumer.
    
    This blob can be consumed multiple times.
    """
    
    def __init__(self, obj, reactor=None, threadpool=None):
        self.reactor = reactor
        if self.reactor is None:
            self.reactor = default_reactor
        
        self.threadpool = threadpool
        if self.threadpool is None:
            self.threadpool = self.reactor.getThreadPool()
            
        self.obj = obj
        self._size = picklesize.picklesize(obj, pickle.HIGHEST_PROTOCOL)
        
    def size(self):
        return self._size
    
    def write_to(self, consumer):
        if self._size < 64*1024:
            data = pickle.dumps(self.obj, pickle.HIGHEST_PROTOCOL)
            consumer.write(data)
            return defer.succeed(None)
        else:
            fh = FilePushProducer(self._size, consumer, self.reactor)
            return threads.deferToThreadPool(self.reactor,
                                             self.threadpool,
                                             _dump_to_file, self.obj, fh)



def _dump_to_file(obj, fh):
    try:
        pickle.dump(obj, fh, pickle.HIGHEST_PROTOCOL)
        fh.close()
    except EOFError:
        pass # consumer is no longer interested

def dumps_to_blob(obj, reactor=None, threadpool=None):
    return PickleDumpBlob(obj, reactor, threadpool)

def loads_from_blob(blob, reactor=None):
    consumer = BlockingConsumer(reactor)
    reader = BufferedReader(consumer)
    blob.write_to(consumer)
    
    def loads():
        return pickle.load(reader)

    return threads.deferToThread(loads)

class FilePushProducer(object):
    """
    File-like object with a blocking :meth:`write` method that writes
    data to a consumer. It implements `IPushProducer` and registers
    itself as such with the consumer.
    
    It handles thread safety (the consumer will be called with the ractor's
    thread, while :meth:`write` can  be called from a different thread.
    
    The :meth:`write` blocks if we are paused.
    
    If we are asked to stop producing more data, :meth:`write` will throw
    an `EOFError`.
    """
    
    block_size = 64*1024
    
    def __init__(self, size, consumer, reactor):
        """
        :param size: Number of bytes that will be written to this file.
            If the file is :meth:`closed` before that, or if more data is
            written, an exception is thrown.
            
        :param consumer: Where to forward the data to.
        
        :param reactor: Provider of `IReactorThreads`. We use this
            thread to talk to the consumer.
        """
        self.size = size
        self.consumer = consumer
        self.reactor = reactor
        self.paused = threading.Event()
        self.paused.set()
        self.stopped = False
        self.closed = False
        self.bytes_written = 0
        self.write_buffer = []
        self.write_buffer_len = 0
        self.consumer.registerProducer(self, True)
    
    def pauseProducing(self):
        if self.closed or self.stopped:
            return
        self.paused.clear()
    
    def resumeProducing(self):
        if self.closed or self.stopped:
            return
        self.paused.set()
    
    def stopProducing(self):
        self.stopped = True
        self.paused.set()
    
    def write(self, data):
        """
        Write data to the consumer. This method will block
        if the consumer has told us to pause. It may also block
        because the reactor thread is busy.
        """
        datalen = len(data)
        
        if self.closed:
            raise ValueError("Write to closed file.")
        
        self.bytes_written += datalen
        if self.bytes_written > self.size:
            raise IOError("Write beyond end of file.")
        
        if self.write_buffer_len + datalen < self.block_size:
            self.write_buffer.append(data)
            self.write_buffer_len += datalen
            return
        
        self.paused.wait()
        
        if self.closed:
            raise ValueError("Write to closed file.")
        if self.stopped:
            raise EOFError("Consumer asks to stop write more data.")
        
        buffer_data = "".join(self.write_buffer)
        threads.blockingCallFromThread(self.reactor, 
                                       self.consumer.write, 
                                       buffer_data)


        
        if datalen < self.block_size:
            self.write_buffer = [data]
            self.write_buffer_len = datalen
        else:
            self.write_buffer = []
            self.write_buffer_len = 0 
            threads.blockingCallFromThread(self.reactor, 
                                           self.consumer.write, 
                                           data)
        
        
        
    def close(self):
        if self.closed:
            return
        if self.bytes_written != self.size:
            raise IOError("close() before all the data has been written.")
        self.closed = True
        self.paused.set()
        
        if self.write_buffer_len > 0:
            buffer_data = "".join(self.write_buffer)
            threads.blockingCallFromThread(self.reactor, 
                                           self.consumer.write, 
                                           buffer_data)
            self.write_buffer = []
            self.write_buffer_len = 0 
        
        threads.blockingCallFromThread(self.reactor, 
                                              self.consumer.unregisterProducer)
        
        self.consumer = None


        
        
class BlockingConsumer(tools.AbstractConsumer):
    
    min_queue = 128*1024
    max_queue = 10*1024*1024
    
    def __init__(self, reactor=None):
        tools.AbstractConsumer.__init__(self)
        
        self.reactor = reactor
        if self.reactor is None:
            self.reactor = default_reactor
        
        self._queue = collections.deque()
        self._queue_size = 0
        self._lock = threading.Condition()
        self._paused = False
        
    def write(self, data):
        self._lock.acquire()
        self._queue.append(data)
        self._queue_size += len(data)
        
        should_be_paused = (self._queue_size >= self.max_queue and 
                            len(self._queue) > 1)
        
        pausing = should_be_paused and not self._paused
        
        self._lock.notify()
        self._lock.release()
        
        if pausing:
            self._pause()
            self._paused = True
        
    def read(self):
        self._lock.acquire()
        while not self._queue:
            self._lock.wait()
        data = self._queue.popleft()
        self._queue_size -= len(data)
        
        should_be_paused = (self._queue_size >= self.max_queue and 
                            len(self._queue) > 1)
        
        resuming = not should_be_paused and self._paused
        
        self._lock.release()
        
        if resuming:
            def resume():
                self._resume()
                self._paused = False
            threads.blockingCallFromThread(self.reactor, resume)
            
        return data
    
class BufferedReader(object):
    
    def __init__(self, src):
        self.src = src
        self.buffer = ""
        self.position = 0
    
    def read(self, count):
        parts = []
        
        remaining = count
        
        start = self.position
        bfrlen = len(self.buffer)
        
        end = min(start + count, bfrlen)
        parts.append(self.buffer[start:end])
        remaining -= end-start
        self.position = end
    
        while remaining > 0:
            data = self.src.read()
            datalen = len(data)
            remaining -= datalen
        
            if remaining < 0:
                self.buffer = data
                self.position =  datalen + remaining
                dataleft = data[:remaining]
            else:
                dataleft = data
            parts.append(dataleft)
        return "".join(parts)
                
    def readline(self):
        p = -1
        parts = []
        while p < 0:
            p = self.buffer.find("\n", self.position)
            if p < 0:
                parts.append(self.buffer[self.position:])
                self.buffer = self.src.read()
                self.position = 0
            else:
                p += 1
                parts.append(self.buffer[self.position:p])
                self.position = p
        return "".join(parts)