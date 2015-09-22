'''
Created on 18.09.2015

@author: stefan
'''
from sourblossom import blob
import picklesize
import pickle
import threading
from twisted.internet import defer, threads
from twisted.internet import reactor as default_reactor

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
        self.size = picklesize.picklesize(obj, pickle.HIGHEST_PROTOCOL)
        
    def size(self):
        return self.size
    
    def write_to(self, consumer):
        if self.size < 64*1024:
            data = pickle.dumps(self.obj, pickle.HIGHEST_PROTOCOL)
            consumer.write(data)
            return defer.succeed(None)
        else:
            fh = FilePushProducer(self.size, consumer, self.reactor)
            return threads.deferToThreadPool(self.reactor,
                                             self.threadpool,
                                             _dump_to_file, self.obj, fh)
            
def _dump_to_file(obj, fh):
    try:
        pickle.dump(obj, fh, pickle.HIGHEST_PROTOCOL)
        fh.close()
    except EOFError:
        pass # consumer is no longer interested

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
        self.consumer. registerProducer(self, True)
    
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
        if self.closed:
            raise ValueError("Write to closed file.")
        
        self.paused.wait()
        
        if self.closed:
            raise ValueError("Write to closed file.")
        if self.stopped:
            raise EOFError("Consumer asks to stop write more data.")
        
        self.bytes_written += len(data)
        if self.bytes_written > self.size:
            raise IOError("Write beyond end of file.")
        
        return threads.blockingCallFromThread(self.reactor, 
                                              self.consumer.write, 
                                              data)
        
    def close(self):
        if self.closed:
            return
        if self.bytes_written != self.size:
            raise IOError("close() before all the data has been written.")
        self.closed = True
        self.paused.set()
        
        threads.blockingCallFromThread(self.reactor, 
                                              self.consumer.unregisterProducer)
        
        self.consumer = None
