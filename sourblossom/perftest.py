'''
Created on 24.08.2015

@author: stefan
'''

import time
from twisted.internet import protocol, reactor, defer
from twisted.internet.endpoints import TCP4ClientEndpoint



class SourceProtocol(protocol.Protocol):
    
    def __init__(self):
        self.data = "."*(1024*1024)
        self.megabytes = 1024
        self.run = False
        
    def connectionMade(self):
        self.transport.registerProducer(self, True)
        self.resumeProducing()
        
    def connectionLost(self, reason=None):
        print "source dis"

    def pauseProducing(self):
        self.run = False
        
    def resumeProducing(self):
        self.run = True
        while self.run and self.megabytes:
            self.transport.write(self.data)
            self.megabytes -= 1
        if not self.megabytes:
            self.transport.unregisterProducer()
            self.transport.loseConnection()
            
class SourceFactory(protocol.ServerFactory):
    protocol = SourceProtocol
        
class DestProtocol(protocol.Protocol):
    
    def __init__(self):
        self.size = 0
        self.stream = nxt.ConsumerStream()
        self.splitter = nxt.Splitter()
        self.splitter.request(self.request_received, 10, 10)
        self.countdown = 10
        
    def connectionMade(self):
        self.start = time.time()
        #self.stream.registerProducer(self.transport, True)
        #reactor.callLater(0, reader, self.stream)
        
    def connectionLost(self, reason=None):
        #self.stream.unregisterProducer()
        print "disconnect"
        duration = time.time() - self.start
        print "Throughput: %s Mbs" % (self.size / duration / (1024*1024))
        reactor.stop()
    
    def dataReceived(self, data):

        self.size += len(data)
        self.splitter.write(data)
        #self.stream.write(data)
        
    def request_received(self, data):
        
        if self.countdown > 1:
            print len(data)
        
        self.countdown = max(1, self.countdown - 1)
        if self.countdown>1:
            m = self.countdown
        else:
            m = 1024*1024
        self.splitter.request(self.request_received, self.countdown, m)
        
class DestFactory(protocol.ClientFactory):
    protocol = DestProtocol




@defer.inlineCallbacks
def reader(stream):
    
    while True:
        data = yield stream.read(1024*1024)
        
if __name__ == '__main__':

    reactor.listenTCP(8000,SourceFactory())
    reactor.connectTCP("localhost", 8000, DestFactory())
    reactor.run()
