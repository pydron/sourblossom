

from twisted.internet import protocol, endpoints, reactor, defer

from . import framing, pool
import twistit
from twisted.python import failure


class MsgRouter(object):
    
    def __init__(self, my_addr):
        self.my_addr = my_addr
        self._factory = MsgFactory(self)
        self._pool = pool.KeyedPool(self._connect, self._disconnect)
    
    def listen(self, port):
        ep = endpoints.TCP4ServerEndpoint(reactor, port)
        d = ep.listen(self._factory)
        def port_open(listening_port):
            self._listening_port = listening_port
        d.addCallback(port_open)
        return d
    
    @twistit.yieldefer
    def shutdown(self):
        yield self._listening_port.stopListening()
        self._pool.dispose_all()
        
    @twistit.yieldefer
    def send(self, addr, frameid, blob):
        conn = yield self._pool.acquire(addr)
        try:
            yield conn.send_message(frameid, blob)
        finally:
            self._pool.release(conn)
    
    def _frame_received(self, frameid, blob):
        self.frame_received(frameid, blob)
    
    def frame_received(self, frameid, blob):
        pass
    
    def _connect(self, addr):
        host, port = addr
        ep = endpoints.TCP4ClientEndpoint(reactor, host, port)
        return ep.connect(self._factory)
    
    def _disconnect(self, connection):
        connection.transport.loseConnection()
        return connection.connection_lost_d

class MsgConnection(protocol.Protocol):
    
    def __init__(self):
        self.connection_lost_d = defer.Deferred()
        
    def connectionMade(self):
        self.merger = framing.MergeFrames(self.transport)
        self.splitter = framing.SplitFrames()
        self.splitter.frame_received = self._frame_received
        self.splitter.registerProducer(self.transport, True)
        
    def connectionLost(self, reason=protocol.connectionDone):
        try:
            raise reason
        except:
            self.splitter.fail(failure.Failure())
        self.connection_lost_d.callback(None)
        
    def dataReceived(self, data):
        self.splitter.write(data)
        
    def send_message(self, frameid, blob):
        return self.merger.write_blob(frameid, blob)
    
    def _frame_received(self, frameid, blob):
        return self.frame_received(frameid, blob)

    def frame_received(self, frameid, blob):
        raise NotImplementedError("abstract")
    
class MsgFactory(protocol.Factory):
    
    def __init__(self, msgrouter):
        self.msgrouter = msgrouter
    
    def buildProtocol(self, addr):
        conn = MsgConnection()
        def frame_received(frameid, blob):
            self.msgrouter._frame_received(frameid, blob)
        conn.frame_received = frame_received
        return conn

    
    