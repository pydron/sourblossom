

from twisted.internet import protocol, endpoints, reactor, defer

from . import framing, pool, blob
import twistit
from twisted.python import failure
import pickle
import logging
from sourblossom import tools

logger = logging.getLogger(__name__)
_HELLO_FRAMEID = 10

class MsgRouter(object):
    
    def __init__(self, my_addr):
        self.my_addr = my_addr
        self._factory = MsgFactory(self)
        self._pool = pool.KeyedPool(self._connect, self._disconnect)
    
    def listen(self):
        _, port = self.my_addr
        ep = endpoints.TCP4ServerEndpoint(reactor, port, backlog=200)
        d = ep.listen(self._factory)
        def port_open(listening_port):
            self._listening_port = listening_port
            port = listening_port.getHost().port
            self.my_addr = (self.my_addr[0], port)
            return self.my_addr
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
    
    def _frame_received(self, frameid, blob, peer_address):
        self.frame_received(frameid, blob, peer_address)
        
    def _incomming_connection(self, peer_addr, conn):
        self._pool.add(peer_addr, conn)
    
    def frame_received(self, frameid, blob, peer_address):
        pass
    
    def _connect(self, addr):
        host, port = addr
        ep = endpoints.TCP4ClientEndpoint(reactor, host, port)
        d = ep.connect(self._factory)
        
        def connected(conn):
            b = blob.StringBlob(pickle.dumps(self.my_addr, pickle.HIGHEST_PROTOCOL))
            d = conn.send_message(_HELLO_FRAMEID, b)
            d.addCallback(lambda _:conn)
            return d
        
        d.addCallback(connected)
        
        return d
    
    def _disconnect(self, connection):
        connection.transport.loseConnection()
        return connection.connection_lost_d

class MsgConnection(protocol.Protocol):
    
    def __init__(self):
        self.connection_lost_d = defer.Deferred()
        self.peer_address = None
        
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
        #tools.friendly_delayed_call(self.splitter.write, data)
        
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
            if frameid == _HELLO_FRAMEID:
                d = blob.read_all()
                
                def read(content):
                    peer_address = pickle.loads(content)
                    conn.peer_address = peer_address
                    self.msgrouter._incomming_connection(peer_address, conn)
                    
                d.addCallback(read)
            else: 
                self.msgrouter._frame_received(frameid, blob, conn.peer_address)
                
        conn.frame_received = frame_received
        return conn

    
    