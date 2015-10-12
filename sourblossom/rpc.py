'''
Created on 16.09.2015

@author: stefan
'''
import pickle
from sourblossom import blob, tools, router
from twisted.internet import defer
import struct

import logging
import traceback
from twisted.python import failure
logger = logging.getLogger(__name__)

HEADER = struct.Struct("<iq")

_rpc_system = None
myaddr = None

def my_address():
    return myaddr

def register(function):
    return _rpc_system.register(function)

def listen(my_addr):
    d = RPCSystem.listen(my_addr)
    def listening(rpcsystem):
        global _rpc_system, myaddr
        _rpc_system = rpcsystem
        myaddr = rpcsystem.router.my_addr
        return myaddr
    d.addCallback(listening)
    return d


def shutdown():
    return _rpc_system.shutdown()

class context(object):
    def __init__(self, rpcsystem):
        self.prev_system = None
        self.rpcsystem = rpcsystem
        
    def __enter__(self):
        global _rpc_system
        self.prev_system = _rpc_system
        _rpc_system = self.rpcsystem
    
    def __exit__(self, exc_type, exc_value, traceback):
        global _rpc_system
        _rpc_system = self.prev_system
        return False
    
class RPCSystem(object):
    
    def __init__(self, router):
        self.router = router
        self.router.frame_received = self._frame_received

        # Maps functionid -> function
        self._functions = {}
        
        #: Maps callid -> deferred
        self._pending_calls = {}
        
        self._next_callid = 0;
        self._next_functionid = 0;
        
    @staticmethod
    def listen(my_addr):
        r = router.MsgRouter(my_addr)
        system = RPCSystem(r)
        d = r.listen()
        def listening(_):
            return system
        d.addCallback(listening)
        return d
        
    @staticmethod
    def instance():
        return _rpc_system
    
    def shutdown(self):
        return self.router.shutdown()
        
    def register(self, function):
        if isinstance(function, Stub):
            return function
        functionid = self._next_functionid
        self._next_functionid += 1
        self._functions[functionid] = function 
        return Stub(self, self.router.my_addr, functionid, getattr(function, "__name__", "stub"))
                
                
    def call(self, addr, functionid, args, kwargs):
        
        if addr == self.router.my_addr:
            # local call
            function = self._functions[functionid]
            return function(*args, **kwargs)
        
        callid = self._next_callid
        self._next_callid += 1
        
        msg = CallMsg()
        msg.caller_addr = self.router.my_addr
        msg.call_id = callid
        msg.functionid = functionid
        msg.args = args
        msg.kwargs = kwargs
        msg.blob = kwargs.pop("blob", None)
        
        # We have an interesting corner case here.
        #
        # If we send a blob, the receiver will invoke the remote function
        # before we have finished sending all the data. This function can
        # return before consuming all the data from the blob. We then receive
        # the return value before we actually finished sending the request!
        #
        # The question is what we should do in this case: should we return
        # the value to the caller or should we wait until the send has 
        # completed, even though we already know the result?
        #
        # Originally, we waited. But this turned out to be a bad idea.
        # If the remote function returns a blob, and the data from that
        # blob somehow comes from the blob we passed to the function, then
        # we have a dead-lock. The caller won't consume the returned blob since 
        # we haven't given it to the caller yet. Therefore, remote side won't
        # consume from the blob we passed to it. Thus the send operation
        # never completes. See `test_rpc.test_echo_blob()`.
        #
        # If we don't wait, then the send operation is in limbo. If it fails
        # we don't have the caller's deferred to report back to. The caller
        # also has no way of knowing when the transfer has truly completed,
        # making it hard to clean up the reactor properly.
        # We accept those drawbacks for now and log errors.
        
        def cancelled(_):
            logger.error("Cancellation of RPC calls not yet supported")
            
        result_d = defer.Deferred(cancelled)
        self._pending_calls[callid] = result_d
        
        send_d = self._send(addr, msg)
        
        def error(fail):
            if callid in self._pending_calls:
                self._pending_calls.errback(fail)
            else:
                logger.error("Sending data for call failed after receiving the reply: %s" % fail.getTraceback())
        send_d.addErrback(error)
        return result_d


    def _call_received(self, call_msg):
        
        def call():
            func = self._functions.get(call_msg.functionid, None)
            if func is None:
                raise ValueError("RPC call to unregistered function")
        
            args = call_msg.args
            kwargs = call_msg.kwargs
            if call_msg.blob:
                kwargs["blob"] = call_msg.blob
            
            return func(*args, **kwargs)
        d = defer.maybeDeferred(call)
        
        def done(result):
            ret_msg = CallCompletedMsg()
            ret_msg.call_id = call_msg.call_id
            
            if isinstance(result, blob.Blob):
                ret_msg.result = None
                ret_msg.blob = result
            else:
                ret_msg.result = result
                ret_msg.blob = None

            return self._send(call_msg.caller_addr, ret_msg)

            
        def replying_failed(failure):
            """
            We failed to send the reply. Lets try to send that error message
            instead. This happens, for example, if the return value is not
            compatible with pickle.
            """
            ret_msg = CallCompletedMsg()
            ret_msg.call_id = call_msg.call_id
            ret_msg.result = failure
            ret_msg.blob = None
            return self._send(call_msg.caller_addr, ret_msg)
        
        def replying_failed_badly(fail):
            """
            We even failed to send the error message of the failed reply!
            Maybe that failure instance is not compatible with pickle either.
            So lets send a more generic error message then. At least the
            call won't block for ever on the caller's side.
            """
            result = failure.Failure(ValueError("I could not give you a reply, I'm sorry, but I cannot tell you why."))
                
            ret_msg = CallCompletedMsg()
            ret_msg.call_id = call_msg.call_id
            ret_msg.result = result
            ret_msg.blob = None
            return self._send(call_msg.caller_addr, ret_msg)
        
        
        def replying_failed_completely(fail):
            logger.error("Unable to send reply to call: %s" % fail.getTraceback())
        
        
        d.addBoth(done)
        d.addErrback(replying_failed)
        d.addErrback(replying_failed_badly)
        d.addErrback(replying_failed_completely)
        
        
    def _result_received(self, ret_msg):
        callid = ret_msg.call_id
        d = self._pending_calls.get(callid, None)
        
        if d is None:
            logger.error("Received reply for unknown call.")
            return
        
        if ret_msg.result is not None:
            value = ret_msg.result
        else:
            value = ret_msg.blob
            
        d.callback(value)
        

    def _send(self, addr, obj):
        blob = serialize(obj)
        return self.router.send(addr, 0, blob)

    def _frame_received(self, frameid, blob):
        d = deserialize(blob)
        def success(obj):
            if isinstance(obj, CallMsg):
                self._call_received(obj)
            else:
                self._result_received(obj)
        def fail(failure):
            logger.error("Error while receiving message: %s" % failure.getTraceback())
        d.addCallbacks(success, fail)
        

def serialize(obj):
    
    appendix = obj.blob
    obj.blob = None
    
    appendix_length = appendix.size() if appendix is not None else 0
    
    try:
        meta = pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)
    except pickle.PickleError:
        raise
    except:
        raise pickle.PickleError("Error while pickling: %s" % traceback.format_exc())
    header = HEADER.pack(len(meta), appendix_length)
    
    if appendix is None:
        return blob.StringBlob(header + meta)
    else:
        total_length = len(header) + len(meta) + appendix_length
        concat = blob.PassthroughBlob(total_length)
        concat.write(header)
        concat.write(meta)
        d = appendix.write_to(concat)
        def finished(_):
            concat.done()
        def failed(fail):
            concat.fail(fail)
        d.addCallbacks(finished, failed)
        return concat

def deserialize(data):
    consumer = DeserializeConsumer()
    result = consumer.obj_d
    
    d = data.write_to(consumer)
    
    def success(_):
        consumer.done()
    def failure(f):
        consumer.fail(f)
    d.addCallbacks(success, failure)
    
    return result
    
    
class DeserializeConsumer(tools.AbstractConsumer):
    
    def __init__(self):
        tools.AbstractConsumer.__init__(self)
        self.obj_d = defer.Deferred()
        self.appendix = None
        
        self.splitter = tools.Splitter()
        self.splitter.request(self.header, HEADER.size, HEADER.size)
        
    def done(self):
        if self.appendix:
            self.appendix.done()
        if self.obj_d:
            try:
                raise ValueError("Unexpected end of data stream.")
            except:
                self.obj_d.errback()
            self.obj_d = None
    
    def fail(self, failure):
        if self.appendix:
            self.appendix.fail(failure)
        if self.obj_d:
            self.obj_d.errback(failure)
            self.obj_d = None
    
    def write(self, data):
        self.splitter.write(data)

    def header(self, data):
        meta_length, appendix_length = HEADER.unpack(data)
        self.appendix_length = appendix_length
        
        self.splitter.request(self.meta, meta_length, meta_length)
        
    def meta(self, data):
        obj = pickle.loads(data)
        
        if self.appendix_length <= 0:
            
            self.obj_d.callback(obj)
            self.obj_d = None
            
            self.splitter.request(self.unexpected, 1, 1024*1024)
        else:
            self.appendix = blob.PassthroughBlob(self.appendix_length)
            self.appendix.registerProducer(self.forward_producer, True)
            
            obj.blob = self.appendix
            self.obj_d.callback(obj)
            self.obj_d = None
            
            self.splitter.request(self.payload, 1, 1024*1024)
            
    def payload(self, data):
        self.appendix.write(data)
            
    def unexpected(self, data):
        pass

class CallMsg(object):
    def __init__(self):
        self.caller_addr = None
        self.call_id = None
        self.functionid = None
        self.args = None
        self.kwargs = None
        self.blob = None

class CallCompletedMsg(object):
    def __init__(self):
        self.call_id = None
        self.result = None
        self.blob = None
        

class Stub(object):
    
    def __init__(self, system, addr, functionid, nicename):
        self.system = system
        self.addr = addr
        self.functionid = functionid
        self.nicename = nicename
        self.__name__ = self.nicename

    
    def __call__(self, *args, **kwargs):
        return self.system.call(self.addr, 
                                 self.functionid, 
                                 args, 
                                 kwargs)
    
    def __getstate__(self):
        return (self.addr, self.functionid, self.nicename)
    
    def __setstate__(self, state):
        self.addr, self.functionid, self.nicename = state
        self.__name__ = self.nicename
        self.system = RPCSystem.instance()

    def __repr__(self):
        return "%s[%s]" % (self.nicename, self.addr)
