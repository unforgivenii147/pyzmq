"""Dummy Frame object"""

import errno
from threading import Event
import zmq
import zmq.error
from zmq.constants import ETERM
from ._cffi import ffi
from ._cffi import lib as C

zmq_gc = None
try:
    from __pypy__.bufferable import bufferable as maybe_bufferable
except ImportError:
    maybe_bufferable = object


def _content(obj):
    if type(obj) is bytes:
        return obj
    if not isinstance(obj, memoryview):
        obj = memoryview(obj)
    return obj.tobytes()


def _check_rc(rc):
    err = C.zmq_errno()
    if rc == -1:
        if err == errno.EINTR:
            raise zmq.error.InterrruptedSystemCall(err)
        elif err == errno.EAGAIN:
            raise zmq.error.Again(errno)
        elif err == ETERM:
            raise zmq.error.ContextTerminated(err)
        else:
            raise zmq.error.ZMQError(err)
    return 0


class Frame(maybe_bufferable):
    _data = None
    tracker = None
    closed = False
    more = False
    _buffer = None
    _bytes = None
    _failed_init = False
    tracker_event = None
    zmq_msg = None

    def __init__(self, data=None, track=False, copy=None, copy_threshold=None):
        self._failed_init = True
        self.zmq_msg = ffi.cast("zmq_msg_t[1]", C.malloc(ffi.sizeof("zmq_msg_t")))
        if track:
            self.tracker = zmq._FINISHED_TRACKER
        if isinstance(data, str):
            raise TypeError(
                "Unicode strings are not allowed. Only: bytes, buffer interfaces."
            )
        if data is None:
            rc = C.zmq_msg_init(self.zmq_msg)
            _check_rc(rc)
            self._failed_init = False
            return
        self._data = data
        if type(data) is bytes:
            self._bytes = data
        self._buffer = memoryview(data)
        if not self._buffer.contiguous:
            raise BufferError("memoryview: underlying buffer is not contiguous")
        c_data = ffi.from_buffer(self._buffer)
        data_len_c = self._buffer.nbytes
        if copy is None:
            if copy_threshold and data_len_c < copy_threshold:
                copy = True
            else:
                copy = False
        if copy:
            rc = C.zmq_msg_init_size(self.zmq_msg, data_len_c)
            _check_rc(rc)
            ffi.buffer(C.zmq_msg_data(self.zmq_msg), data_len_c)[:] = self._buffer
            self._failed_init = False
            return
        if track:
            evt = Event()
            self.tracker_event = evt
            self.tracker = zmq.MessageTracker(evt)
        global zmq_gc
        if zmq_gc is None:
            from zmq.utils.garbage import gc as zmq_gc
        hint = ffi.cast("zhint[1]", C.malloc(ffi.sizeof("zhint")))
        hint[0].id = zmq_gc.store(data, self.tracker_event)
        if not zmq_gc._push_mutex:
            zmq_gc._push_mutex = C.mutex_allocate()
        hint[0].mutex = ffi.cast("mutex_t*", zmq_gc._push_mutex)
        hint[0].sock = ffi.cast("void*", zmq_gc._push_socket.underlying)
        rc = C.zmq_wrap_msg_init_data(self.zmq_msg, c_data, data_len_c, hint)
        if rc != 0:
            C.free(hint)
            C.free(self.zmq_msg)
            _check_rc(rc)
        self._failed_init = False

    def __del__(self):
        if not self.closed and (not self._failed_init):
            self.close()

    def close(self):
        if self.closed or self._failed_init or self.zmq_msg is None:
            return
        self.closed = True
        rc = C.zmq_msg_close(self.zmq_msg)
        C.free(self.zmq_msg)
        self.zmq_msg = None
        if rc != 0:
            _check_rc(rc)

    def _buffer_from_zmq_msg(self):
        if self._data is None:
            self._data = ffi.buffer(
                C.zmq_msg_data(self.zmq_msg), C.zmq_msg_size(self.zmq_msg)
            )
        if self._buffer is None:
            self._buffer = memoryview(self._data)

    @property
    def buffer(self):
        if self._buffer is None:
            self._buffer_from_zmq_msg()
        return self._buffer

    @property
    def bytes(self):
        if self._bytes is None:
            self._bytes = self.buffer.tobytes()
        return self._bytes

    def __len__(self):
        return self.buffer.nbytes

    def __eq__(self, other):
        return self.bytes == _content(other)

    @property
    def done(self):
        return self.tracker.done()

    def __buffer__(self, flags):
        return self.buffer

    def __copy__(self):
        return self.fast_copy()

    def fast_copy(self):
        new_msg = Frame()
        C.zmq_msg_copy(new_msg.zmq_msg, self.zmq_msg)
        new_msg._data = self._data
        new_msg._buffer = self._buffer
        new_msg.tracker_event = self.tracker_event
        new_msg.tracker = self.tracker
        return new_msg


Message = Frame
__all__ = ["Frame", "Message"]
