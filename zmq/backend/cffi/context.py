"""zmq Context class"""

from zmq.constants import EINVAL, IO_THREADS
from zmq.error import InterruptedSystemCall, ZMQError, _check_rc
from ._cffi import ffi
from ._cffi import lib as C


class Context:
    _zmq_ctx = None
    _iothreads = None
    _closed = True
    _shadow = False

    def __init__(self, io_threads=1, shadow=None):
        if shadow:
            self._zmq_ctx = ffi.cast("void *", shadow)
            self._shadow = True
        else:
            self._shadow = False
            if not io_threads >= 0:
                raise ZMQError(EINVAL)
            self._zmq_ctx = C.zmq_ctx_new()
        if self._zmq_ctx == ffi.NULL:
            raise ZMQError(C.zmq_errno())
        if not shadow:
            C.zmq_ctx_set(self._zmq_ctx, IO_THREADS, io_threads)
        self._closed = False

    @property
    def underlying(self):
        return int(ffi.cast("size_t", self._zmq_ctx))

    @property
    def closed(self):
        return self._closed

    def set(self, option, value):
        rc = C.zmq_ctx_set(self._zmq_ctx, option, value)
        _check_rc(rc)

    def get(self, option):
        rc = C.zmq_ctx_get(self._zmq_ctx, option)
        _check_rc(rc, error_without_errno=False)
        return rc

    def term(self):
        if self.closed:
            return
        rc = C.zmq_ctx_destroy(self._zmq_ctx)
        try:
            _check_rc(rc)
        except InterruptedSystemCall:
            pass
        self._zmq_ctx = None
        self._closed = True


__all__ = ["Context"]
