"""miscellaneous zmq_utils wrapping"""

from zmq.error import InterruptedSystemCall, _check_rc, _check_version
from ._cffi import ffi
from ._cffi import lib as C


def has(capability):
    _check_version((4, 1), "zmq.has")
    if isinstance(capability, str):
        capability = capability.encode("utf8")
    return bool(C.zmq_has(capability))


def curve_keypair():
    public = ffi.new("char[64]")
    private = ffi.new("char[64]")
    rc = C.zmq_curve_keypair(public, private)
    _check_rc(rc)
    return (ffi.buffer(public)[:40], ffi.buffer(private)[:40])


def curve_public(private):
    if isinstance(private, str):
        private = private.encode("utf8")
    _check_version((4, 2), "curve_public")
    public = ffi.new("char[64]")
    rc = C.zmq_curve_public(public, private)
    _check_rc(rc)
    return ffi.buffer(public)[:40]


def _retry_sys_call(f, *args, **kwargs):
    while True:
        rc = f(*args)
        try:
            _check_rc(rc)
        except InterruptedSystemCall:
            continue
        else:
            break
    return rc


PYZMQ_DRAFT_API: bool = bool(C.PYZMQ_DRAFT_API)
__all__ = ["has", "curve_keypair", "curve_public", "PYZMQ_DRAFT_API"]
