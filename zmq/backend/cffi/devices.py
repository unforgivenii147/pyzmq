"""zmq device functions"""

from ._cffi import ffi
from ._cffi import lib as C
from .socket import Socket
from .utils import _retry_sys_call


def proxy(frontend, backend, capture=None):
    if isinstance(capture, Socket):
        capture = capture._zmq_socket
    else:
        capture = ffi.NULL
    _retry_sys_call(C.zmq_proxy, frontend._zmq_socket, backend._zmq_socket, capture)


def proxy_steerable(frontend, backend, capture=None, control=None):
    if isinstance(capture, Socket):
        capture = capture._zmq_socket
    else:
        capture = ffi.NULL
    if isinstance(control, Socket):
        control = control._zmq_socket
    else:
        control = ffi.NULL
    _retry_sys_call(
        C.zmq_proxy_steerable,
        frontend._zmq_socket,
        backend._zmq_socket,
        capture,
        control,
    )


__all__ = ["proxy", "proxy_steerable"]
