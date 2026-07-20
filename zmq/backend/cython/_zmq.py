"""Cython backend for pyzmq"""

from __future__ import annotations

try:
    import cython

    if not cython.compiled:
        raise ImportError()
except ImportError:
    from pathlib import Path

    zmq_root = Path(__file__).parents[3]
    msg = f"\n    Attempting to import zmq Cython backend, which has not been compiled.\n\n    This probably means you are importing zmq from its source tree.\n    if this is what you want, make sure to do an in-place build first:\n\n        pip install -e '{zmq_root}'\n\n    If it is not, then '{zmq_root}' is probably on your sys.path,\n    when it shouldn't be. Is that your current working directory?\n\n    If neither of those is true and this file is actually installed,\n    something seems to have gone wrong with the install!\n    Please report at https://github.com/zeromq/pyzmq/issues\n    "
    raise ImportError(msg)
import warnings
from threading import Event
from time import monotonic
from weakref import ref
import cython as C
from cython import (
    NULL,
    Py_ssize_t,
    address,
    bint,
    cast,
    cclass,
    cfunc,
    char,
    declare,
    inline,
    nogil,
    p_char,
    p_void,
    pointer,
    size_t,
    sizeof,
)
from cython.cimports.cpython.buffer import (
    Py_buffer,
    PyBUF_ANY_CONTIGUOUS,
    PyBUF_WRITABLE,
    PyBuffer_Release,
    PyObject_GetBuffer,
)
from cython.cimports.cpython.bytes import (
    PyBytes_AsString,
    PyBytes_FromStringAndSize,
    PyBytes_Size,
)
from cython.cimports.cpython.exc import PyErr_CheckSignals
from cython.cimports.libc.errno import EAGAIN, EINTR, ENAMETOOLONG, ENOENT, ENOTSOCK
from cython.cimports.libc.stdint import uint32_t
from cython.cimports.libc.stdio import fprintf
from cython.cimports.libc.stdio import stderr as cstderr
from cython.cimports.libc.stdlib import free, malloc
from cython.cimports.libc.string import memcpy
from cython.cimports.zmq.backend.cython import libzmq
from cython.cimports.zmq.backend.cython._externs import (
    get_ipc_path_max_len,
    getpid,
    mutex_allocate,
    mutex_lock,
    mutex_t,
    mutex_unlock,
)
from cython.cimports.zmq.backend.cython.libzmq import (
    ZMQ_ENOTSOCK,
    ZMQ_ETERM,
    ZMQ_EVENT_ALL,
    ZMQ_FD,
    ZMQ_IDENTITY,
    ZMQ_IO_THREADS,
    ZMQ_LINGER,
    ZMQ_POLLIN,
    ZMQ_POLLOUT,
    ZMQ_RCVMORE,
    ZMQ_ROUTER,
    ZMQ_SNDMORE,
    ZMQ_THREAD_SAFE,
    ZMQ_TYPE,
    _zmq_version,
    fd_t,
    int64_t,
    zmq_bind,
    zmq_close,
    zmq_connect,
    zmq_ctx_destroy,
    zmq_ctx_get,
    zmq_ctx_new,
    zmq_ctx_set,
    zmq_curve_keypair,
    zmq_curve_public,
    zmq_disconnect,
    zmq_free_fn,
    zmq_getsockopt,
    zmq_has,
    zmq_join,
    zmq_leave,
    zmq_msg_close,
    zmq_msg_copy,
    zmq_msg_data,
    zmq_msg_get,
    zmq_msg_gets,
    zmq_msg_group,
    zmq_msg_init,
    zmq_msg_init_data,
    zmq_msg_init_size,
    zmq_msg_recv,
    zmq_msg_routing_id,
    zmq_msg_send,
    zmq_msg_set,
    zmq_msg_set_group,
    zmq_msg_set_routing_id,
    zmq_msg_size,
    zmq_msg_t,
    zmq_poller_add,
    zmq_poller_destroy,
    zmq_poller_fd,
    zmq_poller_new,
    zmq_pollitem_t,
    zmq_proxy,
    zmq_proxy_steerable,
    zmq_recv,
    zmq_setsockopt,
    zmq_socket,
    zmq_socket_monitor,
    zmq_strerror,
    zmq_unbind,
)
from cython.cimports.zmq.backend.cython.libzmq import zmq_errno as _zmq_errno
from cython.cimports.zmq.backend.cython.libzmq import zmq_poll as zmq_poll_c
import zmq
from zmq.constants import SocketOption, _OptType
from zmq.error import (
    Again,
    ContextTerminated,
    InterruptedSystemCall,
    ZMQError,
    _check_version,
)

IPC_PATH_MAX_LEN: int = get_ipc_path_max_len()
PYZMQ_DRAFT_API: bool = bool(libzmq.PYZMQ_DRAFT_API)


@cfunc
@inline
@C.exceptval(-1)
def _check_rc(rc: C.int, error_without_errno: bint = False) -> C.int:
    errno: C.int = _zmq_errno()
    PyErr_CheckSignals()
    if errno == 0 and (not error_without_errno):
        return 0
    if rc == -1:
        if errno == EINTR:
            raise InterruptedSystemCall(errno)
        elif errno == EAGAIN:
            raise Again(errno)
        elif errno == ZMQ_ETERM:
            raise ContextTerminated(errno)
        else:
            raise ZMQError(errno)
    return 0


_zhint = C.struct(sock=p_void, mutex=pointer(mutex_t), id=size_t)


@cfunc
@nogil
def free_python_msg(data: p_void, vhint: p_void) -> C.int:
    msg = declare(zmq_msg_t)
    msg_ptr: pointer(zmq_msg_t) = address(msg)
    hint: pointer(_zhint) = cast(pointer(_zhint), vhint)
    rc: C.int
    if hint != NULL:
        zmq_msg_init_size(msg_ptr, sizeof(size_t))
        memcpy(zmq_msg_data(msg_ptr), address(hint.id), sizeof(size_t))
        rc = mutex_lock(hint.mutex)
        if rc != 0:
            fprintf(cstderr, "pyzmq-gc mutex lock failed rc=%d\n", rc)
        rc = zmq_msg_send(msg_ptr, hint.sock, 0)
        if rc < 0:
            if _zmq_errno() != ZMQ_ENOTSOCK:
                fprintf(
                    cstderr, "pyzmq-gc send failed: %s\n", zmq_strerror(_zmq_errno())
                )
        rc = mutex_unlock(hint.mutex)
        if rc != 0:
            fprintf(cstderr, "pyzmq-gc mutex unlock failed rc=%d\n", rc)
        zmq_msg_close(msg_ptr)
        free(hint)
        return 0


@cfunc
@inline
def _copy_zmq_msg_bytes(zmq_msg: pointer(zmq_msg_t)) -> bytes:
    data_c: p_char = NULL
    data_len_c: Py_ssize_t
    data_c = cast(p_char, zmq_msg_data(zmq_msg))
    data_len_c = zmq_msg_size(zmq_msg)
    return PyBytes_FromStringAndSize(data_c, data_len_c)


@cfunc
@inline
def _asbuffer(obj, data_c: pointer(p_void), writable: bint = False) -> size_t:
    pybuf = declare(Py_buffer)
    flags: C.int = PyBUF_ANY_CONTIGUOUS
    if writable:
        flags |= PyBUF_WRITABLE
    rc: C.int = PyObject_GetBuffer(obj, address(pybuf), flags)
    if rc < 0:
        raise ValueError("Couldn't create buffer")
    data_c[0] = pybuf.buf
    data_size: size_t = pybuf.len
    PyBuffer_Release(address(pybuf))
    return data_size


_gc = None


@cclass
class Frame:
    def __init__(
        self, data=None, track=False, copy=None, copy_threshold=None, **kwargs
    ):
        rc: C.int
        data_c: p_char = NULL
        data_len_c: Py_ssize_t = 0
        hint: pointer(_zhint)
        if copy_threshold is None:
            copy_threshold = zmq.COPY_THRESHOLD
        c_copy_threshold: C.size_t = 0
        if copy_threshold is not None:
            c_copy_threshold = copy_threshold
        zmq_msg_ptr: pointer(zmq_msg_t) = address(self.zmq_msg)
        self.more = False
        self._data = data
        self._failed_init = True
        self._buffer = None
        self._bytes = None
        self.tracker_event = None
        self.tracker = None
        if track:
            self.tracker = zmq._FINISHED_TRACKER
        if isinstance(data, str):
            raise TypeError("Str objects not allowed. Only: bytes, buffer interfaces.")
        if data is None:
            rc = zmq_msg_init(zmq_msg_ptr)
            _check_rc(rc)
            self._failed_init = False
            return
        data_len_c = _asbuffer(data, cast(pointer(p_void), address(data_c)))
        c_copy: bint = True
        if copy is None:
            if c_copy_threshold and data_len_c < c_copy_threshold:
                c_copy = True
            else:
                c_copy = False
        else:
            c_copy = copy
        if c_copy:
            rc = zmq_msg_init_size(zmq_msg_ptr, data_len_c)
            _check_rc(rc)
            memcpy(zmq_msg_data(zmq_msg_ptr), data_c, data_len_c)
            self._failed_init = False
            return
        if track:
            evt = Event()
            self.tracker_event = evt
            self.tracker = zmq.MessageTracker(evt)
        global _gc
        if _gc is None:
            from zmq.utils.garbage import gc as _gc
        hint: pointer(_zhint) = cast(pointer(_zhint), malloc(sizeof(_zhint)))
        hint.id = _gc.store(data, self.tracker_event)
        if not _gc._push_mutex:
            hint.mutex = mutex_allocate()
            _gc._push_mutex = cast(size_t, hint.mutex)
        else:
            hint.mutex = cast(pointer(mutex_t), cast(size_t, _gc._push_mutex))
        hint.sock = cast(p_void, cast(size_t, _gc._push_socket.underlying))
        rc = zmq_msg_init_data(
            zmq_msg_ptr,
            cast(p_void, data_c),
            data_len_c,
            cast(pointer(zmq_free_fn), free_python_msg),
            cast(p_void, hint),
        )
        if rc != 0:
            free(hint)
            _check_rc(rc)
        self._failed_init = False

    def __dealloc__(self):
        if self._failed_init:
            return
        with nogil:
            rc: C.int = zmq_msg_close(address(self.zmq_msg))
        _check_rc(rc)

    def __copy__(self):
        return self.fast_copy()

    def fast_copy(self) -> Frame:
        new_msg: Frame = Frame()
        zmq_msg_copy(address(new_msg.zmq_msg), address(self.zmq_msg))
        if self._data is not None:
            new_msg._data = self._data
        if self._buffer is not None:
            new_msg._buffer = self._buffer
        if self._bytes is not None:
            new_msg._bytes = self._bytes
        new_msg.tracker_event = self.tracker_event
        new_msg.tracker = self.tracker
        return new_msg

    def __getbuffer__(self, buffer: pointer(Py_buffer), flags: C.int):
        buffer.buf = zmq_msg_data(address(self.zmq_msg))
        buffer.len = zmq_msg_size(address(self.zmq_msg))
        buffer.obj = self
        buffer.readonly = 0
        buffer.format = "B"
        buffer.ndim = 1
        buffer.shape = address(buffer.len)
        buffer.strides = NULL
        buffer.suboffsets = NULL
        buffer.itemsize = 1
        buffer.internal = NULL

    def __len__(self) -> size_t:
        sz: size_t = zmq_msg_size(address(self.zmq_msg))
        return sz

    @property
    def buffer(self):
        _buffer = self._buffer and self._buffer()
        if _buffer is not None:
            return _buffer
        _buffer = memoryview(self)
        self._buffer = ref(_buffer)
        return _buffer

    @property
    def bytes(self):
        if self._bytes is None:
            self._bytes = _copy_zmq_msg_bytes(address(self.zmq_msg))
        return self._bytes

    def get(self, option):
        rc: C.int = 0
        property_c: p_char = NULL
        if isinstance(option, int):
            rc = zmq_msg_get(address(self.zmq_msg), option)
            _check_rc(rc)
            return rc
        if option == "routing_id":
            routing_id: uint32_t = zmq_msg_routing_id(address(self.zmq_msg))
            if routing_id == 0:
                _check_rc(-1)
            return routing_id
        elif option == "group":
            buf = zmq_msg_group(address(self.zmq_msg))
            if buf == NULL:
                _check_rc(-1)
            return buf.decode("utf8")
        _check_version((4, 1), "get string properties")
        if isinstance(option, str):
            option = option.encode("utf8")
        if not isinstance(option, bytes):
            raise TypeError(f"expected str, got: {option!r}")
        property_c = option
        result: p_char = cast(p_char, zmq_msg_gets(address(self.zmq_msg), property_c))
        if result == NULL:
            _check_rc(-1)
        return result.decode("utf8")

    def set(self, option, value):
        rc: C.int
        if option == "routing_id":
            routing_id: uint32_t = value
            rc = zmq_msg_set_routing_id(address(self.zmq_msg), routing_id)
            _check_rc(rc)
            return
        elif option == "group":
            if isinstance(value, str):
                value = value.encode("utf8")
            rc = zmq_msg_set_group(address(self.zmq_msg), value)
            _check_rc(rc)
            return
        rc = zmq_msg_set(address(self.zmq_msg), option, value)
        _check_rc(rc)


@cclass
class Context:
    def __init__(self, io_threads: C.int = 1, shadow: size_t = 0):
        self.handle = NULL
        self._pid = 0
        self._shadow = False
        if shadow:
            self.handle = cast(p_void, shadow)
            self._shadow = True
        else:
            self._shadow = False
            self.handle = zmq_ctx_new()
        if self.handle == NULL:
            raise ZMQError()
        rc: C.int = 0
        if not self._shadow:
            rc = zmq_ctx_set(self.handle, ZMQ_IO_THREADS, io_threads)
            _check_rc(rc)
        self.closed = False
        self._pid = getpid()

    @property
    def underlying(self):
        return cast(size_t, self.handle)

    @cfunc
    @inline
    def _term(self) -> C.int:
        rc: C.int = 0
        if self.handle != NULL and (not self.closed) and (getpid() == self._pid):
            with nogil:
                rc = zmq_ctx_destroy(self.handle)
        self.handle = NULL
        return rc

    def term(self):
        rc: C.int = self._term()
        try:
            _check_rc(rc)
        except InterruptedSystemCall:
            pass
        self.closed = True

    def set(self, option: C.int, optval):
        optval_int_c: C.int
        rc: C.int
        if self.closed:
            raise RuntimeError("Context has been destroyed")
        if not isinstance(optval, int):
            raise TypeError(f"expected int, got: {optval!r}")
        optval_int_c = optval
        rc = zmq_ctx_set(self.handle, option, optval_int_c)
        _check_rc(rc)

    def get(self, option: C.int):
        rc: C.int
        if self.closed:
            raise RuntimeError("Context has been destroyed")
        rc = zmq_ctx_get(self.handle, option)
        _check_rc(rc, error_without_errno=False)
        return rc


@cfunc
@inline
def _c_addr(addr) -> bytes:
    if isinstance(addr, str):
        addr = addr.encode("utf-8")
    try:
        c_addr: bytes = addr
    except TypeError:
        raise TypeError(f"Expected addr to be str, got addr={addr!r}")
    return c_addr


@cclass
class Socket:
    def __init__(
        self,
        context=None,
        socket_type: C.int = -1,
        shadow: size_t = 0,
        copy_threshold=None,
    ):
        self.handle = NULL
        self._draft_poller = NULL
        self._pid = 0
        self._shadow = False
        self.context = None
        if copy_threshold is None:
            copy_threshold = zmq.COPY_THRESHOLD
        self.copy_threshold = copy_threshold
        self.handle = NULL
        self.context = context
        if shadow:
            self._shadow = True
            self.handle = cast(p_void, shadow)
        else:
            if context is None:
                raise TypeError("context must be specified")
            if socket_type < 0:
                raise TypeError("socket_type must be specified")
            self._shadow = False
            self.handle = zmq_socket(self.context.handle, socket_type)
        if self.handle == NULL:
            raise ZMQError()
        self._closed = False
        self._pid = getpid()

    @property
    def underlying(self):
        return cast(size_t, self.handle)

    @property
    def closed(self):
        return _check_closed_deep(self)

    def close(self, linger: int | None = None):
        rc: C.int = 0
        linger_c: C.int
        setlinger: bint = False
        if linger is not None:
            linger_c = linger
            setlinger = True
        if self.handle != NULL and (not self._closed) and (getpid() == self._pid):
            if setlinger:
                zmq_setsockopt(self.handle, ZMQ_LINGER, address(linger_c), sizeof(int))
            if self._draft_poller != NULL:
                zmq_poller_destroy(address(self._draft_poller))
                self._draft_poller = NULL
            rc = zmq_close(self.handle)
            if rc < 0 and _zmq_errno() != ENOTSOCK:
                _check_rc(rc)
            self._closed = True
            self.handle = NULL

    def set(self, option: C.int, optval):
        optval_int64_c: int64_t
        optval_int_c: C.int
        optval_c: p_char
        sz: Py_ssize_t
        _check_closed(self)
        if isinstance(optval, str):
            raise TypeError("unicode not allowed, use setsockopt_string")
        try:
            sopt = SocketOption(option)
        except ValueError:
            opt_type = _OptType.int
        else:
            opt_type = sopt._opt_type
        if opt_type == _OptType.bytes:
            if not isinstance(optval, bytes):
                raise TypeError(f"expected bytes, got: {optval!r}")
            optval_c = PyBytes_AsString(optval)
            sz = PyBytes_Size(optval)
            _setsockopt(self.handle, option, optval_c, sz)
        elif opt_type == _OptType.int64:
            if not isinstance(optval, int):
                raise TypeError(f"expected int, got: {optval!r}")
            optval_int64_c = optval
            _setsockopt(self.handle, option, address(optval_int64_c), sizeof(int64_t))
        else:
            if not isinstance(optval, int):
                raise TypeError(f"expected int, got: {optval!r}")
            optval_int_c = optval
            _setsockopt(self.handle, option, address(optval_int_c), sizeof(int))

    def get(self, option: C.int):
        optval_int64_c = declare(int64_t)
        optval_int_c = declare(C.int)
        optval_fd_c = declare(fd_t)
        identity_str_c = declare(char[255])
        sz: size_t
        _check_closed(self)
        try:
            sopt = SocketOption(option)
        except ValueError:
            opt_type = _OptType.int
        else:
            opt_type = sopt._opt_type
        if opt_type == _OptType.bytes:
            sz = 255
            _getsockopt(self.handle, option, cast(p_void, identity_str_c), address(sz))
            if (
                option != ZMQ_IDENTITY
                and sz > 0
                and (cast(p_char, identity_str_c)[sz - 1] == b"\x00")
            ):
                sz -= 1
            result = PyBytes_FromStringAndSize(cast(p_char, identity_str_c), sz)
        elif opt_type == _OptType.int64:
            sz = sizeof(int64_t)
            _getsockopt(
                self.handle, option, cast(p_void, address(optval_int64_c)), address(sz)
            )
            result = optval_int64_c
        elif option == ZMQ_FD and self._draft_poller != NULL:
            rc = zmq_poller_fd(self._draft_poller, address(optval_fd_c))
            _check_rc(rc)
            result = optval_fd_c
        elif opt_type == _OptType.fd:
            sz = sizeof(fd_t)
            try:
                _getsockopt(
                    self.handle, option, cast(p_void, address(optval_fd_c)), address(sz)
                )
            except ZMQError as e:
                if (
                    option == ZMQ_FD
                    and e.errno == zmq.Errno.EINVAL
                    and self.get(ZMQ_THREAD_SAFE)
                ):
                    _check_version(
                        (4, 3, 2), "draft socket FD support via zmq_poller_fd"
                    )
                    if not zmq.DRAFT_API:
                        raise RuntimeError(
                            "libzmq and pyzmq must be built with draft support"
                        )
                    warnings.warn(zmq.error.DraftFDWarning(), stacklevel=2)
                    self._draft_poller = zmq_poller_new()
                    if self._draft_poller == NULL:
                        raise
                    rc = zmq_poller_add(
                        self._draft_poller, self.handle, NULL, ZMQ_POLLIN | ZMQ_POLLOUT
                    )
                    _check_rc(rc)
                    rc = zmq_poller_fd(self._draft_poller, address(optval_fd_c))
                    _check_rc(rc)
                else:
                    raise
            result = optval_fd_c
        else:
            sz = sizeof(int)
            _getsockopt(
                self.handle, option, cast(p_void, address(optval_int_c)), address(sz)
            )
            result = optval_int_c
        return result

    def bind(self, addr: str | bytes):
        _addr_bytes: bytes = _c_addr(addr)
        c_addr: p_char = _addr_bytes
        _check_closed(self)
        rc: C.int = zmq_bind(self.handle, c_addr)
        if rc != 0:
            _errno: C.int = _zmq_errno()
            _ipc_max: C.int = get_ipc_path_max_len()
            if _ipc_max and _errno == ENAMETOOLONG:
                path = addr.split("://", 1)[-1]
                msg = f'ipc path "{path}" is longer than {_ipc_max} characters (sizeof(sockaddr_un.sun_path)). zmq.IPC_PATH_MAX_LEN constant can be used to check addr length (if it is defined).'
                raise ZMQError(msg=msg)
            elif _errno == ENOENT:
                path = addr.split("://", 1)[-1]
                msg = f'No such file or directory for ipc path "{path}".'
                raise ZMQError(msg=msg)
        while True:
            try:
                _check_rc(rc)
            except InterruptedSystemCall:
                rc = zmq_bind(self.handle, c_addr)
                continue
            else:
                break

    def connect(self, addr: str | bytes) -> None:
        rc: C.int
        _addr_bytes: bytes = _c_addr(addr)
        c_addr: p_char = _addr_bytes
        _check_closed(self)
        while True:
            try:
                rc = zmq_connect(self.handle, c_addr)
                _check_rc(rc)
            except InterruptedSystemCall:
                continue
            else:
                break

    def unbind(self, addr: str | bytes):
        _addr_bytes: bytes = _c_addr(addr)
        c_addr: p_char = _addr_bytes
        _check_closed(self)
        rc: C.int = zmq_unbind(self.handle, c_addr)
        if rc != 0:
            raise ZMQError()

    def disconnect(self, addr: str | bytes):
        _addr_bytes: bytes = _c_addr(addr)
        c_addr: p_char = _addr_bytes
        _check_closed(self)
        rc: C.int = zmq_disconnect(self.handle, c_addr)
        if rc != 0:
            raise ZMQError()

    def monitor(self, addr: str | bytes | None, events: C.int = ZMQ_EVENT_ALL):
        c_addr: p_char = NULL
        if addr is not None:
            _addr_bytes: bytes = _c_addr(addr)
            c_addr: p_char = _addr_bytes
        _check_closed(self)
        _check_rc(zmq_socket_monitor(self.handle, c_addr, events))

    def join(self, group: str | bytes):
        _check_version((4, 2), "RADIO-DISH")
        if not zmq.DRAFT_API:
            raise RuntimeError("libzmq and pyzmq must be built with draft support")
        if isinstance(group, str):
            group = group.encode("utf8")
        c_group: bytes = group
        rc: C.int = zmq_join(self.handle, c_group)
        _check_rc(rc)

    def leave(self, group):
        _check_version((4, 2), "RADIO-DISH")
        if not zmq.DRAFT_API:
            raise RuntimeError("libzmq and pyzmq must be built with draft support")
        rc: C.int = zmq_leave(self.handle, group)
        _check_rc(rc)

    def send(self, data, flags=0, copy: bint = True, track: bint = False):
        _check_closed(self)
        if isinstance(data, str):
            raise TypeError("unicode not allowed, use send_string")
        if copy and (not isinstance(data, Frame)):
            return _send_copy(self.handle, data, flags)
        else:
            if isinstance(data, Frame):
                if track and (not data.tracker):
                    raise ValueError("Not a tracked message")
                msg = data
            else:
                if self.copy_threshold:
                    buf = memoryview(data)
                    nbytes: size_t = buf.nbytes
                    copy_threshold: size_t = self.copy_threshold
                    if nbytes < copy_threshold:
                        _send_copy(self.handle, buf, flags)
                        return zmq._FINISHED_TRACKER
                msg = Frame(data, track=track, copy_threshold=self.copy_threshold)
            return _send_frame(self.handle, msg, flags)

    def recv(self, flags=0, copy: bint = True, track: bint = False):
        _check_closed(self)
        if copy:
            return _recv_copy(self.handle, flags)
        else:
            frame = _recv_frame(self.handle, flags, track)
            more: bint = False
            sz: size_t = sizeof(bint)
            _getsockopt(
                self.handle, ZMQ_RCVMORE, cast(p_void, address(more)), address(sz)
            )
            frame.more = more
            return frame

    def recv_into(self, buffer, /, *, nbytes=0, flags=0) -> C.int:
        c_flags: C.int = flags
        _check_closed(self)
        c_nbytes: size_t = nbytes
        if c_nbytes < 0:
            raise ValueError(f"nbytes={nbytes!r} must be non-negative")
        view = memoryview(buffer)
        c_data = declare(pointer(C.void))
        view_bytes: C.size_t = _asbuffer(view, address(c_data), True)
        if nbytes == 0:
            c_nbytes = view_bytes
        elif c_nbytes > view_bytes:
            raise ValueError(
                f"nbytes={nbytes!r} too big for memoryview of {view_bytes}B"
            )
        while True:
            with nogil:
                rc: C.int = zmq_recv(self.handle, c_data, c_nbytes, c_flags)
            try:
                _check_rc(rc)
            except InterruptedSystemCall:
                continue
            else:
                return rc


@inline
@cfunc
def _check_closed(s: Socket):
    if s._closed:
        raise ZMQError(ENOTSOCK)


@inline
@cfunc
def _check_closed_deep(s: Socket) -> bint:
    rc: C.int
    errno: C.int
    stype = declare(C.int)
    sz: size_t = sizeof(int)
    if s._closed:
        return True
    else:
        rc = zmq_getsockopt(
            s.handle, ZMQ_TYPE, cast(p_void, address(stype)), address(sz)
        )
        if rc < 0:
            errno = _zmq_errno()
            if errno == ENOTSOCK:
                s._closed = True
                return True
            elif errno == ZMQ_ETERM:
                return False
        else:
            _check_rc(rc)
    return False


@cfunc
@inline
def _recv_frame(handle: p_void, flags: C.int = 0, track: bint = False) -> Frame:
    rc: C.int
    msg = zmq.Frame(track=track)
    cmsg: Frame = msg
    while True:
        with nogil:
            rc = zmq_msg_recv(address(cmsg.zmq_msg), handle, flags)
        try:
            _check_rc(rc)
        except InterruptedSystemCall:
            continue
        else:
            break
    return msg


@cfunc
@inline
def _recv_copy(handle: p_void, flags: C.int = 0):
    zmq_msg = declare(zmq_msg_t)
    zmq_msg_p: pointer(zmq_msg_t) = address(zmq_msg)
    rc: C.int = zmq_msg_init(zmq_msg_p)
    _check_rc(rc)
    while True:
        with nogil:
            rc = zmq_msg_recv(zmq_msg_p, handle, flags)
        try:
            _check_rc(rc)
        except InterruptedSystemCall:
            continue
        except Exception:
            zmq_msg_close(zmq_msg_p)
            raise
        else:
            break
    msg_bytes = _copy_zmq_msg_bytes(zmq_msg_p)
    zmq_msg_close(zmq_msg_p)
    return msg_bytes


@cfunc
@inline
def _send_frame(handle: p_void, msg: Frame, flags: C.int = 0):
    rc: C.int
    msg_copy: Frame
    msg_copy = msg.fast_copy()
    while True:
        with nogil:
            rc = zmq_msg_send(address(msg_copy.zmq_msg), handle, flags)
        try:
            _check_rc(rc)
        except InterruptedSystemCall:
            continue
        else:
            break
    return msg.tracker


@cfunc
@inline
def _send_copy(handle: p_void, buf, flags: C.int = 0):
    rc: C.int
    msg = declare(zmq_msg_t)
    c_bytes = declare(p_void)
    c_bytes_len = _asbuffer(buf, address(c_bytes))
    rc = zmq_msg_init_size(address(msg), c_bytes_len)
    _check_rc(rc)
    while True:
        with nogil:
            memcpy(zmq_msg_data(address(msg)), c_bytes, zmq_msg_size(address(msg)))
            rc = zmq_msg_send(address(msg), handle, flags)
        try:
            _check_rc(rc)
        except InterruptedSystemCall:
            continue
        except Exception:
            zmq_msg_close(address(msg))
            raise
        else:
            rc = zmq_msg_close(address(msg))
            _check_rc(rc)
            break


@cfunc
@inline
def _getsockopt(handle: p_void, option: C.int, optval: p_void, sz: pointer(size_t)):
    rc: C.int = 0
    while True:
        rc = zmq_getsockopt(handle, option, optval, sz)
        try:
            _check_rc(rc)
        except InterruptedSystemCall:
            continue
        else:
            break


@cfunc
@inline
def _setsockopt(handle: p_void, option: C.int, optval: p_void, sz: size_t):
    rc: C.int = 0
    while True:
        rc = zmq_setsockopt(handle, option, optval, sz)
        try:
            _check_rc(rc)
        except InterruptedSystemCall:
            continue
        else:
            break


def zmq_errno() -> C.int:
    return _zmq_errno()


def strerror(errno: C.int) -> str:
    str_e: bytes = zmq_strerror(errno)
    return str_e.decode("utf8", "replace")


def zmq_version_info() -> tuple[int, int, int]:
    major: C.int = 0
    minor: C.int = 0
    patch: C.int = 0
    _zmq_version(address(major), address(minor), address(patch))
    return (major, minor, patch)


def has(capability: str) -> bool:
    _check_version((4, 1), "zmq.has")
    ccap: bytes = capability.encode("utf8")
    return bool(zmq_has(ccap))


def curve_keypair() -> tuple[bytes, bytes]:
    rc: C.int
    public_key = declare(char[64])
    secret_key = declare(char[64])
    _check_version((4, 0), "curve_keypair")
    rc = zmq_curve_keypair(public_key, secret_key)
    _check_rc(rc)
    return (public_key, secret_key)


def curve_public(secret_key) -> bytes:
    if isinstance(secret_key, str):
        secret_key = secret_key.encode("utf8")
    if not len(secret_key) == 40:
        raise ValueError("secret key must be a 40 byte z85 encoded string")
    rc: C.int
    public_key = declare(char[64])
    c_secret_key: pointer(char) = secret_key
    _check_version((4, 2), "curve_public")
    rc = zmq_curve_public(public_key, c_secret_key)
    _check_rc(rc)
    return public_key[:40]


def zmq_poll(sockets, timeout: C.int = -1):
    rc: C.int
    i: C.int
    fileno: fd_t
    events: C.int
    pollitems: pointer(zmq_pollitem_t) = NULL
    nsockets: C.int = len(sockets)
    if nsockets == 0:
        return []
    pollitems = cast(pointer(zmq_pollitem_t), malloc(nsockets * sizeof(zmq_pollitem_t)))
    if pollitems == NULL:
        raise MemoryError("Could not allocate poll items")
    for i in range(nsockets):
        s, events = sockets[i]
        if isinstance(s, Socket):
            pollitems[i].socket = cast(Socket, s).handle
            pollitems[i].fd = 0
            pollitems[i].events = events
            pollitems[i].revents = 0
        elif isinstance(s, int):
            fileno = s
            pollitems[i].socket = NULL
            pollitems[i].fd = fileno
            pollitems[i].events = events
            pollitems[i].revents = 0
        elif hasattr(s, "fileno"):
            try:
                fileno = int(s.fileno())
            except Exception:
                free(pollitems)
                raise ValueError("fileno() must return a valid integer fd")
            else:
                pollitems[i].socket = NULL
                pollitems[i].fd = fileno
                pollitems[i].events = events
                pollitems[i].revents = 0
        else:
            free(pollitems)
            raise TypeError(
                f"Socket must be a 0MQ socket, an integer fd or have a fileno() method: {s!r}"
            )
    ms_passed: C.int = 0
    tic: C.int
    try:
        while True:
            start: C.int = monotonic()
            with nogil:
                rc = zmq_poll_c(pollitems, nsockets, timeout)
            try:
                _check_rc(rc)
            except InterruptedSystemCall:
                if timeout > 0:
                    tic = monotonic()
                    ms_passed = int(1000 * (tic - start))
                    if ms_passed < 0:
                        warnings.warn(
                            f"Negative elapsed time for interrupted poll: {ms_passed}.  Did the clock change?",
                            RuntimeWarning,
                        )
                        ms_passed = 0
                    timeout = max(0, timeout - ms_passed)
                continue
            else:
                break
    except Exception:
        free(pollitems)
        raise
    results = []
    for i in range(nsockets):
        revents = pollitems[i].revents
        if revents > 0:
            if pollitems[i].socket != NULL:
                s = sockets[i][0]
            else:
                s = pollitems[i].fd
            results.append((s, revents))
    free(pollitems)
    return results


def proxy(frontend: Socket, backend: Socket, capture: Socket = None):
    rc: C.int = 0
    capture_handle: p_void
    if isinstance(capture, Socket):
        capture_handle = capture.handle
    else:
        capture_handle = NULL
    while True:
        with nogil:
            rc = zmq_proxy(frontend.handle, backend.handle, capture_handle)
        try:
            _check_rc(rc)
        except InterruptedSystemCall:
            continue
        else:
            break
    return rc


def proxy_steerable(
    frontend: Socket, backend: Socket, capture: Socket = None, control: Socket = None
):
    rc: C.int = 0
    capture_handle: p_void
    if isinstance(capture, Socket):
        capture_handle = capture.handle
    else:
        capture_handle = NULL
    if isinstance(control, Socket):
        control_handle = control.handle
    else:
        control_handle = NULL
    while True:
        with nogil:
            rc = zmq_proxy_steerable(
                frontend.handle, backend.handle, capture_handle, control_handle
            )
        try:
            _check_rc(rc)
        except InterruptedSystemCall:
            continue
        else:
            break
    return rc


@cfunc
@inline
@nogil
def _mq_relay(
    in_socket: p_void,
    out_socket: p_void,
    side_socket: p_void,
    msg: zmq_msg_t,
    side_msg: zmq_msg_t,
    id_msg: zmq_msg_t,
    swap_ids: bint,
) -> C.int:
    rc: C.int
    flags: C.int
    flagsz = declare(size_t)
    more = declare(int)
    flagsz = sizeof(int)
    if swap_ids:
        rc = zmq_msg_recv(address(msg), in_socket, 0)
        if rc < 0:
            return rc
        rc = zmq_msg_recv(address(id_msg), in_socket, 0)
        if rc < 0:
            return rc
        rc = zmq_msg_copy(address(side_msg), address(id_msg))
        if rc < 0:
            return rc
        rc = zmq_msg_send(address(side_msg), out_socket, ZMQ_SNDMORE)
        if rc < 0:
            return rc
        rc = zmq_msg_send(address(id_msg), side_socket, ZMQ_SNDMORE)
        if rc < 0:
            return rc
        rc = zmq_msg_copy(address(side_msg), address(msg))
        if rc < 0:
            return rc
        rc = zmq_msg_send(address(side_msg), out_socket, ZMQ_SNDMORE)
        if rc < 0:
            return rc
        rc = zmq_msg_send(address(msg), side_socket, ZMQ_SNDMORE)
        if rc < 0:
            return rc
    while True:
        rc = zmq_msg_recv(address(msg), in_socket, 0)
        if rc < 0:
            return rc
        rc = zmq_getsockopt(in_socket, ZMQ_RCVMORE, address(more), address(flagsz))
        if rc < 0:
            return rc
        flags = 0
        if more:
            flags |= ZMQ_SNDMORE
        rc = zmq_msg_copy(address(side_msg), address(msg))
        if rc < 0:
            return rc
        if flags:
            rc = zmq_msg_send(address(side_msg), out_socket, flags)
            if rc < 0:
                return rc
            rc = zmq_msg_send(address(msg), side_socket, ZMQ_SNDMORE)
            if rc < 0:
                return rc
        else:
            rc = zmq_msg_send(address(side_msg), out_socket, 0)
            if rc < 0:
                return rc
            rc = zmq_msg_send(address(msg), side_socket, 0)
            if rc < 0:
                return rc
            break
    return rc


@cfunc
@inline
@nogil
def _mq_inline(
    in_socket: p_void,
    out_socket: p_void,
    side_socket: p_void,
    in_msg_ptr: pointer(zmq_msg_t),
    out_msg_ptr: pointer(zmq_msg_t),
    swap_ids: bint,
) -> C.int:
    msg: zmq_msg_t = declare(zmq_msg_t)
    rc: C.int = zmq_msg_init(address(msg))
    id_msg = declare(zmq_msg_t)
    rc = zmq_msg_init(address(id_msg))
    if rc < 0:
        return rc
    side_msg = declare(zmq_msg_t)
    rc = zmq_msg_init(address(side_msg))
    if rc < 0:
        return rc
    items = declare(zmq_pollitem_t[2])
    items[0].socket = in_socket
    items[0].events = ZMQ_POLLIN
    items[0].fd = items[0].revents = 0
    items[1].socket = out_socket
    items[1].events = ZMQ_POLLIN
    items[1].fd = items[1].revents = 0
    while True:
        rc = zmq_poll_c(address(items[0]), 2, -1)
        if rc < 0:
            return rc
        if items[0].revents & ZMQ_POLLIN:
            rc = zmq_msg_copy(address(side_msg), in_msg_ptr)
            if rc < 0:
                return rc
            rc = zmq_msg_send(address(side_msg), side_socket, ZMQ_SNDMORE)
            if rc < 0:
                return rc
            rc = _mq_relay(
                in_socket, out_socket, side_socket, msg, side_msg, id_msg, swap_ids
            )
            if rc < 0:
                return rc
        if items[1].revents & ZMQ_POLLIN:
            rc = zmq_msg_copy(address(side_msg), out_msg_ptr)
            if rc < 0:
                return rc
            rc = zmq_msg_send(address(side_msg), side_socket, ZMQ_SNDMORE)
            if rc < 0:
                return rc
            rc = _mq_relay(
                out_socket, in_socket, side_socket, msg, side_msg, id_msg, swap_ids
            )
            if rc < 0:
                return rc
    return rc


def monitored_queue(
    in_socket: Socket,
    out_socket: Socket,
    mon_socket: Socket,
    in_prefix: bytes = b"in",
    out_prefix: bytes = b"out",
):
    ins: p_void = in_socket.handle
    outs: p_void = out_socket.handle
    mons: p_void = mon_socket.handle
    in_msg = declare(zmq_msg_t)
    out_msg = declare(zmq_msg_t)
    swap_ids: bint
    msg_c: p_void = NULL
    msg_c_len = declare(Py_ssize_t)
    rc: C.int
    swap_ids = in_socket.type == ZMQ_ROUTER and out_socket.type == ZMQ_ROUTER
    msg_c_len = _asbuffer(in_prefix, address(msg_c))
    rc = zmq_msg_init_size(address(in_msg), msg_c_len)
    _check_rc(rc)
    memcpy(zmq_msg_data(address(in_msg)), msg_c, zmq_msg_size(address(in_msg)))
    msg_c_len = _asbuffer(out_prefix, address(msg_c))
    rc = zmq_msg_init_size(address(out_msg), msg_c_len)
    _check_rc(rc)
    while True:
        with nogil:
            memcpy(
                zmq_msg_data(address(out_msg)), msg_c, zmq_msg_size(address(out_msg))
            )
            rc = _mq_inline(
                ins, outs, mons, address(in_msg), address(out_msg), swap_ids
            )
        try:
            _check_rc(rc)
        except InterruptedSystemCall:
            continue
        else:
            break
    return rc


__all__ = [
    "IPC_PATH_MAX_LEN",
    "PYZMQ_DRAFT_API",
    "Context",
    "Socket",
    "Frame",
    "has",
    "curve_keypair",
    "curve_public",
    "zmq_version_info",
    "zmq_errno",
    "zmq_poll",
    "strerror",
    "proxy",
    "proxy_steerable",
]
