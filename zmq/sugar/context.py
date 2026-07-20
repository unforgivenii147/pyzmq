"""Python bindings for 0MQ."""

from __future__ import annotations
import atexit
import os
from threading import Lock
from typing import Any, Callable, Generic, TypeVar, overload
from warnings import warn
from weakref import WeakSet
import zmq
from zmq._typing import TypeAlias
from zmq.backend import Context as ContextBase
from zmq.constants import ContextOption, Errno, SocketOption
from zmq.error import ZMQError
from zmq.utils.interop import cast_int_addr
from .attrsettr import AttributeSetter, OptValT
from .socket import Socket, SyncSocket

_exiting = False


def _notice_atexit() -> None:
    global _exiting
    _exiting = True


atexit.register(_notice_atexit)
_ContextType = TypeVar("_ContextType", bound="Context")
_SocketType = TypeVar("_SocketType", bound="Socket", covariant=True)


class Context(ContextBase, AttributeSetter, Generic[_SocketType]):
    sockopts: dict[int, Any]
    _instance: Any = None
    _instance_lock = Lock()
    _instance_pid: int | None = None
    _shadow = False
    _shadow_obj = None
    _warn_destroy_close = False
    _sockets: WeakSet
    _socket_class: type[_SocketType] = Socket

    @overload
    def __init__(self: SyncContext, io_threads: int = 1): ...

    @overload
    def __init__(self: SyncContext, io_threads: Context, /): ...

    @overload
    def __init__(self: SyncContext, *, shadow: Context | int): ...

    def __init__(
        self: SyncContext, io_threads: int | Context = 1, shadow: Context | int = 0
    ) -> None:
        if isinstance(io_threads, Context):
            shadow = io_threads
            io_threads = 1
        shadow_address: int = 0
        if shadow:
            self._shadow = True
            self._shadow_obj = shadow
            if not isinstance(shadow, int):
                try:
                    shadow = shadow.underlying
                except AttributeError:
                    pass
            shadow_address = cast_int_addr(shadow)
        else:
            self._shadow = False
        super().__init__(io_threads=io_threads, shadow=shadow_address)
        self.sockopts = {}
        self._sockets = WeakSet()

    def __del__(self) -> None:
        locals()
        if not self._shadow and (not _exiting) and (not self.closed):
            self._warn_destroy_close = True
            if warn is not None and getattr(self, "_sockets", None) is not None:
                warn(
                    f"Unclosed context {self}",
                    ResourceWarning,
                    stacklevel=2,
                    source=self,
                )
            self.destroy()

    _repr_cls = "zmq.Context"

    def __repr__(self) -> str:
        cls = self.__class__
        _repr_cls = cls.__dict__.get("_repr_cls", None)
        if _repr_cls is None:
            _repr_cls = f"{cls.__module__}.{cls.__name__}"
        closed = " closed" if self.closed else ""
        if getattr(self, "_sockets", None):
            n_sockets = len(self._sockets)
            s = "s" if n_sockets > 1 else ""
            sockets = f"{n_sockets} socket{s}"
        else:
            sockets = ""
        return f"<{_repr_cls}({sockets}) at {hex(id(self))}{closed}>"

    def __enter__(self: _ContextType) -> _ContextType:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self._warn_destroy_close = True
        self.destroy()

    def __copy__(self: _ContextType, memo: Any = None) -> _ContextType:
        return self.__class__.shadow(self.underlying)

    __deepcopy__ = __copy__

    @classmethod
    def shadow(cls: type[_ContextType], address: int | zmq.Context) -> _ContextType:
        return cls(shadow=address)

    @classmethod
    def shadow_pyczmq(cls: type[_ContextType], ctx: Any) -> _ContextType:
        from pyczmq import zctx
        from zmq.utils.interop import cast_int_addr

        underlying = zctx.underlying(ctx)
        address = cast_int_addr(underlying)
        return cls(shadow=address)

    @classmethod
    def instance(cls: type[_ContextType], io_threads: int = 1) -> _ContextType:
        if (
            cls._instance is None
            or cls._instance_pid != os.getpid()
            or cls._instance.closed
        ):
            with cls._instance_lock:
                if (
                    cls._instance is None
                    or cls._instance_pid != os.getpid()
                    or cls._instance.closed
                ):
                    cls._instance = cls(io_threads=io_threads)
                    cls._instance_pid = os.getpid()
        return cls._instance

    def term(self) -> None:
        super().term()

    def __dir__(self) -> list[str]:
        keys = dir(self.__class__)
        keys.extend(ContextOption.__members__)
        return keys

    def _add_socket(self, socket: Any) -> None:
        self._sockets.add(socket)

    def _rm_socket(self, socket: Any) -> None:
        if getattr(self, "_sockets", None) is not None:
            self._sockets.discard(socket)

    def destroy(self, linger: int | None = None) -> None:
        if self.closed:
            return
        sockets: list[_SocketType] = list(getattr(self, "_sockets", None) or [])
        for s in sockets:
            if s and (not s.closed):
                if self._warn_destroy_close and warn is not None:
                    warn(
                        f"Destroying context with unclosed socket {s}",
                        ResourceWarning,
                        stacklevel=3,
                        source=s,
                    )
                if linger is not None:
                    s.setsockopt(SocketOption.LINGER, linger)
                s.close()
        self.term()

    def socket(
        self: _ContextType,
        socket_type: int,
        socket_class: Callable[[_ContextType, int], _SocketType] | None = None,
        **kwargs: Any,
    ) -> _SocketType:
        if self.closed:
            raise ZMQError(Errno.ENOTSUP)
        if socket_class is None:
            socket_class = self._socket_class
        s: _SocketType = socket_class(self, socket_type, **kwargs)
        for opt, value in self.sockopts.items():
            try:
                s.setsockopt(opt, value)
            except ZMQError:
                pass
        self._add_socket(s)
        return s

    def setsockopt(self, opt: int, value: Any) -> None:
        self.sockopts[opt] = value

    def getsockopt(self, opt: int) -> OptValT:
        return self.sockopts[opt]

    def _set_attr_opt(self, name: str, opt: int, value: OptValT) -> None:
        if name in ContextOption.__members__:
            return self.set(opt, value)
        elif name in SocketOption.__members__:
            self.sockopts[opt] = value
        else:
            raise AttributeError(f"No such context or socket option: {name}")

    def _get_attr_opt(self, name: str, opt: int) -> OptValT:
        if name in ContextOption.__members__:
            return self.get(opt)
        elif opt not in self.sockopts:
            raise AttributeError(name)
        else:
            return self.sockopts[opt]

    def __delattr__(self, key: str) -> None:
        if key in self.__dict__:
            self.__dict__.pop(key)
            return
        key = key.upper()
        try:
            opt = getattr(SocketOption, key)
        except AttributeError:
            raise AttributeError(f"No such socket option: {key!r}")
        else:
            if opt not in self.sockopts:
                raise AttributeError(key)
            else:
                del self.sockopts[opt]


SyncContext: TypeAlias = Context[SyncSocket]
__all__ = ["Context", "SyncContext"]
