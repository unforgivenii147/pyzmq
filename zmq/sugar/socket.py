"""0MQ Socket pure Python methods."""

from __future__ import annotations
import errno
import pickle
import random
import sys
from typing import (
    Any,
    Callable,
    Generic,
    List,
    Literal,
    Sequence,
    TypeVar,
    Union,
    cast,
    overload,
)
from warnings import warn
import zmq
from zmq._typing import TypeAlias
from zmq.backend import Socket as SocketBase
from zmq.error import ZMQBindError, ZMQError
from zmq.utils import jsonapi
from zmq.utils.interop import cast_int_addr
from ..constants import SocketOption, SocketType, _OptType
from .attrsettr import AttributeSetter
from .poll import Poller

try:
    DEFAULT_PROTOCOL = pickle.DEFAULT_PROTOCOL
except AttributeError:
    DEFAULT_PROTOCOL = pickle.HIGHEST_PROTOCOL
_SocketType = TypeVar("_SocketType", bound="Socket")
_JSONType: TypeAlias = "int | str | bool | list[_JSONType] | dict[str, _JSONType]"


class _SocketContext(Generic[_SocketType]):
    socket: _SocketType
    kind: str
    addr: str

    def __repr__(self):
        return f"<SocketContext({self.kind}={self.addr!r})>"

    def __init__(
        self: _SocketContext[_SocketType], socket: _SocketType, kind: str, addr: str
    ):
        assert kind in {"bind", "connect"}
        self.socket = socket
        self.kind = kind
        self.addr = addr

    def __enter__(self: _SocketContext[_SocketType]) -> _SocketType:
        return self.socket

    def __exit__(self, *args):
        if self.socket.closed:
            return
        if self.kind == "bind":
            self.socket.unbind(self.addr)
        elif self.kind == "connect":
            self.socket.disconnect(self.addr)


SocketReturnType = TypeVar("SocketReturnType")


class Socket(SocketBase, AttributeSetter, Generic[SocketReturnType]):
    _shadow = False
    _shadow_obj = None
    _monitor_socket = None
    _type_name = "UNKNOWN"

    @overload
    def __init__(
        self: Socket[bytes],
        ctx_or_socket: zmq.Context,
        socket_type: int,
        *,
        copy_threshold: int | None = None,
    ): ...

    @overload
    def __init__(
        self: Socket[bytes], *, shadow: Socket | int, copy_threshold: int | None = None
    ): ...

    @overload
    def __init__(self: Socket[bytes], ctx_or_socket: Socket): ...

    def __init__(
        self: Socket[bytes],
        ctx_or_socket: zmq.Context | Socket | None = None,
        socket_type: int = 0,
        *,
        shadow: Socket | int = 0,
        copy_threshold: int | None = None,
    ):
        shadow_context: zmq.Context | None = None
        if isinstance(ctx_or_socket, zmq.Socket):
            shadow = ctx_or_socket
            ctx_or_socket = None
        shadow_address: int = 0
        if shadow:
            self._shadow = True
            self._shadow_obj = shadow
            if not isinstance(shadow, int):
                if isinstance(shadow, zmq.Socket):
                    shadow_context = shadow.context
                try:
                    shadow = cast(int, shadow.underlying)
                except AttributeError:
                    pass
            shadow_address = cast_int_addr(shadow)
        else:
            self._shadow = False
        super().__init__(
            ctx_or_socket,
            socket_type,
            shadow=shadow_address,
            copy_threshold=copy_threshold,
        )
        if self._shadow_obj and shadow_context:
            self.context = shadow_context
        try:
            socket_type = cast(int, self.get(zmq.TYPE))
        except Exception:
            pass
        else:
            try:
                self.__dict__["type"] = stype = SocketType(socket_type)
            except ValueError:
                self._type_name = str(socket_type)
            else:
                self._type_name = stype.name

    def __del__(self):
        if not self._shadow and (not self.closed):
            if warn is not None:
                warn(
                    f"Unclosed socket {self}",
                    ResourceWarning,
                    stacklevel=2,
                    source=self,
                )
            self.close()

    _repr_cls = "zmq.Socket"

    def __repr__(self):
        cls = self.__class__
        _repr_cls = cls.__dict__.get("_repr_cls", None)
        if _repr_cls is None:
            _repr_cls = f"{cls.__module__}.{cls.__name__}"
        closed = " closed" if self._closed else ""
        return f"<{_repr_cls}(zmq.{self._type_name}) at {hex(id(self))}{closed}>"

    def __enter__(self: _SocketType) -> _SocketType:
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def __copy__(self: _SocketType, memo=None) -> _SocketType:
        return self.__class__.shadow(self.underlying)

    __deepcopy__ = __copy__

    @classmethod
    def shadow(cls: type[_SocketType], address: int | zmq.Socket) -> _SocketType:
        return cls(shadow=address)

    def close(self, linger=None) -> None:
        if self.context:
            self.context._rm_socket(self)
        super().close(linger=linger)

    def _connect_cm(self: _SocketType, addr: str) -> _SocketContext[_SocketType]:
        return _SocketContext(self, "connect", addr)

    def _bind_cm(self: _SocketType, addr: str) -> _SocketContext[_SocketType]:
        try:
            addr = cast(bytes, self.get(zmq.LAST_ENDPOINT)).decode("utf8")
        except (AttributeError, ZMQError, UnicodeDecodeError):
            pass
        return _SocketContext(self, "bind", addr)

    def bind(self: _SocketType, addr: str) -> _SocketContext[_SocketType]:
        try:
            super().bind(addr)
        except ZMQError as e:
            e.strerror += f" (addr={addr!r})"
            raise
        return self._bind_cm(addr)

    def connect(self: _SocketType, addr: str) -> _SocketContext[_SocketType]:
        try:
            super().connect(addr)
        except ZMQError as e:
            e.strerror += f" (addr={addr!r})"
            raise
        return self._connect_cm(addr)

    @property
    def socket_type(self) -> int:
        warn("Socket.socket_type is deprecated, use Socket.type", DeprecationWarning)
        return cast(int, self.type)

    def __dir__(self):
        keys = dir(self.__class__)
        keys.extend(SocketOption.__members__)
        return keys

    setsockopt = SocketBase.set
    getsockopt = SocketBase.get

    def __setattr__(self, key, value):
        if key in self.__dict__:
            object.__setattr__(self, key, value)
            return
        _key = key.lower()
        if _key in ("subscribe", "unsubscribe"):
            if isinstance(value, str):
                value = value.encode("utf8")
            if _key == "subscribe":
                self.set(zmq.SUBSCRIBE, value)
            else:
                self.set(zmq.UNSUBSCRIBE, value)
            return
        super().__setattr__(key, value)

    def fileno(self) -> int:
        return self.FD

    def subscribe(self, topic: str | bytes) -> None:
        if isinstance(topic, str):
            topic = topic.encode("utf8")
        self.set(zmq.SUBSCRIBE, topic)

    def unsubscribe(self, topic: str | bytes) -> None:
        if isinstance(topic, str):
            topic = topic.encode("utf8")
        self.set(zmq.UNSUBSCRIBE, topic)

    def set_string(self, option: int, optval: str, encoding="utf-8") -> None:
        if not isinstance(optval, str):
            raise TypeError(f"strings only, not {type(optval)}: {optval!r}")
        return self.set(option, optval.encode(encoding))

    setsockopt_unicode = setsockopt_string = set_string

    def get_string(self, option: int, encoding="utf-8") -> str:
        if SocketOption(option)._opt_type != _OptType.bytes:
            raise TypeError(f"option {option} will not return a string to be decoded")
        return cast(bytes, self.get(option)).decode(encoding)

    getsockopt_unicode = getsockopt_string = get_string

    def bind_to_random_port(
        self: _SocketType,
        addr: str,
        min_port: int = 49152,
        max_port: int = 65536,
        max_tries: int = 100,
    ) -> int:
        if min_port == 49152 and max_port == 65536:
            self.bind(f"{addr}:*")
            url = cast(bytes, self.last_endpoint).decode("ascii", "replace")
            _, port_s = url.rsplit(":", 1)
            return int(port_s)
        for i in range(max_tries):
            try:
                port = random.randrange(min_port, max_port)
                self.bind(f"{addr}:{port}")
            except ZMQError as exception:
                en = exception.errno
                if en == zmq.EADDRINUSE:
                    continue
                elif sys.platform == "win32" and en == errno.EACCES:
                    continue
                else:
                    raise
            else:
                return port
        raise ZMQBindError("Could not bind socket to random port.")

    def get_hwm(self) -> int:
        try:
            return cast(int, self.get(zmq.SNDHWM))
        except zmq.ZMQError:
            pass
        return cast(int, self.get(zmq.RCVHWM))

    def set_hwm(self, value: int) -> None:
        raised = None
        try:
            self.sndhwm = value
        except Exception as e:
            raised = e
        try:
            self.rcvhwm = value
        except Exception as e:
            raised = e
        if raised:
            raise raised

    hwm = property(
        get_hwm,
        set_hwm,
        None,
        "Property for High Water Mark.\n\n        Setting hwm sets both SNDHWM and RCVHWM as appropriate.\n        It gets SNDHWM if available, otherwise RCVHWM.\n        ",
    )

    @overload
    def send(
        self,
        data: Any,
        flags: int = ...,
        copy: bool = ...,
        *,
        track: Literal[True],
        routing_id: int | None = ...,
        group: str | None = ...,
    ) -> zmq.MessageTracker: ...

    @overload
    def send(
        self,
        data: Any,
        flags: int = ...,
        copy: bool = ...,
        *,
        track: Literal[False],
        routing_id: int | None = ...,
        group: str | None = ...,
    ) -> None: ...

    @overload
    def send(
        self,
        data: Any,
        flags: int = ...,
        *,
        copy: bool = ...,
        routing_id: int | None = ...,
        group: str | None = ...,
    ) -> None: ...

    @overload
    def send(
        self,
        data: Any,
        flags: int = ...,
        copy: bool = ...,
        track: bool = ...,
        routing_id: int | None = ...,
        group: str | None = ...,
    ) -> zmq.MessageTracker | None: ...

    def send(
        self,
        data: Any,
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
        routing_id: int | None = None,
        group: str | None = None,
    ) -> zmq.MessageTracker | None:
        if routing_id is not None:
            if not isinstance(data, zmq.Frame):
                data = zmq.Frame(
                    data,
                    track=track,
                    copy=copy or None,
                    copy_threshold=self.copy_threshold,
                )
            data.routing_id = routing_id
        if group is not None:
            if not isinstance(data, zmq.Frame):
                data = zmq.Frame(
                    data,
                    track=track,
                    copy=copy or None,
                    copy_threshold=self.copy_threshold,
                )
            data.group = group
        return super().send(data, flags=flags, copy=copy, track=track)

    def send_multipart(
        self,
        msg_parts: Sequence,
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
        **kwargs,
    ):
        for i, msg in enumerate(msg_parts):
            if isinstance(msg, (zmq.Frame, bytes, memoryview)):
                continue
            try:
                memoryview(msg)
            except Exception:
                rmsg = repr(msg)
                if len(rmsg) > 32:
                    rmsg = rmsg[:32] + "..."
                raise TypeError(
                    f"Frame {i} ({rmsg}) does not support the buffer interface."
                )
        for msg in msg_parts[:-1]:
            self.send(msg, zmq.SNDMORE | flags, copy=copy, track=track)
        return self.send(msg_parts[-1], flags, copy=copy, track=track)

    @overload
    def recv_multipart(
        self, flags: int = ..., *, copy: Literal[True], track: bool = ...
    ) -> list[bytes]: ...

    @overload
    def recv_multipart(
        self, flags: int = ..., *, copy: Literal[False], track: bool = ...
    ) -> list[zmq.Frame]: ...

    @overload
    def recv_multipart(self, flags: int = ..., *, track: bool = ...) -> list[bytes]: ...

    @overload
    def recv_multipart(
        self, flags: int = 0, copy: bool = True, track: bool = False
    ) -> list[zmq.Frame] | list[bytes]: ...

    def recv_multipart(
        self, flags: int = 0, copy: bool = True, track: bool = False
    ) -> list[zmq.Frame] | list[bytes]:
        parts = [self.recv(flags, copy=copy, track=track)]
        while self.getsockopt(zmq.RCVMORE):
            part = self.recv(flags, copy=copy, track=track)
            parts.append(part)
        return cast(Union[List[zmq.Frame], List[bytes]], parts)

    def _deserialize(self, recvd: bytes, load: Callable[[bytes], Any]) -> Any:
        return load(recvd)

    def send_serialized(self, msg, serialize, flags=0, copy=True, **kwargs):
        frames = serialize(msg)
        return self.send_multipart(frames, flags=flags, copy=copy, **kwargs)

    def recv_serialized(self, deserialize, flags=0, copy=True):
        frames = self.recv_multipart(flags=flags, copy=copy)
        return self._deserialize(frames, deserialize)

    def send_string(
        self,
        u: str,
        flags: int = 0,
        copy: bool = True,
        encoding: str = "utf-8",
        **kwargs,
    ) -> zmq.Frame | None:
        if not isinstance(u, str):
            raise TypeError("str objects only")
        return self.send(u.encode(encoding), flags=flags, copy=copy, **kwargs)

    send_unicode = send_string

    def recv_string(self, flags: int = 0, encoding: str = "utf-8") -> str:
        msg = self.recv(flags=flags)
        return self._deserialize(msg, lambda buf: buf.decode(encoding))

    recv_unicode = recv_string

    def send_pyobj(
        self, obj: Any, flags: int = 0, protocol: int = DEFAULT_PROTOCOL, **kwargs
    ) -> zmq.Frame | None:
        msg = pickle.dumps(obj, protocol)
        return self.send(msg, flags=flags, **kwargs)

    def recv_pyobj(self, flags: int = 0) -> Any:
        msg = self.recv(flags)
        return self._deserialize(msg, pickle.loads)

    def send_json(self, obj: Any, flags: int = 0, **kwargs) -> None:
        send_kwargs = {}
        for key in ("routing_id", "group"):
            if key in kwargs:
                send_kwargs[key] = kwargs.pop(key)
        msg = jsonapi.dumps(obj, **kwargs)
        return self.send(msg, flags=flags, **send_kwargs)

    def recv_json(self, flags: int = 0, **kwargs) -> _JSONType:
        msg = self.recv(flags)
        return self._deserialize(msg, lambda buf: jsonapi.loads(buf, **kwargs))

    _poller_class = Poller

    def poll(self, timeout: int | None = None, flags: int = zmq.POLLIN) -> int:
        if self.closed:
            raise ZMQError(zmq.ENOTSUP)
        p = self._poller_class()
        p.register(self, flags)
        evts = dict(p.poll(timeout))
        return evts.get(self, 0)

    def get_monitor_socket(
        self: _SocketType, events: int | None = None, addr: str | None = None
    ) -> _SocketType:
        if zmq.zmq_version_info() < (4,):
            raise NotImplementedError(
                f"get_monitor_socket requires libzmq >= 4, have {zmq.zmq_version()}"
            )
        if self._monitor_socket:
            if self._monitor_socket.closed:
                self._monitor_socket = None
            else:
                return self._monitor_socket
        if addr is None:
            addr = f"inproc://monitor.s-{self.FD}"
        if events is None:
            events = zmq.EVENT_ALL
        self.monitor(addr, events)
        self._monitor_socket = self.context.socket(zmq.PAIR)
        self._monitor_socket.connect(addr)
        return self._monitor_socket

    def disable_monitor(self) -> None:
        self._monitor_socket = None
        self.monitor(None, 0)


SyncSocket: TypeAlias = Socket[bytes]
__all__ = ["Socket", "SyncSocket"]
