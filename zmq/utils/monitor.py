"""Module holding utility and convenience functions for zmq event monitoring."""

from __future__ import annotations
import struct
from typing import Awaitable, TypedDict, overload
import zmq
import zmq.asyncio
from zmq.error import _check_version


class _MonitorMessage(TypedDict):
    event: int
    value: int
    endpoint: bytes


def parse_monitor_message(msg: list[bytes]) -> _MonitorMessage:
    if len(msg) != 2 or len(msg[0]) != 6:
        raise RuntimeError(f"Invalid event message format: {msg}")
    event_id, value = struct.unpack("=hi", msg[0])
    event: _MonitorMessage = {
        "event": zmq.Event(event_id),
        "value": zmq.Event(value),
        "endpoint": msg[1],
    }
    return event


async def _parse_monitor_msg_async(
    awaitable_msg: Awaitable[list[bytes]],
) -> _MonitorMessage:
    msg = await awaitable_msg
    return parse_monitor_message(msg)


@overload
def recv_monitor_message(
    socket: zmq.asyncio.Socket, flags: int = 0
) -> Awaitable[_MonitorMessage]: ...


@overload
def recv_monitor_message(
    socket: zmq.Socket[bytes], flags: int = 0
) -> _MonitorMessage: ...


def recv_monitor_message(
    socket: zmq.Socket, flags: int = 0
) -> _MonitorMessage | Awaitable[_MonitorMessage]:
    _check_version((4, 0), "libzmq event API")
    msg = socket.recv_multipart(flags)
    if isinstance(msg, Awaitable):
        return _parse_monitor_msg_async(msg)
    return parse_monitor_message(msg)


__all__ = ["parse_monitor_message", "recv_monitor_message"]
