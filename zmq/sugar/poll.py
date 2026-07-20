"""0MQ polling related functions and classes."""

from __future__ import annotations
from typing import Any
from zmq.backend import zmq_poll
from zmq.constants import POLLERR, POLLIN, POLLOUT


class Poller:
    sockets: list[tuple[Any, int]]
    _map: dict

    def __init__(self) -> None:
        self.sockets = []
        self._map = {}

    def __contains__(self, socket: Any) -> bool:
        return socket in self._map

    def register(self, socket: Any, flags: int = POLLIN | POLLOUT):
        if flags:
            if socket in self._map:
                idx = self._map[socket]
                self.sockets[idx] = (socket, flags)
            else:
                idx = len(self.sockets)
                self.sockets.append((socket, flags))
                self._map[socket] = idx
        elif socket in self._map:
            self.unregister(socket)
        else:
            pass

    def modify(self, socket, flags=POLLIN | POLLOUT):
        self.register(socket, flags)

    def unregister(self, socket: Any):
        idx = self._map.pop(socket)
        self.sockets.pop(idx)
        for socket, flags in self.sockets[idx:]:
            self._map[socket] -= 1

    def poll(self, timeout: int | None = None) -> list[tuple[Any, int]]:
        if timeout is None or timeout < 0:
            timeout = -1
        elif isinstance(timeout, float):
            timeout = int(timeout)
        return zmq_poll(self.sockets, timeout=timeout)


def select(
    rlist: list, wlist: list, xlist: list, timeout: float | None = None
) -> tuple[list, list, list]:
    if timeout is None:
        timeout = -1
    timeout = int(timeout * 1000.0)
    if timeout < 0:
        timeout = -1
    sockets = []
    for s in set(rlist + wlist + xlist):
        flags = 0
        if s in rlist:
            flags |= POLLIN
        if s in wlist:
            flags |= POLLOUT
        if s in xlist:
            flags |= POLLERR
        sockets.append((s, flags))
    return_sockets = zmq_poll(sockets, timeout)
    rlist, wlist, xlist = ([], [], [])
    for s, flags in return_sockets:
        if flags & POLLIN:
            rlist.append(s)
        if flags & POLLOUT:
            wlist.append(s)
        if flags & POLLERR:
            xlist.append(s)
    return (rlist, wlist, xlist)


__all__ = ["Poller", "select"]
