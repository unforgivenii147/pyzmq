from __future__ import annotations
import gevent
from gevent import select
import zmq
from zmq import Poller as _original_Poller


class _Poller(_original_Poller):
    _gevent_bug_timeout = 1.33

    def _get_descriptors(self):
        rlist = []
        wlist = []
        xlist = []
        for socket, flags in self.sockets:
            if isinstance(socket, zmq.Socket):
                rlist.append(socket.getsockopt(zmq.FD))
                continue
            elif isinstance(socket, int):
                fd = socket
            elif hasattr(socket, "fileno"):
                try:
                    fd = int(socket.fileno())
                except Exception:
                    raise ValueError("fileno() must return an valid integer fd")
            else:
                raise TypeError(
                    f"Socket must be a 0MQ socket, an integer fd or have a fileno() method: {socket!r}"
                )
            if flags & zmq.POLLIN:
                rlist.append(fd)
            if flags & zmq.POLLOUT:
                wlist.append(fd)
            if flags & zmq.POLLERR:
                xlist.append(fd)
        return (rlist, wlist, xlist)

    def poll(self, timeout=-1):
        if timeout is None:
            timeout = -1
        if timeout < 0:
            timeout = -1
        rlist = None
        wlist = None
        xlist = None
        if timeout > 0:
            tout = gevent.Timeout.start_new(timeout / 1000.0)
        else:
            tout = None
        try:
            rlist, wlist, xlist = self._get_descriptors()
            while True:
                events = super().poll(0)
                if events or timeout == 0:
                    return events
                _bug_timeout = gevent.Timeout.start_new(self._gevent_bug_timeout)
                try:
                    select.select(rlist, wlist, xlist)
                except gevent.Timeout as t:
                    if t is not _bug_timeout:
                        raise
                finally:
                    _bug_timeout.cancel()
        except gevent.Timeout as t:
            if t is not tout:
                raise
            return []
        finally:
            if timeout > 0:
                tout.cancel()
