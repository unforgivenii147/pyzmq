"""tornado IOLoop API with zmq compatibility

If you have tornado ≥ 3.0, this is a subclass of tornado's IOLoop,
otherwise we ship a minimal subset of tornado in zmq.eventloop.minitornado.

The minimal shipped version of tornado's IOLoop does not include
support for concurrent futures - this will only be available if you
have tornado ≥ 3.0.
"""

import time
import warnings
from typing import Tuple
from zmq import ETERM, POLLERR, POLLIN, POLLOUT, Poller, ZMQError

tornado_version: Tuple = ()
try:
    import tornado

    tornado_version = tornado.version_info
except (ImportError, AttributeError):
    pass
from .minitornado.ioloop import PeriodicCallback, PollIOLoop
from .minitornado.log import gen_log


class DelayedCallback(PeriodicCallback):
    def __init__(self, callback, callback_time, io_loop=None):
        warnings.warn(
            "DelayedCallback is deprecated.\n        Use loop.add_timeout instead.",
            DeprecationWarning,
        )
        callback_time = max(callback_time, 0.001)
        super().__init__(callback, callback_time, io_loop)

    def start(self):
        self._running = True
        self._firstrun = True
        self._next_timeout = time.time() + self.callback_time / 1000.0
        self.io_loop.add_timeout(self._next_timeout, self._run)

    def _run(self):
        if not self._running:
            return
        self._running = False
        try:
            self.callback()
        except Exception:
            gen_log.error("Error in delayed callback", exc_info=True)


class ZMQPoller:
    def __init__(self):
        self._poller = Poller()

    @staticmethod
    def _map_events(events):
        z_events = 0
        if events & IOLoop.READ:
            z_events |= POLLIN
        if events & IOLoop.WRITE:
            z_events |= POLLOUT
        if events & IOLoop.ERROR:
            z_events |= POLLERR
        return z_events

    @staticmethod
    def _remap_events(z_events):
        events = 0
        if z_events & POLLIN:
            events |= IOLoop.READ
        if z_events & POLLOUT:
            events |= IOLoop.WRITE
        if z_events & POLLERR:
            events |= IOLoop.ERROR
        return events

    def register(self, fd, events):
        return self._poller.register(fd, self._map_events(events))

    def modify(self, fd, events):
        return self._poller.modify(fd, self._map_events(events))

    def unregister(self, fd):
        return self._poller.unregister(fd)

    def poll(self, timeout):
        z_events = self._poller.poll(1000 * timeout)
        return [(fd, self._remap_events(evt)) for fd, evt in z_events]

    def close(self):
        pass


class ZMQIOLoop(PollIOLoop):
    _zmq_impl = ZMQPoller

    def initialize(self, impl=None, **kwargs):
        impl = self._zmq_impl() if impl is None else impl
        super().initialize(impl=impl, **kwargs)

    @classmethod
    def instance(cls, *args, **kwargs):
        if tornado_version >= (3,):
            PollIOLoop.configure(cls)
        loop = PollIOLoop.instance(*args, **kwargs)
        if not isinstance(loop, cls):
            warnings.warn(
                f"IOLoop.current expected instance of {cls!r}, got {loop!r}",
                RuntimeWarning,
                stacklevel=2,
            )
        return loop

    @classmethod
    def current(cls, *args, **kwargs):
        if tornado_version >= (3,):
            PollIOLoop.configure(cls)
        loop = PollIOLoop.current(*args, **kwargs)
        if not isinstance(loop, cls):
            warnings.warn(
                f"IOLoop.current expected instance of {cls!r}, got {loop!r}",
                RuntimeWarning,
                stacklevel=2,
            )
        return loop

    def start(self):
        try:
            super().start()
        except ZMQError as e:
            if e.errno == ETERM:
                pass
            else:
                raise


IOLoop = ZMQIOLoop


def install():
    from tornado import ioloop

    assert (
        not ioloop.IOLoop.initialized() or ioloop.IOLoop.instance() is IOLoop.instance()
    ), "tornado IOLoop already initialized"
    if tornado_version >= (3,):
        ioloop.IOLoop.configure(ZMQIOLoop)
    else:
        ioloop.IOLoop._instance = IOLoop.instance()
