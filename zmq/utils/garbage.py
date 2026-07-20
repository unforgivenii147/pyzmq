"""Garbage collection thread for representing zmq refcount of Python objects
used in zero-copy sends.
"""

import atexit
import struct
import warnings
from collections import namedtuple
from os import getpid
from threading import Event, Lock, Thread
import zmq

gcref = namedtuple("gcref", ["obj", "event"])


class GarbageCollectorThread(Thread):
    def __init__(self, gc):
        super().__init__()
        self.gc = gc
        self.daemon = True
        self.pid = getpid()
        self.ready = Event()

    def run(self):
        if getpid is None or getpid() != self.pid:
            self.ready.set()
            return
        try:
            s = self.gc.context.socket(zmq.PULL)
            s.linger = 0
            s.bind(self.gc.url)
        finally:
            self.ready.set()
        while True:
            if getpid is None or getpid() != self.pid:
                return
            msg = s.recv()
            if msg == b"DIE":
                break
            fmt = "L" if len(msg) == 4 else "Q"
            key = struct.unpack(fmt, msg)[0]
            tup = self.gc.refs.pop(key, None)
            if tup and tup.event:
                tup.event.set()
            del tup
        s.close()


class GarbageCollector:
    refs = None
    _context = None
    _lock = None
    url = "inproc://pyzmq.gc.01"

    def __init__(self, context=None):
        super().__init__()
        self.refs = {}
        self.pid = None
        self.thread = None
        self._context = context
        self._lock = Lock()
        self._stay_down = False
        self._push = None
        self._push_mutex = None
        atexit.register(self._atexit)

    @property
    def context(self):
        if self._context is None:
            if Thread.__module__.startswith("gevent"):
                from zmq import green

                self._context = green.Context()
            else:
                self._context = zmq.Context()
        return self._context

    @context.setter
    def context(self, ctx):
        if self.is_alive():
            if self.refs:
                warnings.warn(
                    "Replacing gc context while gc is running", RuntimeWarning
                )
            self.stop()
        self._context = ctx

    def _atexit(self):
        self._stay_down = True
        self.stop()

    def stop(self):
        if not self.is_alive():
            return
        self._stop()

    def _clear(self):
        self._push = None
        self._push_mutex = None
        self.thread = None
        self.refs.clear()
        self.context = None

    def _stop(self):
        push = self.context.socket(zmq.PUSH)
        push.connect(self.url)
        push.send(b"DIE")
        push.close()
        if self._push:
            self._push.close()
        self.thread.join()
        self.context.term()
        self._clear()

    @property
    def _push_socket(self):
        if getattr(self, "_stay_down", False):
            raise RuntimeError("zmq gc socket requested during shutdown")
        if not self.is_alive() or self._push is None:
            self._push = self.context.socket(zmq.PUSH)
            self._push.connect(self.url)
        return self._push

    def start(self):
        if self.thread is not None and self.pid != getpid():
            self._clear()
        self.pid = getpid()
        self.refs = {}
        self.thread = GarbageCollectorThread(self)
        self.thread.start()
        self.thread.ready.wait()

    def is_alive(self):
        if (
            getpid is None
            or getpid() != self.pid
            or self.thread is None
            or (not self.thread.is_alive())
        ):
            return False
        return True

    def store(self, obj, event=None):
        if not self.is_alive():
            if self._stay_down:
                return 0
            with self._lock:
                if not self.is_alive():
                    self.start()
        tup = gcref(obj, event)
        theid = id(tup)
        self.refs[theid] = tup
        return theid

    def __del__(self):
        if not self.is_alive():
            return
        try:
            self.stop()
        except Exception as e:
            raise e


gc = GarbageCollector()
