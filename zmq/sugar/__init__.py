"""pure-Python sugar wrappers for core 0MQ objects."""

from __future__ import annotations
from zmq import error
from zmq.backend import proxy
from zmq.constants import DeviceType
from zmq.sugar import context, frame, poll, socket, tracker, version


def device(device_type: DeviceType, frontend: socket.Socket, backend: socket.Socket):
    return proxy(frontend, backend)


__all__ = ["device"]
for submod in (context, error, frame, poll, socket, tracker, version):
    __all__.extend(submod.__all__)
from zmq.error import *
from zmq.sugar.context import *
from zmq.sugar.frame import *
from zmq.sugar.poll import *
from zmq.sugar.socket import *
from zmq.sugar.stopwatch import Stopwatch
from zmq.sugar.tracker import *
from zmq.sugar.version import *

__all__.append("Stopwatch")
