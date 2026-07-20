"""zmq.green - gevent compatibility with zeromq.

Usage
-----

Instead of importing zmq directly, do so in the following manner:

..

    import zmq.green as zmq


Any calls that would have blocked the current thread will now only block the
current green thread.

This compatibility is accomplished by ensuring the nonblocking flag is set
before any blocking operation and the ØMQ file descriptor is polled internally
to trigger needed events.
"""

from __future__ import annotations
from typing import List
import zmq as _zmq
from zmq import *
from zmq.green.core import _Context, _Socket
from zmq.green.poll import _Poller

Context = _Context
Socket = _Socket
Poller = _Poller
from zmq.green.device import device

__all__: list[str] = []
__all__.extend(_zmq.__all__)
