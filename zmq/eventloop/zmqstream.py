"""A utility class for event-based messaging on a zmq socket using tornado.

.. seealso::

    - :mod:`zmq.asyncio`
    - :mod:`zmq.eventloop.future`
"""

from __future__ import annotations
import asyncio
import pickle
import warnings
from queue import Queue
from typing import Any, Awaitable, Callable, Literal, Sequence, cast, overload
from tornado.ioloop import IOLoop
from tornado.log import gen_log
import zmq
import zmq._future
from zmq import POLLIN, POLLOUT
from zmq.utils import jsonapi


class ZMQStream:
    socket: zmq.Socket
    io_loop: IOLoop
    poller: zmq.Poller
    _send_queue: Queue
    _recv_callback: Callable | None
    _send_callback: Callable | None
    _close_callback: Callable | None
    _state: int = 0
    _flushed: bool = False
    _recv_copy: bool = False
    _fd: int

    def __init__(self, socket: zmq.Socket, io_loop: IOLoop | None = None):
        if isinstance(socket, zmq._future._AsyncSocket):
            warnings.warn(
                f"ZMQStream only supports the base zmq.Socket class.\n\n                Use zmq.Socket(shadow=other_socket)\n                or `ctx.socket(zmq.{socket._type_name}, socket_class=zmq.Socket)`\n                to create a base zmq.Socket object,\n                no matter what other kind of socket your Context creates.\n                ",
                RuntimeWarning,
                stacklevel=2,
            )
            socket = zmq.Socket(shadow=socket)
        self.socket = socket
        self.io_loop = io_loop or IOLoop.current()
        self.poller = zmq.Poller()
        self._fd = cast(int, self.socket.FD)
        self._send_queue = Queue()
        self._recv_callback = None
        self._send_callback = None
        self._close_callback = None
        self._recv_copy = False
        self._flushed = False
        self._state = 0
        self._init_io_state()
        self.bind = self.socket.bind
        self.bind_to_random_port = self.socket.bind_to_random_port
        self.connect = self.socket.connect
        self.setsockopt = self.socket.setsockopt
        self.getsockopt = self.socket.getsockopt
        self.setsockopt_string = self.socket.setsockopt_string
        self.getsockopt_string = self.socket.getsockopt_string
        self.setsockopt_unicode = self.socket.setsockopt_unicode
        self.getsockopt_unicode = self.socket.getsockopt_unicode

    def stop_on_recv(self):
        return self.on_recv(None)

    def stop_on_send(self):
        return self.on_send(None)

    def stop_on_err(self):
        gen_log.warn("on_err does nothing, and will be removed")

    def on_err(self, callback: Callable):
        gen_log.warn("on_err does nothing, and will be removed")

    @overload
    def on_recv(self, callback: Callable[[list[bytes]], Any]) -> None: ...

    @overload
    def on_recv(
        self, callback: Callable[[list[bytes]], Any], copy: Literal[True]
    ) -> None: ...

    @overload
    def on_recv(
        self, callback: Callable[[list[zmq.Frame]], Any], copy: Literal[False]
    ) -> None: ...

    @overload
    def on_recv(
        self,
        callback: Callable[[list[zmq.Frame]], Any] | Callable[[list[bytes]], Any],
        copy: bool = ...,
    ): ...

    def on_recv(
        self,
        callback: Callable[[list[zmq.Frame]], Any] | Callable[[list[bytes]], Any],
        copy: bool = True,
    ) -> None:
        self._check_closed()
        assert callback is None or callable(callback)
        self._recv_callback = callback
        self._recv_copy = copy
        if callback is None:
            self._drop_io_state(zmq.POLLIN)
        else:
            self._add_io_state(zmq.POLLIN)

    @overload
    def on_recv_stream(
        self, callback: Callable[[ZMQStream, list[bytes]], Any]
    ) -> None: ...

    @overload
    def on_recv_stream(
        self, callback: Callable[[ZMQStream, list[bytes]], Any], copy: Literal[True]
    ) -> None: ...

    @overload
    def on_recv_stream(
        self,
        callback: Callable[[ZMQStream, list[zmq.Frame]], Any],
        copy: Literal[False],
    ) -> None: ...

    @overload
    def on_recv_stream(
        self,
        callback: Callable[[ZMQStream, list[zmq.Frame]], Any]
        | Callable[[ZMQStream, list[bytes]], Any],
        copy: bool = ...,
    ): ...

    def on_recv_stream(
        self,
        callback: Callable[[ZMQStream, list[zmq.Frame]], Any]
        | Callable[[ZMQStream, list[bytes]], Any],
        copy: bool = True,
    ):
        if callback is None:
            self.stop_on_recv()
        else:

            def stream_callback(msg):
                return callback(self, msg)

            self.on_recv(stream_callback, copy=copy)

    def on_send(
        self, callback: Callable[[Sequence[Any], zmq.MessageTracker | None], Any]
    ):
        self._check_closed()
        assert callback is None or callable(callback)
        self._send_callback = callback

    def on_send_stream(
        self,
        callback: Callable[[ZMQStream, Sequence[Any], zmq.MessageTracker | None], Any],
    ):
        if callback is None:
            self.stop_on_send()
        else:
            self.on_send(lambda msg, status: callback(self, msg, status))

    def send(self, msg, flags=0, copy=True, track=False, callback=None, **kwargs):
        return self.send_multipart(
            [msg], flags=flags, copy=copy, track=track, callback=callback, **kwargs
        )

    def send_multipart(
        self,
        msg: Sequence[Any],
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
        callback: Callable | None = None,
        **kwargs: Any,
    ) -> None:
        kwargs.update(dict(flags=flags, copy=copy, track=track))
        self._send_queue.put((msg, kwargs))
        callback = callback or self._send_callback
        if callback is not None:
            self.on_send(callback)
        else:
            self.on_send(lambda *args: None)
        self._add_io_state(zmq.POLLOUT)

    def send_string(
        self,
        u: str,
        flags: int = 0,
        encoding: str = "utf-8",
        callback: Callable | None = None,
        **kwargs: Any,
    ):
        if not isinstance(u, str):
            raise TypeError("unicode/str objects only")
        return self.send(u.encode(encoding), flags=flags, callback=callback, **kwargs)

    send_unicode = send_string

    def send_json(
        self, obj: Any, flags: int = 0, callback: Callable | None = None, **kwargs: Any
    ):
        msg = jsonapi.dumps(obj)
        return self.send(msg, flags=flags, callback=callback, **kwargs)

    def send_pyobj(
        self,
        obj: Any,
        flags: int = 0,
        protocol: int = -1,
        callback: Callable | None = None,
        **kwargs: Any,
    ):
        msg = pickle.dumps(obj, protocol)
        return self.send(msg, flags, callback=callback, **kwargs)

    def _finish_flush(self):
        self._flushed = False

    def flush(self, flag: int = zmq.POLLIN | zmq.POLLOUT, limit: int | None = None):
        self._check_closed()
        already_flushed = self._flushed
        self._flushed = False
        count = 0

        def update_flag():
            return flag & zmq.POLLIN | (self.sending() and flag & zmq.POLLOUT)

        flag = update_flag()
        if not flag:
            return 0
        self.poller.register(self.socket, flag)
        events = self.poller.poll(0)
        while events and (not limit or count < limit):
            s, event = events[0]
            if event & POLLIN:
                self._handle_recv()
                count += 1
                if self.socket is None:
                    break
            if event & POLLOUT and self.sending():
                self._handle_send()
                count += 1
                if self.socket is None:
                    break
            flag = update_flag()
            if flag:
                self.poller.register(self.socket, flag)
                events = self.poller.poll(0)
            else:
                events = []
        if count:
            self._flushed = True
            if not already_flushed:
                self.io_loop.add_callback(self._finish_flush)
        elif already_flushed:
            self._flushed = True
        self._rebuild_io_state()
        return count

    def set_close_callback(self, callback: Callable | None):
        self._close_callback = callback

    def close(self, linger: int | None = None) -> None:
        if self.socket is not None:
            if self.socket.closed:
                warnings.warn(
                    f"Unregistering FD {self._fd} after closing socket. This could result in unregistering handlers for the wrong socket. Please use stream.close() instead of closing the socket directly.",
                    stacklevel=2,
                )
                self.io_loop.remove_handler(self._fd)
            else:
                self.io_loop.remove_handler(self.socket)
                self.socket.close(linger)
            self.socket = None
            if self._close_callback:
                self._run_callback(self._close_callback)

    def receiving(self) -> bool:
        return self._recv_callback is not None

    def sending(self) -> bool:
        return not self._send_queue.empty()

    def closed(self) -> bool:
        if self.socket is None:
            return True
        if self.socket.closed:
            self.close()
            return True
        return False

    def _run_callback(self, callback, *args, **kwargs):
        try:
            f = callback(*args, **kwargs)
            if isinstance(f, Awaitable):
                f = asyncio.ensure_future(f)
            else:
                f = None
        except Exception:
            gen_log.error("Uncaught exception in ZMQStream callback", exc_info=True)
            raise
        if f is not None:

            def _log_error(f):
                try:
                    f.result()
                except Exception:
                    gen_log.error(
                        "Uncaught exception in ZMQStream callback", exc_info=True
                    )

            f.add_done_callback(_log_error)

    def _handle_events(self, fd, events):
        if not self.socket:
            gen_log.warning("Got events for closed stream %s", self)
            return
        try:
            zmq_events = self.socket.EVENTS
        except zmq.ContextTerminated:
            gen_log.warning("Got events for stream %s after terminating context", self)
            self.closed()
            return
        except zmq.ZMQError as e:
            if self.closed():
                gen_log.warning(
                    "Got events for stream %s attached to closed socket: %s", self, e
                )
            else:
                gen_log.error("Error getting events for %s: %s", self, e)
            return
        try:
            if zmq_events & zmq.POLLIN and self.receiving():
                self._handle_recv()
                if not self.socket:
                    return
            if zmq_events & zmq.POLLOUT and self.sending():
                self._handle_send()
                if not self.socket:
                    return
            self._rebuild_io_state()
        except Exception:
            gen_log.error("Uncaught exception in zmqstream callback", exc_info=True)
            raise

    def _handle_recv(self):
        if self._flushed:
            return
        try:
            msg = self.socket.recv_multipart(zmq.NOBLOCK, copy=self._recv_copy)
        except zmq.ZMQError as e:
            if e.errno == zmq.EAGAIN:
                pass
            else:
                raise
        else:
            if self._recv_callback:
                callback = self._recv_callback
                self._run_callback(callback, msg)

    def _handle_send(self):
        if self._flushed:
            return
        if not self.sending():
            gen_log.error("Shouldn't have handled a send event")
            return
        msg, kwargs = self._send_queue.get()
        try:
            status = self.socket.send_multipart(msg, **kwargs)
        except zmq.ZMQError as e:
            gen_log.error("SEND Error: %s", e)
            status = e
        if self._send_callback:
            callback = self._send_callback
            self._run_callback(callback, msg, status)

    def _check_closed(self):
        if not self.socket:
            raise OSError("Stream is closed")

    def _rebuild_io_state(self):
        if self.socket is None:
            return
        state = 0
        if self.receiving():
            state |= zmq.POLLIN
        if self.sending():
            state |= zmq.POLLOUT
        self._state = state
        self._update_handler(state)

    def _add_io_state(self, state):
        self._state = self._state | state
        self._update_handler(self._state)

    def _drop_io_state(self, state):
        self._state = self._state & ~state
        self._update_handler(self._state)

    def _update_handler(self, state):
        if self.socket is None:
            return
        if state & self.socket.events:
            self.io_loop.add_callback(lambda: self._handle_events(self.socket, 0))

    def _init_io_state(self):
        self.io_loop.add_handler(self.socket, self._handle_events, self.io_loop.READ)
