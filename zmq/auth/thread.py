"""ZAP Authenticator in a Python Thread.

.. versionadded:: 14.1
"""

import asyncio
from threading import Event, Thread
from typing import Any, List, Optional
import zmq
import zmq.asyncio
from .base import Authenticator


class AuthenticationThread(Thread):
    pipe: zmq.Socket
    loop: asyncio.AbstractEventLoop
    authenticator: Authenticator
    poller: Optional[zmq.asyncio.Poller] = None

    def __init__(self, authenticator: Authenticator, pipe: zmq.Socket) -> None:
        super().__init__(daemon=True)
        self.authenticator = authenticator
        self.log = authenticator.log
        self.pipe = pipe
        self.started = Event()

    def run(self) -> None:
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self._run())
        finally:
            if self.pipe:
                self.pipe.close()
                self.pipe = None
            loop.close()

    async def _run(self):
        self.poller = zmq.asyncio.Poller()
        self.poller.register(self.pipe, zmq.POLLIN)
        self.poller.register(self.authenticator.zap_socket, zmq.POLLIN)
        self.started.set()
        while True:
            events = dict(await self.poller.poll())
            if self.pipe in events:
                msg = self.pipe.recv_multipart()
                if self._handle_pipe_message(msg):
                    return
            if self.authenticator.zap_socket in events:
                msg = self.authenticator.zap_socket.recv_multipart()
                await self.authenticator.handle_zap_message(msg)

    def _handle_pipe_message(self, msg: List[bytes]) -> bool:
        command = msg[0]
        self.log.debug("auth received API command %r", command)
        if command == b"TERMINATE":
            return True
        else:
            self.log.error("Invalid auth command from API: %r", command)
            self.pipe.send(b"ERROR")
        return False


class ThreadAuthenticator(Authenticator):
    pipe: "zmq.Socket"
    pipe_endpoint: str = ""
    thread: AuthenticationThread

    def __init__(
        self,
        context: Optional["zmq.Context"] = None,
        encoding: str = "utf-8",
        log: Any = None,
    ):
        super().__init__(context=context, encoding=encoding, log=log)
        self.pipe = None
        self.pipe_endpoint = f"inproc://{id(self)}.inproc"
        self.thread = None

    def start(self) -> None:
        super().start()
        self.pipe = self.context.socket(zmq.PAIR, socket_class=zmq.Socket)
        self.pipe.linger = 1
        self.pipe.bind(self.pipe_endpoint)
        thread_pipe = self.context.socket(zmq.PAIR, socket_class=zmq.Socket)
        thread_pipe.linger = 1
        thread_pipe.connect(self.pipe_endpoint)
        self.thread = AuthenticationThread(authenticator=self, pipe=thread_pipe)
        self.thread.start()
        if not self.thread.started.wait(timeout=10):
            raise RuntimeError("Authenticator thread failed to start")

    def stop(self) -> None:
        if self.pipe:
            self.pipe.send(b"TERMINATE")
            if self.is_alive():
                self.thread.join()
            self.thread = None
            self.pipe.close()
            self.pipe = None
        super().stop()

    def is_alive(self) -> bool:
        return bool(self.thread and self.thread.is_alive())

    def __del__(self) -> None:
        self.stop()


__all__ = ["ThreadAuthenticator"]
