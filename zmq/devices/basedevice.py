"""Classes for running 0MQ Devices in the background."""

import time
from multiprocessing import Process
from threading import Thread
from typing import Any, Callable, List, Optional, Tuple
import zmq
from zmq import ENOTSOCK, ETERM, PUSH, QUEUE, Context, ZMQBindError, ZMQError, proxy


class Device:
    context_factory: Callable[[], zmq.Context] = Context.instance
    "Callable that returns a context. Typically either Context.instance or Context,\n    depending on whether the device should share the global instance or not.\n    "
    daemon: bool
    device_type: int
    in_type: int
    out_type: int
    _in_binds: List[str]
    _in_connects: List[str]
    _in_sockopts: List[Tuple[int, Any]]
    _out_binds: List[str]
    _out_connects: List[str]
    _out_sockopts: List[Tuple[int, Any]]
    _random_addrs: List[str]
    _sockets: List[zmq.Socket]

    def __init__(
        self,
        device_type: int = QUEUE,
        in_type: Optional[int] = None,
        out_type: Optional[int] = None,
    ) -> None:
        self.device_type = device_type
        if in_type is None:
            raise TypeError("in_type must be specified")
        if out_type is None:
            raise TypeError("out_type must be specified")
        self.in_type = in_type
        self.out_type = out_type
        self._in_binds = []
        self._in_connects = []
        self._in_sockopts = []
        self._out_binds = []
        self._out_connects = []
        self._out_sockopts = []
        self._random_addrs = []
        self.daemon = True
        self.done = False
        self._sockets = []

    def bind_in(self, addr: str) -> None:
        self._in_binds.append(addr)

    def bind_in_to_random_port(self, addr: str, *args, **kwargs) -> int:
        port = self._reserve_random_port(addr, *args, **kwargs)
        self.bind_in(f"{addr}:{port}")
        return port

    def connect_in(self, addr: str) -> None:
        self._in_connects.append(addr)

    def setsockopt_in(self, opt: int, value: Any) -> None:
        self._in_sockopts.append((opt, value))

    def bind_out(self, addr: str) -> None:
        self._out_binds.append(addr)

    def bind_out_to_random_port(self, addr: str, *args, **kwargs) -> int:
        port = self._reserve_random_port(addr, *args, **kwargs)
        self.bind_out(f"{addr}:{port}")
        return port

    def connect_out(self, addr: str):
        self._out_connects.append(addr)

    def setsockopt_out(self, opt: int, value: Any):
        self._out_sockopts.append((opt, value))

    def _reserve_random_port(self, addr: str, *args, **kwargs) -> int:
        with Context() as ctx:
            with ctx.socket(PUSH) as binder:
                for i in range(5):
                    port = binder.bind_to_random_port(addr, *args, **kwargs)
                    new_addr = f"{addr}:{port}"
                    if new_addr in self._random_addrs:
                        continue
                    else:
                        break
                else:
                    raise ZMQBindError("Could not reserve random port.")
                self._random_addrs.append(new_addr)
        return port

    def _setup_sockets(self) -> Tuple[zmq.Socket, zmq.Socket]:
        ctx: zmq.Context[zmq.Socket] = self.context_factory()
        self._context = ctx
        ins = ctx.socket(self.in_type)
        self._sockets.append(ins)
        if self.out_type < 0:
            outs = ins
        else:
            outs = ctx.socket(self.out_type)
            self._sockets.append(outs)
        for opt, value in self._in_sockopts:
            ins.setsockopt(opt, value)
        for opt, value in self._out_sockopts:
            outs.setsockopt(opt, value)
        for iface in self._in_binds:
            ins.bind(iface)
        for iface in self._out_binds:
            outs.bind(iface)
        for iface in self._in_connects:
            ins.connect(iface)
        for iface in self._out_connects:
            outs.connect(iface)
        return (ins, outs)

    def run_device(self) -> None:
        ins, outs = self._setup_sockets()
        proxy(ins, outs)

    def _close_sockets(self):
        for s in self._sockets:
            if s and (not s.closed):
                s.close()

    def run(self) -> None:
        try:
            self.run_device()
        except ZMQError as e:
            if e.errno in {ETERM, ENOTSOCK}:
                pass
            else:
                raise
        finally:
            self.done = True
            self._close_sockets()

    def start(self) -> None:
        return self.run()

    def join(self, timeout: Optional[float] = None) -> None:
        tic = time.monotonic()
        toc = tic
        while not self.done and (not (timeout is not None and toc - tic > timeout)):
            time.sleep(0.001)
            toc = time.monotonic()


class BackgroundDevice(Device):
    launcher: Any = None
    _launch_class: Any = None

    def start(self) -> None:
        self.launcher = self._launch_class(target=self.run)
        self.launcher.daemon = self.daemon
        return self.launcher.start()

    def join(self, timeout: Optional[float] = None) -> None:
        return self.launcher.join(timeout=timeout)


class ThreadDevice(BackgroundDevice):
    _launch_class = Thread


class ProcessDevice(BackgroundDevice):
    _launch_class = Process
    context_factory = Context
    "Callable that returns a context. Typically either Context.instance or Context,\n    depending on whether the device should share the global instance or not.\n    "


__all__ = ["Device", "ThreadDevice", "ProcessDevice"]
