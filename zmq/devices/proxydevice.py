"""Proxy classes and functions."""

import zmq
from zmq.devices.basedevice import Device, ProcessDevice, ThreadDevice


class ProxyBase:
    def __init__(self, in_type, out_type, mon_type=zmq.PUB):
        Device.__init__(self, in_type=in_type, out_type=out_type)
        self.mon_type = mon_type
        self._mon_binds = []
        self._mon_connects = []
        self._mon_sockopts = []

    def bind_mon(self, addr):
        self._mon_binds.append(addr)

    def bind_mon_to_random_port(self, addr, *args, **kwargs):
        port = self._reserve_random_port(addr, *args, **kwargs)
        self.bind_mon(f"{addr}:{port}")
        return port

    def connect_mon(self, addr):
        self._mon_connects.append(addr)

    def setsockopt_mon(self, opt, value):
        self._mon_sockopts.append((opt, value))

    def _setup_sockets(self):
        ins, outs = Device._setup_sockets(self)
        ctx = self._context
        mons = ctx.socket(self.mon_type)
        self._sockets.append(mons)
        for opt, value in self._mon_sockopts:
            mons.setsockopt(opt, value)
        for iface in self._mon_binds:
            mons.bind(iface)
        for iface in self._mon_connects:
            mons.connect(iface)
        return (ins, outs, mons)

    def run_device(self):
        ins, outs, mons = self._setup_sockets()
        zmq.proxy(ins, outs, mons)


class Proxy(ProxyBase, Device):
    pass


class ThreadProxy(ProxyBase, ThreadDevice):
    pass


class ProcessProxy(ProxyBase, ProcessDevice):
    pass


__all__ = ["Proxy", "ThreadProxy", "ProcessProxy"]
