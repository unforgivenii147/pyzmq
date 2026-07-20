"""Classes for running a steerable ZMQ proxy"""

import zmq
from zmq.devices.proxydevice import ProcessProxy, Proxy, ThreadProxy


class ProxySteerableBase:
    def __init__(self, in_type, out_type, mon_type=zmq.PUB, ctrl_type=None):
        super().__init__(in_type=in_type, out_type=out_type, mon_type=mon_type)
        self.ctrl_type = ctrl_type
        self._ctrl_binds = []
        self._ctrl_connects = []
        self._ctrl_sockopts = []

    def bind_ctrl(self, addr):
        self._ctrl_binds.append(addr)

    def bind_ctrl_to_random_port(self, addr, *args, **kwargs):
        port = self._reserve_random_port(addr, *args, **kwargs)
        self.bind_ctrl(f"{addr}:{port}")
        return port

    def connect_ctrl(self, addr):
        self._ctrl_connects.append(addr)

    def setsockopt_ctrl(self, opt, value):
        self._ctrl_sockopts.append((opt, value))

    def _setup_sockets(self):
        ins, outs, mons = super()._setup_sockets()
        ctx = self._context
        ctrls = ctx.socket(self.ctrl_type)
        self._sockets.append(ctrls)
        for opt, value in self._ctrl_sockopts:
            ctrls.setsockopt(opt, value)
        for iface in self._ctrl_binds:
            ctrls.bind(iface)
        for iface in self._ctrl_connects:
            ctrls.connect(iface)
        return (ins, outs, mons, ctrls)

    def run_device(self):
        ins, outs, mons, ctrls = self._setup_sockets()
        zmq.proxy_steerable(ins, outs, mons, ctrls)


class ProxySteerable(ProxySteerableBase, Proxy):
    pass


class ThreadProxySteerable(ProxySteerableBase, ThreadProxy):
    pass


class ProcessProxySteerable(ProxySteerableBase, ProcessProxy):
    pass


__all__ = ["ProxySteerable", "ThreadProxySteerable", "ProcessProxySteerable"]
