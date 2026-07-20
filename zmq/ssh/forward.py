"""
Sample script showing how to do local port forwarding over paramiko.

This script connects to the requested SSH server and sets up local port
forwarding (the openssh -L option) from a local port through a tunneled
connection to a destination reachable from the SSH server machine.
"""

import logging
import select
import socketserver

logger = logging.getLogger("ssh")


class ForwardServer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            chan = self.ssh_transport.open_channel(
                "direct-tcpip",
                (self.chain_host, self.chain_port),
                self.request.getpeername(),
            )
        except Exception as e:
            logger.debug(
                "Incoming request to %s:%d failed: %r",
                self.chain_host,
                self.chain_port,
                e,
            )
            return
        if chan is None:
            logger.debug(
                "Incoming request to %s:%d was rejected by the SSH server.",
                self.chain_host,
                self.chain_port,
            )
            return
        logger.debug(
            f"Connected!  Tunnel open {self.request.getpeername()!r} -> {chan.getpeername()!r} -> {(self.chain_host, self.chain_port)!r}"
        )
        while True:
            r, w, x = select.select([self.request, chan], [], [])
            if self.request in r:
                data = self.request.recv(1024)
                if len(data) == 0:
                    break
                chan.send(data)
            if chan in r:
                data = chan.recv(1024)
                if len(data) == 0:
                    break
                self.request.send(data)
        chan.close()
        self.request.close()
        logger.debug("Tunnel closed ")


def forward_tunnel(local_port, remote_host, remote_port, transport):

    class SubHander(Handler):
        chain_host = remote_host
        chain_port = remote_port
        ssh_transport = transport

    ForwardServer(("127.0.0.1", local_port), SubHander).serve_forever()


__all__ = ["forward_tunnel"]
