from __future__ import annotations
import zmq
from zmq.green import Poller


def device(device_type, isocket, osocket):
    p = Poller()
    if osocket == -1:
        osocket = isocket
    p.register(isocket, zmq.POLLIN)
    p.register(osocket, zmq.POLLIN)
    while True:
        events = dict(p.poll())
        if isocket in events:
            osocket.send_multipart(isocket.recv_multipart())
        if osocket in events:
            isocket.send_multipart(osocket.recv_multipart())
