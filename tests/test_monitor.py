import pytest
import zmq
import zmq.asyncio
from zmq.utils.monitor import recv_monitor_message
from zmq_test_utils import require_zmq_4

pytestmark = require_zmq_4


@pytest.fixture(params=["zmq", "asyncio"])
async def Context(request):
    if request.param == "asyncio":
        return zmq.asyncio.Context
    else:
        return zmq.Context


async def test_monitor(context, socket):
    s_rep = socket(zmq.REP)
    s_req = socket(zmq.REQ)
    s_req.bind("tcp://127.0.0.1:6666")
    s_rep.monitor(
        "inproc://monitor.rep",
        zmq.EVENT_CONNECT_DELAYED | zmq.EVENT_CONNECTED | zmq.EVENT_MONITOR_STOPPED,
    )
    s_event = socket(zmq.PAIR)
    s_event.connect("inproc://monitor.rep")
    s_event.linger = 0
    s_rep.connect("tcp://127.0.0.1:6666")
    m = recv_monitor_message(s_event)
    if isinstance(context, zmq.asyncio.Context):
        m = await m
    if m["event"] == zmq.EVENT_CONNECT_DELAYED:
        assert m["endpoint"] == b"tcp://127.0.0.1:6666"
        m = recv_monitor_message(s_event)
        if isinstance(context, zmq.asyncio.Context):
            m = await m
    assert m["event"] == zmq.EVENT_CONNECTED
    assert m["endpoint"] == b"tcp://127.0.0.1:6666"
    s_rep.disable_monitor()
    m = recv_monitor_message(s_event)
    if isinstance(context, zmq.asyncio.Context):
        m = await m
    assert m["event"] == zmq.EVENT_MONITOR_STOPPED


async def test_monitor_repeat(context, socket, sockets):
    s = socket(zmq.PULL)
    m = s.get_monitor_socket()
    sockets.append(m)
    m2 = s.get_monitor_socket()
    assert m is m2
    s.disable_monitor()
    evt = recv_monitor_message(m)
    if isinstance(context, zmq.asyncio.Context):
        evt = await evt
    assert evt["event"] == zmq.EVENT_MONITOR_STOPPED
    m.close()
    s.close()


async def test_monitor_connected(context, socket, sockets):
    s_rep = socket(zmq.REP)
    s_req = socket(zmq.REQ)
    s_req.bind("tcp://127.0.0.1:6667")
    s_event = s_rep.get_monitor_socket()
    s_event.linger = 0
    sockets.append(s_event)
    s_rep.connect("tcp://127.0.0.1:6667")
    m = recv_monitor_message(s_event)
    if isinstance(context, zmq.asyncio.Context):
        m = await m
    if m["event"] == zmq.EVENT_CONNECT_DELAYED:
        assert m["endpoint"] == b"tcp://127.0.0.1:6667"
        m = recv_monitor_message(s_event)
        if isinstance(context, zmq.asyncio.Context):
            m = await m
    assert m["event"] == zmq.EVENT_CONNECTED
    assert m["endpoint"] == b"tcp://127.0.0.1:6667"
