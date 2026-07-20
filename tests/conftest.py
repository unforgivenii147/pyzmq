"""pytest configuration and fixtures"""

import asyncio
import inspect
import os
import signal
import time
from functools import partial
from threading import Thread

try:
    import tornado
    from tornado import version_info
except ImportError:
    tornado = None
else:
    if version_info < (5,):
        tornado = None
    from tornado.ioloop import IOLoop
import pytest
import zmq
import zmq.asyncio

test_timeout_seconds = os.environ.get("ZMQ_TEST_TIMEOUT")
teardown_timeout = 10


def pytest_collection_modifyitems(items):
    for item in items:
        if inspect.iscoroutinefunction(item.obj):
            item.add_marker("asyncio")
        assert not inspect.isasyncgenfunction(item.obj)


@pytest.fixture
async def io_loop(request):
    if tornado is None:
        pytest.skip()
    io_loop = IOLoop.current()
    assert io_loop.asyncio_loop is asyncio.get_running_loop()

    def _close():
        io_loop.close(all_fds=True)

    request.addfinalizer(_close)
    return io_loop


def term_context(ctx, timeout):
    t = Thread(target=ctx.term)
    t.daemon = True
    t.start()
    t.join(timeout=timeout)
    if t.is_alive():
        zmq.sugar.context.Context._instance = None
        raise RuntimeError(
            f"context {ctx} could not terminate, open sockets likely remain in test"
        )


@pytest.fixture
def sigalrm_timeout():
    if not hasattr(signal, "SIGALRM") or not test_timeout_seconds:
        return

    def _alarm_timeout(*args):
        raise TimeoutError(f"Test did not complete in {test_timeout_seconds} seconds")

    signal.signal(signal.SIGALRM, _alarm_timeout)
    signal.alarm(test_timeout_seconds)


@pytest.fixture
def Context():
    return zmq.Context


@pytest.fixture
def contexts(sigalrm_timeout):
    contexts = set()
    yield contexts
    for ctx in contexts:
        try:
            term_context(ctx, teardown_timeout)
        except Exception:
            zmq.sugar.context.Context._instance = None
            raise


@pytest.fixture
def context(Context, contexts):
    ctx = Context()
    contexts.add(ctx)
    return ctx


@pytest.fixture
def sockets(contexts):
    sockets = []
    yield sockets
    for socket in sockets:
        contexts.add(socket.context)
    for socket in sockets:
        socket.close(linger=0)


@pytest.fixture
def socket(context, sockets):

    def new_socket(*args, **kwargs):
        s = context.socket(*args, **kwargs)
        sockets.append(s)
        return s

    return new_socket


def assert_raises_errno(errno):
    try:
        yield
    except zmq.ZMQError as e:
        assert e.errno == errno, (
            f"wrong error raised, expected {zmq.ZMQError(errno)} got {zmq.ZMQError(e.errno)}"
        )
    else:
        pytest.fail(f"Expected {zmq.ZMQError(errno)}, no error raised")


def recv(socket, *, timeout=5, flags=0, multipart=False, **kwargs):
    if zmq.zmq_version_info() >= (3, 1, 0):
        time.sleep(0.1)
    r, w, x = zmq.select([socket], [], [], timeout=timeout)
    assert r, "Should have received a message"
    kwargs["flags"] = zmq.DONTWAIT | kwargs.get("flags", 0)
    recv = socket.recv_multipart if multipart else socket.recv
    return recv(flags=flags, **kwargs)


recv_multipart = partial(recv, multipart=True)


@pytest.fixture
def create_bound_pair(socket):

    def create_bound_pair(type1=zmq.PAIR, type2=zmq.PAIR, interface="tcp://127.0.0.1"):
        s1 = socket(type1)
        s1.linger = 0
        port = s1.bind_to_random_port(interface)
        s2 = socket(type2)
        s2.linger = 0
        s2.connect(f"{interface}:{port}")
        return (s1, s2)

    return create_bound_pair


@pytest.fixture
def bound_pair(create_bound_pair):
    return create_bound_pair()


@pytest.fixture
def push_pull(create_bound_pair):
    return create_bound_pair(zmq.PUSH, zmq.PULL)


@pytest.fixture
def dealer_router(create_bound_pair):
    return create_bound_pair(zmq.DEALER, zmq.ROUTER)
