import pytest

tornado = pytest.importorskip("tornado")


def test_ioloop():
    from zmq.eventloop import ioloop

    assert ioloop.IOLoop is tornado.ioloop.IOLoop
    assert ioloop.ZMQIOLoop is ioloop.IOLoop


def test_ioloop_install():
    from zmq.eventloop import ioloop

    with pytest.warns(DeprecationWarning):
        ioloop.install()
