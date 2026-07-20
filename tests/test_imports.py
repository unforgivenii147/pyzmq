"""
Test Imports - the quickest test to ensure that we haven't
introduced version-incompatible syntax errors.
"""

import pytest


def test_toplevel():
    import zmq


def test_core():
    from zmq import (
        Context,
        Frame,
        Poller,
        Socket,
        constants,
        device,
        proxy,
        pyzmq_version,
        pyzmq_version_info,
        zmq_version,
        zmq_version_info,
    )


def test_devices():
    import zmq.devices
    from zmq.devices import basedevice, monitoredqueue, monitoredqueuedevice


def test_log():
    import zmq.log
    from zmq.log import handlers


def test_eventloop():
    pytest.importorskip("tornado")
    import zmq.eventloop
    from zmq.eventloop import ioloop, zmqstream


def test_utils():
    import zmq.utils
    from zmq.utils import jsonapi, strtypes


def test_ssh():
    from zmq.ssh import tunnel


def test_decorators():
    from zmq.decorators import context, socket


def test_zmq_all():
    import zmq

    for name in zmq.__all__:
        assert hasattr(zmq, name)


@pytest.mark.parametrize("pkgname", ["zmq", "zmq.green"])
@pytest.mark.parametrize(
    "attr",
    [
        "RCVTIMEO",
        "PUSH",
        "zmq_version_info",
        "SocketOption",
        "device",
        "Socket",
        "Context",
    ],
)
def test_all_exports(pkgname, attr):
    import zmq

    subpkg = pytest.importorskip(pkgname)
    for name in zmq.__all__:
        assert hasattr(subpkg, name)
    assert attr in subpkg.__all__
    if attr not in ("Socket", "Context", "device"):
        assert getattr(subpkg, attr) is getattr(zmq, attr)
