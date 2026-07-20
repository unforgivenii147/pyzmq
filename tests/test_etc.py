from pytest import mark
import zmq

only_bundled = mark.skipif(not hasattr(zmq, "_libzmq"), reason="bundled libzmq")


@mark.skipif("zmq.zmq_version_info() < (4, 1)")
def test_has():
    assert not zmq.has("something weird")


@only_bundled
def test_has_curve():
    assert zmq.has("curve")


@only_bundled
def test_has_ipc():
    assert zmq.has("ipc")
