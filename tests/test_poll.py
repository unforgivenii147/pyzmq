import os
import sys
import time
from pytest import mark
import zmq
from zmq_test_utils import GreenTest, PollZMQTestCase, have_gevent


def wait():
    time.sleep(0.25)


class TestPoll(PollZMQTestCase):
    Poller = zmq.Poller

    def test_pair(self):
        s1, s2 = self.create_bound_pair(zmq.PAIR, zmq.PAIR)
        wait()
        poller = self.Poller()
        poller.register(s1, zmq.POLLIN | zmq.POLLOUT)
        poller.register(s2, zmq.POLLIN | zmq.POLLOUT)
        socks = dict(poller.poll())
        assert socks[s1] == zmq.POLLOUT
        assert socks[s2] == zmq.POLLOUT
        s1.send(b"msg1")
        s2.send(b"msg2")
        wait()
        socks = dict(poller.poll())
        assert socks[s1] == zmq.POLLOUT | zmq.POLLIN
        assert socks[s2] == zmq.POLLOUT | zmq.POLLIN
        s1.recv()
        s2.recv()
        socks = dict(poller.poll())
        assert socks[s1] == zmq.POLLOUT
        assert socks[s2] == zmq.POLLOUT
        poller.unregister(s1)
        poller.unregister(s2)

    def test_reqrep(self):
        s1, s2 = self.create_bound_pair(zmq.REP, zmq.REQ)
        wait()
        poller = self.Poller()
        poller.register(s1, zmq.POLLIN | zmq.POLLOUT)
        poller.register(s2, zmq.POLLIN | zmq.POLLOUT)
        socks = dict(poller.poll())
        assert s1 not in socks
        assert socks[s2] == zmq.POLLOUT
        s2.send(b"msg1")
        socks = dict(poller.poll())
        assert s2 not in socks
        time.sleep(0.5)
        socks = dict(poller.poll())
        assert socks[s1] == zmq.POLLIN
        s1.recv()
        socks = dict(poller.poll())
        assert socks[s1] == zmq.POLLOUT
        s1.send(b"msg2")
        socks = dict(poller.poll())
        assert s1 not in socks
        time.sleep(0.5)
        socks = dict(poller.poll())
        assert socks[s2] == zmq.POLLIN
        s2.recv()
        socks = dict(poller.poll())
        assert socks[s2] == zmq.POLLOUT
        poller.unregister(s1)
        poller.unregister(s2)

    def test_no_events(self):
        s1, s2 = self.create_bound_pair(zmq.PAIR, zmq.PAIR)
        poller = self.Poller()
        poller.register(s1, zmq.POLLIN | zmq.POLLOUT)
        poller.register(s2, 0)
        assert s1 in poller
        assert s2 not in poller
        poller.register(s1, 0)
        assert s1 not in poller

    def test_pubsub(self):
        s1, s2 = self.create_bound_pair(zmq.PUB, zmq.SUB)
        s2.setsockopt(zmq.SUBSCRIBE, b"")
        wait()
        poller = self.Poller()
        poller.register(s1, zmq.POLLIN | zmq.POLLOUT)
        poller.register(s2, zmq.POLLIN)
        socks = dict(poller.poll())
        assert socks[s1] == zmq.POLLOUT
        assert s2 not in socks
        s1.send(b"msg1")
        socks = dict(poller.poll())
        assert socks[s1] == zmq.POLLOUT
        wait()
        socks = dict(poller.poll())
        assert socks[s2] == zmq.POLLIN
        s2.recv()
        socks = dict(poller.poll())
        assert s2 not in socks
        poller.unregister(s1)
        poller.unregister(s2)

    @mark.skipif(sys.platform.startswith("win"), reason="Windows")
    def test_raw(self):
        r, w = os.pipe()
        r = os.fdopen(r, "rb")
        w = os.fdopen(w, "wb")
        p = self.Poller()
        p.register(r, zmq.POLLIN)
        socks = dict(p.poll(1))
        assert socks == {}
        w.write(b"x")
        w.flush()
        socks = dict(p.poll(1))
        assert socks == {r.fileno(): zmq.POLLIN}
        w.close()
        r.close()

    @mark.flaky(reruns=3)
    def test_timeout(self):
        s1, s2 = self.create_bound_pair(zmq.PAIR, zmq.PAIR)
        poller = self.Poller()
        poller.register(s1, zmq.POLLIN)
        tic = time.perf_counter()
        poller.poll(0.005)
        toc = time.perf_counter()
        toc - tic < 0.5
        tic = time.perf_counter()
        poller.poll(50)
        toc = time.perf_counter()
        assert toc - tic < 0.5
        assert toc - tic > 0.01
        tic = time.perf_counter()
        poller.poll(500)
        toc = time.perf_counter()
        assert toc - tic < 1
        assert toc - tic > 0.1


class TestSelect(PollZMQTestCase):
    def test_pair(self):
        s1, s2 = self.create_bound_pair(zmq.PAIR, zmq.PAIR)
        wait()
        rlist, wlist, xlist = zmq.select([s1, s2], [s1, s2], [s1, s2])
        assert s1 in wlist
        assert s2 in wlist
        assert s1 not in rlist
        assert s2 not in rlist

    @mark.flaky(reruns=3)
    def test_timeout(self):
        s1, s2 = self.create_bound_pair(zmq.PAIR, zmq.PAIR)
        tic = time.perf_counter()
        r, w, x = zmq.select([s1, s2], [], [], 0.005)
        toc = time.perf_counter()
        assert toc - tic < 1
        assert toc - tic > 0.001
        tic = time.perf_counter()
        r, w, x = zmq.select([s1, s2], [], [], 0.25)
        toc = time.perf_counter()
        assert toc - tic < 1
        assert toc - tic > 0.1


if have_gevent:
    import gevent
    from zmq import green as gzmq

    class TestPollGreen(GreenTest, TestPoll):
        Poller = gzmq.Poller

        def test_wakeup(self):
            s1, s2 = self.create_bound_pair(zmq.PAIR, zmq.PAIR)
            poller = self.Poller()
            poller.register(s2, zmq.POLLIN)
            tic = time.perf_counter()
            r = gevent.spawn(lambda: poller.poll(10000))
            s = gevent.spawn(lambda: s1.send(b"msg1"))
            r.join()
            toc = time.perf_counter()
            assert toc - tic < 1

        def test_socket_poll(self):
            s1, s2 = self.create_bound_pair(zmq.PAIR, zmq.PAIR)
            tic = time.perf_counter()
            r = gevent.spawn(lambda: s2.poll(10000))
            s = gevent.spawn(lambda: s1.send(b"msg1"))
            r.join()
            toc = time.perf_counter()
            assert toc - tic < 1
