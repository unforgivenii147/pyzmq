#!/usr/bin/env python


import argparse
import time
from multiprocessing import Process

try:
    now = time.monotonic
except AttributeError:
    now = time.time
import zmq

zmq.COPY_THRESHOLD = 0


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run a zmq performance test")
    parser.add_argument(
        "-p",
        "--poll",
        action="store_true",
        help="use a zmq Poller instead of raw send/recv",
    )
    parser.add_argument(
        "--no-copy",
        action="store_false",
        dest="copy",
        help="enable zero-copy transfer (potentially faster for large messages)",
    )
    parser.add_argument(
        "-s",
        "--size",
        type=int,
        default=1024,
        help="size (in bytes) of the test message",
    )
    parser.add_argument(
        "-n", "--count", type=int, default=1024, help="number of test messages to send"
    )
    parser.add_argument(
        "--url",
        dest="url",
        type=str,
        default="tcp://127.0.0.1:5555",
        help="the zmq URL on which to run the test",
    )
    parser.add_argument(
        dest="test",
        nargs="?",
        type=str,
        default="lat",
        choices=["lat", "thr"],
        help="which test to run",
    )
    return parser.parse_args(argv)


def latency_echo(url, count, size=None, poll=False, copy=True, quiet=False):
    ctx = zmq.Context()
    s = ctx.socket(zmq.REP)
    if poll:
        p = zmq.Poller()
        p.register(s)
    s.bind(url)
    block = zmq.NOBLOCK if poll else 0
    for i in range(count + 1):
        if poll:
            p.poll()
        msg = s.recv(block, copy=copy)
        if poll:
            p.poll()
        s.send(msg, block, copy=copy)
    msg = s.recv()
    assert msg == b"done"
    s.close()
    ctx.term()


def latency(url, count, size, poll=False, copy=True, quiet=False):
    ctx = zmq.Context()
    s = ctx.socket(zmq.REQ)
    s.setsockopt(zmq.LINGER, -1)
    s.connect(url)
    if poll:
        p = zmq.Poller()
        p.register(s)
    msg = b" " * size
    block = zmq.NOBLOCK if poll else 0
    s.send(msg)
    s.recv()
    start = now()
    for i in range(0, count):
        if poll:
            res = p.poll()
            assert res[0][1] & zmq.POLLOUT
        s.send(msg, block, copy=copy)
        if poll:
            res = p.poll()
            assert res[0][1] & zmq.POLLIN
        msg = s.recv(block, copy=copy)
        assert len(msg) == size
    elapsed = now() - start
    s.send(b"done")
    latency = 1000000.0 * elapsed / (count * 2.0)
    if not quiet:
        print(f"message size   : {size:8d}     [B]")
        print(f"roundtrip count: {count:8d}     [msgs]")
        print(f"mean latency   : {latency:12.3f} [µs]")
        print(f"test time      : {elapsed:12.3f} [s]")
    ctx.destroy()
    return latency


def thr_sink(url, count, size, poll=False, copy=True, quiet=False):
    ctx = zmq.Context()
    s = ctx.socket(zmq.ROUTER)
    s.RCVHWM = 0
    if poll:
        p = zmq.Poller()
        p.register(s)
    s.bind(url)
    msg = s.recv_multipart()
    assert msg[1] == b"BEGIN", msg
    count = int(msg[2].decode("ascii"))
    s.send_multipart(msg)
    flags = zmq.NOBLOCK if poll else 0
    for i in range(count):
        if poll:
            res = p.poll()
            assert res[0][1] & zmq.POLLIN
        msg = s.recv_multipart(flags=flags, copy=copy)
    s.send_multipart([msg[0], b"DONE"])
    s.close()
    ctx.term()


def throughput(url, count, size, poll=False, copy=True, quiet=False):
    ctx = zmq.Context()
    s = ctx.socket(zmq.DEALER)
    s.SNDHWM = 0
    if poll:
        p = zmq.Poller()
        p.register(s, zmq.POLLOUT)
    s.connect(url)
    data = b" " * size
    flags = zmq.NOBLOCK if poll else 0
    s.send_multipart([b"BEGIN", str(count).encode("ascii")])
    msg = s.recv_multipart()
    assert msg[0] == b"BEGIN"
    start = now()
    for i in range(count):
        if poll:
            res = p.poll()
            assert res[0][1] & zmq.POLLOUT
        s.send(data, flags=flags, copy=copy)
    sent = now()
    reply = s.recv_multipart()
    elapsed = now() - start
    assert reply[0] == b"DONE"
    send_only = sent - start
    send_throughput = count / send_only
    throughput = count / elapsed
    megabits = throughput * size * 8 / 1000000.0
    if not quiet:
        print(f"message size   : {size:8d}     [B]")
        print(f"message count  : {count:8d}     [msgs]")
        print(f"send only      : {send_throughput:8.0f}     [msg/s]")
        print(f"mean throughput: {throughput:8.0f}     [msg/s]")
        print(f"mean throughput: {megabits:12.3f} [Mb/s]")
        print(f"test time      : {elapsed:12.3f} [s]")
    ctx.destroy()
    return (send_throughput, throughput)


def do_run(test, **kwargs):
    if test == "lat":
        bg_func = latency_echo
        fg_func = latency
    elif test == "thr":
        bg_func = thr_sink
        fg_func = throughput
    bg = Process(target=bg_func, kwargs=kwargs)
    bg.start()
    result = fg_func(**kwargs)
    bg.join()
    return result


def main():
    args = parse_args()
    tic = time.time()
    do_run(
        args.test,
        url=args.url,
        size=args.size,
        count=args.count,
        poll=args.poll,
        copy=args.copy,
    )
    toc = time.time()
    if toc - tic < 3:
        print("For best results, tests should take at least a few seconds.")


if __name__ == "__main__":
    main()
