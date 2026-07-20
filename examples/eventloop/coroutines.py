"""Example using zmq with tornado coroutines"""

import asyncio
import time
import zmq
from zmq.eventloop.future import Context, Poller

url = "tcp://127.0.0.1:5555"
ctx = Context.instance()


async def ping() -> None:
    while True:
        await asyncio.sleep(0.25)
        print(".")


async def receiver() -> None:
    pull = ctx.socket(zmq.PULL)
    pull.connect(url)
    poller = Poller()
    poller.register(pull, zmq.POLLIN)
    while True:
        events = await poller.poll(timeout=500)
        if pull in dict(events):
            print("recving", events)
            msg = await pull.recv_multipart()
            print("recvd", msg)
        else:
            print("nothing to recv")


async def sender() -> None:
    tic = time.time()
    push = ctx.socket(zmq.PUSH)
    push.bind(url)
    poller = Poller()
    poller.register(push, zmq.POLLOUT)
    while True:
        print("sending")
        await push.send_multipart([str(time.time() - tic).encode("ascii")])
        await asyncio.sleep(1)


async def main() -> None:
    await asyncio.gather(ping(), receiver(), sender())


if __name__ == "__main__":
    asyncio.run(main())
