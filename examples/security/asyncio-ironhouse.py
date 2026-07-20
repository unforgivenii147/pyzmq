#!/usr/bin/env python


"""
Ironhouse extends Stonehouse with client public key authentication.

This is the strongest security model we have today, protecting against every
attack we know about, except end-point attacks (where an attacker plants
spyware on a machine to capture data before it's encrypted, or after it's
decrypted).

Author: Steven Armstrong
Based on ./ironhouse.py by Chris Laws
"""

import asyncio
import logging
import sys
from pathlib import Path
import zmq
import zmq.auth
from zmq.asyncio import Context
from zmq.auth.asyncio import AsyncioAuthenticator


async def run() -> None:
    base_dir = Path(__file__).parent
    keys_dir = base_dir / "certificates"
    public_keys_dir = base_dir / "public_keys"
    secret_keys_dir = base_dir / "private_keys"
    if (
        not keys_dir.is_dir()
        or not public_keys_dir.is_dir()
        or (not secret_keys_dir.is_dir())
    ):
        logging.critical(
            "Certificates are missing - run generate_certificates.py script first"
        )
        sys.exit(1)
    ctx = Context.instance()
    auth = AsyncioAuthenticator(ctx)
    auth.start()
    auth.allow("127.0.0.1")
    auth.configure_curve(domain="*", location=public_keys_dir)
    server = ctx.socket(zmq.ROUTER)
    server_secret_file = secret_keys_dir / "server.key_secret"
    server_public, server_secret = zmq.auth.load_certificate(server_secret_file)
    server.curve_secretkey = server_secret
    server.curve_publickey = server_public
    server.curve_server = True
    server.bind("tcp://*:9000")
    client = ctx.socket(zmq.DEALER)
    client_secret_file = secret_keys_dir / "client.key_secret"
    client_public, client_secret = zmq.auth.load_certificate(client_secret_file)
    client.curve_secretkey = client_secret
    client.curve_publickey = client_public
    server_public_file = public_keys_dir / "server.key"
    server_public, _ = zmq.auth.load_certificate(server_public_file)
    client.curve_serverkey = server_public
    client.connect("tcp://127.0.0.1:9000")
    await client.send(b"Hello")
    if await server.poll(1000):
        identity, msg = await server.recv_multipart(copy=False)
        logging.info(f"Received {msg.bytes!r} from {msg['User-Id']!r}")
        if msg.bytes == b"Hello":
            logging.info("Ironhouse test OK")
    else:
        logging.error("Ironhouse test FAIL")
    server.close()
    client.close()
    auth.stop()


if __name__ == "__main__":
    if zmq.zmq_version_info() < (4, 0):
        raise RuntimeError(
            f"Security is not supported in libzmq version < 4.0. libzmq version {zmq.zmq_version()}"
        )
    if "-v" in sys.argv:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(level=level, format="[%(levelname)s] %(message)s")
    asyncio.run(run())
