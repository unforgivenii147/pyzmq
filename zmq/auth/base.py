"""Base implementation of 0MQ authentication."""

import logging
import os
from typing import Any, Awaitable, Dict, List, Optional, Set, Tuple, Union
import zmq
from zmq.error import _check_version
from zmq.utils import z85
from .certs import load_certificates

CURVE_ALLOW_ANY = "*"
VERSION = b"1.0"


class Authenticator:
    context: "zmq.Context"
    encoding: str
    allow_any: bool
    credentials_providers: Dict[str, Any]
    zap_socket: "zmq.Socket"
    _allowed: Set[str]
    _denied: Set[str]
    passwords: Dict[str, Dict[str, str]]
    certs: Dict[str, Dict[bytes, Any]]
    log: Any

    def __init__(
        self,
        context: Optional["zmq.Context"] = None,
        encoding: str = "utf-8",
        log: Any = None,
    ):
        _check_version((4, 0), "security")
        self.context = context or zmq.Context.instance()
        self.encoding = encoding
        self.allow_any = False
        self.credentials_providers = {}
        self.zap_socket = None
        self._allowed = set()
        self._denied = set()
        self.passwords = {}
        self.certs = {}
        self.log = log or logging.getLogger("zmq.auth")

    def start(self) -> None:
        self.zap_socket = self.context.socket(zmq.REP, socket_class=zmq.Socket)
        self.zap_socket.linger = 1
        self.zap_socket.bind("inproc://zeromq.zap.01")
        self.log.debug("Starting")

    def stop(self) -> None:
        if self.zap_socket:
            self.zap_socket.close()
        self.zap_socket = None

    def allow(self, *addresses: str) -> None:
        if self._denied:
            raise ValueError("Only use allow or deny, not both")
        self.log.debug("Allowing %s", ",".join(addresses))
        self._allowed.update(addresses)

    def deny(self, *addresses: str) -> None:
        if self._allowed:
            raise ValueError("Only use a allow or deny, not both")
        self.log.debug("Denying %s", ",".join(addresses))
        self._denied.update(addresses)

    def configure_plain(
        self, domain: str = "*", passwords: Optional[Dict[str, str]] = None
    ) -> None:
        if passwords:
            self.passwords[domain] = passwords
        self.log.debug("Configure plain: %s", domain)

    def configure_curve(
        self, domain: str = "*", location: Union[str, os.PathLike] = "."
    ) -> None:
        self.log.debug("Configure curve: %s[%s]", domain, location)
        if location == CURVE_ALLOW_ANY:
            self.allow_any = True
        else:
            self.allow_any = False
            try:
                self.certs[domain] = load_certificates(location)
            except Exception as e:
                self.log.error("Failed to load CURVE certs from %s: %s", location, e)

    def configure_curve_callback(
        self, domain: str = "*", credentials_provider: Any = None
    ) -> None:
        self.allow_any = False
        if credentials_provider is not None:
            self.credentials_providers[domain] = credentials_provider
        else:
            self.log.error("None credentials_provider provided for domain:%s", domain)

    def curve_user_id(self, client_public_key: bytes) -> str:
        return z85.encode(client_public_key).decode("ascii")

    def configure_gssapi(
        self, domain: str = "*", location: Optional[str] = None
    ) -> None:
        pass

    async def handle_zap_message(self, msg: List[bytes]):
        if len(msg) < 6:
            self.log.error("Invalid ZAP message, not enough frames: %r", msg)
            if len(msg) < 2:
                self.log.error("Not enough information to reply")
            else:
                self._send_zap_reply(msg[1], b"400", b"Not enough frames")
            return
        version, request_id, domain, address, identity, mechanism = msg[:6]
        credentials = msg[6:]
        domain = domain.decode(self.encoding, "replace")
        address = address.decode(self.encoding, "replace")
        if version != VERSION:
            self.log.error("Invalid ZAP version: %r", msg)
            self._send_zap_reply(request_id, b"400", b"Invalid version")
            return
        self.log.debug(
            "version: %r, request_id: %r, domain: %r, address: %r, identity: %r, mechanism: %r",
            version,
            request_id,
            domain,
            address,
            identity,
            mechanism,
        )
        allowed = False
        denied = False
        reason = b"NO ACCESS"
        if self._allowed:
            if address in self._allowed:
                allowed = True
                self.log.debug("PASSED (allowed) address=%s", address)
            else:
                denied = True
                reason = b"Address not allowed"
                self.log.debug("DENIED (not allowed) address=%s", address)
        elif self._denied:
            if address in self._denied:
                denied = True
                reason = b"Address denied"
                self.log.debug("DENIED (denied) address=%s", address)
            else:
                allowed = True
                self.log.debug("PASSED (not denied) address=%s", address)
        username = "anonymous"
        if not denied:
            if mechanism == b"NULL" and (not allowed):
                self.log.debug("ALLOWED (NULL)")
                allowed = True
            elif mechanism == b"PLAIN":
                if len(credentials) != 2:
                    self.log.error("Invalid PLAIN credentials: %r", credentials)
                    self._send_zap_reply(request_id, b"400", b"Invalid credentials")
                    return
                username, password = (
                    c.decode(self.encoding, "replace") for c in credentials
                )
                allowed, reason = self._authenticate_plain(domain, username, password)
            elif mechanism == b"CURVE":
                if len(credentials) != 1:
                    self.log.error("Invalid CURVE credentials: %r", credentials)
                    self._send_zap_reply(request_id, b"400", b"Invalid credentials")
                    return
                key = credentials[0]
                allowed, reason = await self._authenticate_curve(domain, key)
                if allowed:
                    username = self.curve_user_id(key)
            elif mechanism == b"GSSAPI":
                if len(credentials) != 1:
                    self.log.error("Invalid GSSAPI credentials: %r", credentials)
                    self._send_zap_reply(request_id, b"400", b"Invalid credentials")
                    return
                principal = credentials[0]
                username = principal.decode("utf8")
                allowed, reason = self._authenticate_gssapi(domain, principal)
        if allowed:
            self._send_zap_reply(request_id, b"200", b"OK", username)
        else:
            self._send_zap_reply(request_id, b"400", reason)

    def _authenticate_plain(
        self, domain: str, username: str, password: str
    ) -> Tuple[bool, bytes]:
        allowed = False
        reason = b""
        if self.passwords:
            if not domain:
                domain = "*"
            if domain in self.passwords:
                if username in self.passwords[domain]:
                    if password == self.passwords[domain][username]:
                        allowed = True
                    else:
                        reason = b"Invalid password"
                else:
                    reason = b"Invalid username"
            else:
                reason = b"Invalid domain"
            if allowed:
                self.log.debug(
                    "ALLOWED (PLAIN) domain=%s username=%s password=%s",
                    domain,
                    username,
                    password,
                )
            else:
                self.log.debug("DENIED %s", reason)
        else:
            reason = b"No passwords defined"
            self.log.debug("DENIED (PLAIN) %s", reason)
        return (allowed, reason)

    async def _authenticate_curve(
        self, domain: str, client_key: bytes
    ) -> Tuple[bool, bytes]:
        allowed = False
        reason = b""
        if self.allow_any:
            allowed = True
            reason = b"OK"
            self.log.debug("ALLOWED (CURVE allow any client)")
        elif self.credentials_providers != {}:
            if not domain:
                domain = "*"
            if domain in self.credentials_providers:
                z85_client_key = z85.encode(client_key)
                r = self.credentials_providers[domain].callback(domain, z85_client_key)
                if isinstance(r, Awaitable):
                    r = await r
                if r:
                    allowed = True
                    reason = b"OK"
                else:
                    reason = b"Unknown key"
                status = "ALLOWED" if allowed else "DENIED"
                self.log.debug(
                    "%s (CURVE auth_callback) domain=%s client_key=%s",
                    status,
                    domain,
                    z85_client_key,
                )
            else:
                reason = b"Unknown domain"
        else:
            if not domain:
                domain = "*"
            if domain in self.certs:
                z85_client_key = z85.encode(client_key)
                if self.certs[domain].get(z85_client_key):
                    allowed = True
                    reason = b"OK"
                else:
                    reason = b"Unknown key"
                status = "ALLOWED" if allowed else "DENIED"
                self.log.debug(
                    "%s (CURVE) domain=%s client_key=%s", status, domain, z85_client_key
                )
            else:
                reason = b"Unknown domain"
        return (allowed, reason)

    def _authenticate_gssapi(self, domain: str, principal: bytes) -> Tuple[bool, bytes]:
        self.log.debug("ALLOWED (GSSAPI) domain=%s principal=%s", domain, principal)
        return (True, b"OK")

    def _send_zap_reply(
        self,
        request_id: bytes,
        status_code: bytes,
        status_text: bytes,
        user_id: str = "anonymous",
    ) -> None:
        user_id = user_id if status_code == b"200" else b""
        if isinstance(user_id, str):
            user_id = user_id.encode(self.encoding, "replace")
        metadata = b""
        self.log.debug("ZAP reply code=%s text=%s", status_code, status_text)
        reply = [VERSION, request_id, status_code, status_text, user_id, metadata]
        self.zap_socket.send_multipart(reply)


__all__ = ["Authenticator", "CURVE_ALLOW_ANY"]
