"""0MQ Error classes and functions."""

from __future__ import annotations
from errno import EINTR


class DraftFDWarning(RuntimeWarning):
    def __init__(self, msg=""):
        if not msg:
            msg = "pyzmq's back-fill socket.FD support on thread-safe sockets is experimental, and may be removed. This warning will go away automatically if/when libzmq implements socket.FD on thread-safe sockets. You can suppress this warning with `warnings.simplefilter('ignore', zmq.error.DraftFDWarning)"
        super().__init__(msg)


class ZMQBaseError(Exception):
    pass


class ZMQError(ZMQBaseError):
    errno: int | None = None
    strerror: str

    def __init__(self, errno: int | None = None, msg: str | None = None):
        from zmq.backend import strerror, zmq_errno

        if errno is None:
            errno = zmq_errno()
        if isinstance(errno, int):
            self.errno = errno
            if msg is None:
                self.strerror = strerror(errno)
            else:
                self.strerror = msg
        elif msg is None:
            self.strerror = str(errno)
        else:
            self.strerror = msg

    def __str__(self) -> str:
        return self.strerror

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{str(self)}')"


class ZMQBindError(ZMQBaseError):
    pass


class NotDone(ZMQBaseError):
    pass


class ContextTerminated(ZMQError):
    def __init__(self, errno="ignored", msg="ignored"):
        from zmq import ETERM

        super().__init__(ETERM)


class Again(ZMQError):
    def __init__(self, errno="ignored", msg="ignored"):
        from zmq import EAGAIN

        super().__init__(EAGAIN)


class InterruptedSystemCall(ZMQError, InterruptedError):
    errno = EINTR
    strerror: str

    def __init__(self, errno="ignored", msg="ignored"):
        super().__init__(EINTR)

    def __str__(self):
        s = super().__str__()
        return s + ": This call should have been retried. Please report this to pyzmq."


def _check_rc(rc, errno=None, error_without_errno=True):
    if rc == -1:
        if errno is None:
            from zmq.backend import zmq_errno

            errno = zmq_errno()
        if errno == 0 and (not error_without_errno):
            return
        from zmq import EAGAIN, ETERM

        if errno == EINTR:
            raise InterruptedSystemCall(errno)
        elif errno == EAGAIN:
            raise Again(errno)
        elif errno == ETERM:
            raise ContextTerminated(errno)
        else:
            raise ZMQError(errno)


_zmq_version_info = None
_zmq_version = None


class ZMQVersionError(NotImplementedError):
    min_version = None

    def __init__(self, min_version: str, msg: str = "Feature"):
        global _zmq_version
        if _zmq_version is None:
            from zmq import zmq_version

            _zmq_version = zmq_version()
        self.msg = msg
        self.min_version = min_version
        self.version = _zmq_version

    def __repr__(self):
        return f"ZMQVersionError('{str(self)}')"

    def __str__(self):
        return f"{self.msg} requires libzmq >= {self.min_version}, have {self.version}"


def _check_version(
    min_version_info: tuple[int] | tuple[int, int] | tuple[int, int, int],
    msg: str = "Feature",
):
    global _zmq_version_info
    if _zmq_version_info is None:
        from zmq import zmq_version_info

        _zmq_version_info = zmq_version_info()
    if _zmq_version_info < min_version_info:
        min_version = ".".join((str(v) for v in min_version_info))
        raise ZMQVersionError(min_version, msg)


__all__ = [
    "DraftFDWarning",
    "ZMQBaseError",
    "ZMQBindError",
    "ZMQError",
    "NotDone",
    "ContextTerminated",
    "InterruptedSystemCall",
    "Again",
    "ZMQVersionError",
]
