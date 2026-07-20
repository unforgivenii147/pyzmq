"""PyZMQ and 0MQ version functions."""

from __future__ import annotations
import re
from typing import Match, cast
from zmq.backend import zmq_version_info

__version__: str = "27.1.0"
_version_pat = re.compile("(\\d+)\\.(\\d+)\\.(\\d+)(.*)")
_match = cast(Match, _version_pat.match(__version__))
_version_groups = _match.groups()
VERSION_MAJOR = int(_version_groups[0])
VERSION_MINOR = int(_version_groups[1])
VERSION_PATCH = int(_version_groups[2])
VERSION_EXTRA = _version_groups[3].lstrip(".")
version_info: tuple[int, int, int] | tuple[int, int, int, float] = (
    VERSION_MAJOR,
    VERSION_MINOR,
    VERSION_PATCH,
)
if VERSION_EXTRA:
    version_info = (VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH, float("inf"))
__revision__: str = ""


def pyzmq_version() -> str:
    if __revision__:
        return "+".join([__version__, __revision__[:6]])
    else:
        return __version__


def pyzmq_version_info() -> tuple[int, int, int] | tuple[int, int, int, float]:
    return version_info


def zmq_version() -> str:
    return "{}.{}.{}".format(*zmq_version_info())


__all__ = [
    "zmq_version",
    "zmq_version_info",
    "pyzmq_version",
    "pyzmq_version_info",
    "__version__",
    "__revision__",
]
