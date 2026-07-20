"""Python bindings for 0MQ"""

from __future__ import annotations
import os
import sys
from contextlib import contextmanager


@contextmanager
def _libs_on_path():
    if not sys.platform.startswith("win"):
        yield
        return
    libs_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir, "pyzmq.libs")
    )
    if not os.path.exists(libs_dir):
        yield
        return
    path_before = os.environ.get("PATH")
    try:
        os.environ["PATH"] = os.pathsep.join([path_before or "", libs_dir])
        yield
    finally:
        if path_before is None:
            os.environ.pop("PATH")
        else:
            os.environ["PATH"] = path_before


with _libs_on_path():
    from zmq import backend
from . import constants
from .constants import *
from zmq.backend import *
from zmq import sugar
from zmq.sugar import *


def get_includes():
    from os.path import abspath, dirname, exists, join, pardir

    base = dirname(__file__)
    parent = abspath(join(base, pardir))
    includes = [parent] + [join(parent, base, subdir) for subdir in ("utils",)]
    if exists(join(parent, base, "include")):
        includes.append(join(parent, base, "include"))
    return includes


def get_library_dirs():
    from os.path import abspath, dirname, join, pardir

    base = dirname(__file__)
    parent = abspath(join(base, pardir))
    return [join(parent, base)]


COPY_THRESHOLD = 65536
DRAFT_API: bool = backend.has("draft") and backend.PYZMQ_DRAFT_API
__all__ = (
    ["get_includes", "COPY_THRESHOLD", "DRAFT_API"]
    + constants.__all__
    + sugar.__all__
    + backend.__all__
)
