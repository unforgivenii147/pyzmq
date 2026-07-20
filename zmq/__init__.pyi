from typing import List
from . import backend, sugar

COPY_THRESHOLD: int
DRAFT_API: bool
__version__: str
from .backend import IPC_PATH_MAX_LEN as IPC_PATH_MAX_LEN
from .backend import curve_keypair as curve_keypair
from .backend import curve_public as curve_public
from .backend import has as has
from .backend import proxy as proxy
from .backend import proxy_steerable as proxy_steerable
from .backend import strerror as strerror
from .backend import zmq_errno as zmq_errno
from .backend import zmq_poll as zmq_poll
from .constants import *
from .error import *
from .sugar import *

def get_includes() -> list[str]: ...
def get_library_dirs() -> list[str]: ...
