"""Utils for interoperability with other libraries.

Just CFFI pointer casting for now.
"""

from typing import Any


def cast_int_addr(n: Any) -> int:
    if isinstance(n, int):
        return n
    try:
        import cffi
    except ImportError:
        pass
    else:
        ffi = cffi.FFI()
        if isinstance(n, ffi.CData):
            return int(ffi.cast("size_t", n))
    raise ValueError(f"Cannot cast {n!r} to int")
