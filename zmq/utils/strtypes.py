"""Declare basic string types unambiguously for various Python versions.

Authors
-------
* MinRK
"""

import warnings

bytes = bytes
unicode = str
basestring = (str,)


def cast_bytes(s, encoding="utf8", errors="strict"):
    warnings.warn(
        "zmq.utils.strtypes is deprecated in pyzmq 23.",
        DeprecationWarning,
        stacklevel=2,
    )
    if isinstance(s, bytes):
        return s
    elif isinstance(s, str):
        return s.encode(encoding, errors)
    else:
        raise TypeError(f"Expected unicode or bytes, got {s!r}")


def cast_unicode(s, encoding="utf8", errors="strict"):
    warnings.warn(
        "zmq.utils.strtypes is deprecated in pyzmq 23.",
        DeprecationWarning,
        stacklevel=2,
    )
    if isinstance(s, bytes):
        return s.decode(encoding, errors)
    elif isinstance(s, str):
        return s
    else:
        raise TypeError(f"Expected unicode or bytes, got {s!r}")


b = asbytes = cast_bytes
u = cast_unicode
__all__ = [
    "asbytes",
    "bytes",
    "unicode",
    "basestring",
    "b",
    "u",
    "cast_bytes",
    "cast_unicode",
]
