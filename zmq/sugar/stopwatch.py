"""Deprecated Stopwatch implementation"""


class Stopwatch:
    def __init__(self):
        import warnings

        warnings.warn(
            "zmq.Stopwatch is deprecated. Use stdlib time.monotonic and friends instead",
            DeprecationWarning,
            stacklevel=2,
        )
        self._start = 0
        import time

        try:
            self._monotonic = time.monotonic
        except AttributeError:
            self._monotonic = time.time

    def start(self):
        self._start = self._monotonic()

    def stop(self):
        stop = self._monotonic()
        return int(1000000.0 * (stop - self._start))
