"""Tracker for zero-copy messages with 0MQ."""

from __future__ import annotations
import time
from threading import Event
from zmq.backend import Frame
from zmq.error import NotDone


class MessageTracker:
    events: set[Event]
    peers: set[MessageTracker]

    def __init__(self, *towatch: tuple[MessageTracker | Event | Frame]):
        self.events = set()
        self.peers = set()
        for obj in towatch:
            if isinstance(obj, Event):
                self.events.add(obj)
            elif isinstance(obj, MessageTracker):
                self.peers.add(obj)
            elif isinstance(obj, Frame):
                if not obj.tracker:
                    raise ValueError("Not a tracked message")
                self.peers.add(obj.tracker)
            else:
                raise TypeError(f"Require Events or Message Frames, not {type(obj)}")

    @property
    def done(self):
        for evt in self.events:
            if not evt.is_set():
                return False
        for pm in self.peers:
            if not pm.done:
                return False
        return True

    def wait(self, timeout: float | int = -1):
        tic = time.time()
        remaining: float
        if timeout is False or timeout < 0:
            remaining = 3600 * 24 * 7
        else:
            remaining = timeout
        for evt in self.events:
            if remaining < 0:
                raise NotDone
            evt.wait(timeout=remaining)
            if not evt.is_set():
                raise NotDone
            toc = time.time()
            remaining -= toc - tic
            tic = toc
        for peer in self.peers:
            if remaining < 0:
                raise NotDone
            peer.wait(timeout=remaining)
            toc = time.time()
            remaining -= toc - tic
            tic = toc


_FINISHED_TRACKER = MessageTracker()
__all__ = ["MessageTracker", "_FINISHED_TRACKER"]
