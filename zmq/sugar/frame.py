"""0MQ Frame pure Python methods."""

import zmq
from zmq.backend import Frame as FrameBase
from .attrsettr import AttributeSetter


def _draft(v, feature):
    zmq.error._check_version(v, feature)
    if not zmq.DRAFT_API:
        raise RuntimeError(
            f"libzmq and pyzmq must be built with draft support for {feature}"
        )


class Frame(FrameBase, AttributeSetter):
    def __getitem__(self, key):
        return self.get(key)

    def __repr__(self):
        nbytes = len(self)
        msg_suffix = ""
        if nbytes > 16:
            msg_bytes = bytes(memoryview(self.buffer)[:12])
            if nbytes >= 1000000000.0:
                unit = "GB"
                n = nbytes // 1000000000.0
            elif nbytes >= 2**20:
                unit = "MB"
                n = nbytes // 1000000.0
            elif nbytes >= 1000.0:
                unit = "kB"
                n = nbytes // 1000.0
            else:
                unit = "B"
                n = nbytes
            msg_suffix = f"...{n:.0f}{unit}"
        else:
            msg_bytes = self.bytes
        _module = self.__class__.__module__
        if _module == "zmq.sugar.frame":
            _module = "zmq"
        return f"<{_module}.{self.__class__.__name__}({msg_bytes!r}{msg_suffix})>"

    @property
    def group(self):
        _draft((4, 2), "RADIO-DISH")
        return self.get("group")

    @group.setter
    def group(self, group):
        _draft((4, 2), "RADIO-DISH")
        self.set("group", group)

    @property
    def routing_id(self):
        _draft((4, 2), "CLIENT-SERVER")
        return self.get("routing_id")

    @routing_id.setter
    def routing_id(self, routing_id):
        _draft((4, 2), "CLIENT-SERVER")
        self.set("routing_id", routing_id)


Message = Frame
__all__ = ["Frame", "Message"]
