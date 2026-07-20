"""Mixin for mapping set/getattr to self.set/get"""

from __future__ import annotations
import errno
from typing import TypeVar, Union
from .. import constants

T = TypeVar("T")
OptValT = Union[str, bytes, int]


class AttributeSetter:
    def __setattr__(self, key: str, value: OptValT) -> None:
        if key in self.__dict__:
            object.__setattr__(self, key, value)
            return
        for cls in self.__class__.mro():
            if key in cls.__dict__ or key in getattr(cls, "__annotations__", {}):
                object.__setattr__(self, key, value)
                return
        upper_key = key.upper()
        try:
            opt = getattr(constants, upper_key)
        except AttributeError:
            raise AttributeError(
                f"{self.__class__.__name__} has no such option: {upper_key}"
            )
        else:
            self._set_attr_opt(upper_key, opt, value)

    def _set_attr_opt(self, name: str, opt: int, value: OptValT) -> None:
        self.set(opt, value)

    def __getattr__(self, key: str) -> OptValT:
        upper_key = key.upper()
        try:
            opt = getattr(constants, upper_key)
        except AttributeError:
            raise AttributeError(
                f"{self.__class__.__name__} has no such option: {upper_key}"
            ) from None
        else:
            from zmq import ZMQError

            try:
                return self._get_attr_opt(upper_key, opt)
            except ZMQError as e:
                if e.errno in {errno.EINVAL, errno.EFAULT}:
                    raise AttributeError(f"{key} attribute is write-only")
                else:
                    raise

    def _get_attr_opt(self, name, opt) -> OptValT:
        return self.get(opt)

    def get(self, opt: int) -> OptValT:
        raise NotImplementedError("override in subclass")

    def set(self, opt: int, val: OptValT) -> None:
        raise NotImplementedError("override in subclass")


__all__ = ["AttributeSetter"]
