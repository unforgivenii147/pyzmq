"""Decorators for running functions with context/sockets.

.. versionadded:: 15.3

Like using Contexts and Sockets as context managers, but with decorator syntax.
Context and sockets are closed at the end of the function.

For example::

    from zmq.decorators import context, socket

    @context()
    @socket(zmq.PUSH)
    def work(ctx, push):
        ...
"""

from __future__ import annotations

__all__ = ("context", "socket")
from functools import wraps
import zmq


class _Decorator:
    def __init__(self, target=None):
        self._target = target

    def __call__(self, *dec_args, **dec_kwargs):
        kw_name, dec_args, dec_kwargs = self.process_decorator_args(
            *dec_args, **dec_kwargs
        )

        def decorator(func):

            @wraps(func)
            def wrapper(*args, **kwargs):
                target = self.get_target(*args, **kwargs)
                with target(*dec_args, **dec_kwargs) as obj:
                    if kw_name and kw_name not in kwargs:
                        kwargs[kw_name] = obj
                    elif kw_name and kw_name in kwargs:
                        raise TypeError(
                            f"{func.__name__}() got multiple values for argument '{kw_name}'"
                        )
                    else:
                        args = args + (obj,)
                    return func(*args, **kwargs)

            return wrapper

        return decorator

    def get_target(self, *args, **kwargs):
        return self._target

    def process_decorator_args(self, *args, **kwargs):
        kw_name = None
        if isinstance(kwargs.get("name"), str):
            kw_name = kwargs.pop("name")
        elif len(args) >= 1 and isinstance(args[0], str):
            kw_name = args[0]
            args = args[1:]
        return (kw_name, args, kwargs)


class _ContextDecorator(_Decorator):
    def __init__(self):
        super().__init__(zmq.Context)


class _SocketDecorator(_Decorator):
    def process_decorator_args(self, *args, **kwargs):
        kw_name, args, kwargs = super().process_decorator_args(*args, **kwargs)
        self.context_name = kwargs.pop("context_name", "context")
        return (kw_name, args, kwargs)

    def get_target(self, *args, **kwargs):
        context = self._get_context(*args, **kwargs)
        return context.socket

    def _get_context(self, *args, **kwargs):
        if self.context_name in kwargs:
            ctx = kwargs[self.context_name]
            if isinstance(ctx, zmq.Context):
                return ctx
        for arg in args:
            if isinstance(arg, zmq.Context):
                return arg
        return zmq.Context.instance()


def context(*args, **kwargs):
    return _ContextDecorator()(*args, **kwargs)


def socket(*args, **kwargs):
    return _SocketDecorator()(*args, **kwargs)
