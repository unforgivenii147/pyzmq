"""pyzmq logging handlers.

This mainly defines the PUBHandler object for publishing logging messages over
a zmq.PUB socket.

The PUBHandler can be used with the regular logging module, as in::

    >>> import logging
    >>> handler = PUBHandler('tcp://127.0.0.1:12345')
    >>> handler.root_topic = 'foo'
    >>> logger = logging.getLogger('foobar')
    >>> logger.setLevel(logging.DEBUG)
    >>> logger.addHandler(handler)

Or using ``dictConfig``, as in::

    >>> from logging.config import dictConfig
    >>> socket = Context.instance().socket(PUB)
    >>> socket.connect('tcp://127.0.0.1:12345')
    >>> dictConfig({
    >>>     'version': 1,
    >>>     'handlers': {
    >>>         'zmq': {
    >>>             'class': 'zmq.log.handlers.PUBHandler',
    >>>             'level': logging.DEBUG,
    >>>             'root_topic': 'foo',
    >>>             'interface_or_socket': socket
    >>>         }
    >>>     },
    >>>     'root': {
    >>>         'level': 'DEBUG',
    >>>         'handlers': ['zmq'],
    >>>     }
    >>> })


After this point, all messages logged by ``logger`` will be published on the
PUB socket.

Code adapted from StarCluster:

    https://github.com/jtriley/StarCluster/blob/StarCluster-0.91/starcluster/logger.py
"""

from __future__ import annotations
import logging
from copy import copy
import zmq

TOPIC_DELIM = "::"


class PUBHandler(logging.Handler):
    ctx: zmq.Context
    socket: zmq.Socket

    def __init__(
        self,
        interface_or_socket: str | zmq.Socket,
        context: zmq.Context | None = None,
        root_topic: str = "",
    ) -> None:
        logging.Handler.__init__(self)
        self.root_topic = root_topic
        self.formatters = {
            logging.DEBUG: logging.Formatter(
                "%(levelname)s %(filename)s:%(lineno)d - %(message)s\n"
            ),
            logging.INFO: logging.Formatter("%(message)s\n"),
            logging.WARN: logging.Formatter(
                "%(levelname)s %(filename)s:%(lineno)d - %(message)s\n"
            ),
            logging.ERROR: logging.Formatter(
                "%(levelname)s %(filename)s:%(lineno)d - %(message)s - %(exc_info)s\n"
            ),
            logging.CRITICAL: logging.Formatter(
                "%(levelname)s %(filename)s:%(lineno)d - %(message)s\n"
            ),
        }
        if isinstance(interface_or_socket, zmq.Socket):
            self.socket = interface_or_socket
            self.ctx = self.socket.context
        else:
            self.ctx = context or zmq.Context()
            self.socket = self.ctx.socket(zmq.PUB)
            self.socket.bind(interface_or_socket)

    @property
    def root_topic(self) -> str:
        return self._root_topic

    @root_topic.setter
    def root_topic(self, value: str):
        self.setRootTopic(value)

    def setRootTopic(self, root_topic: str):
        if isinstance(root_topic, bytes):
            root_topic = root_topic.decode("utf8")
        self._root_topic = root_topic

    def setFormatter(self, fmt, level=logging.NOTSET):
        if level == logging.NOTSET:
            for fmt_level in self.formatters.keys():
                self.formatters[fmt_level] = fmt
        else:
            self.formatters[level] = fmt

    def format(self, record):
        return self.formatters[record.levelno].format(record)

    def emit(self, record):
        try:
            topic, msg = str(record.msg).split(TOPIC_DELIM, 1)
        except ValueError:
            topic = ""
        else:
            record = copy(record)
            record.msg = msg
        try:
            bmsg = self.format(record).encode("utf8")
        except Exception:
            self.handleError(record)
            return
        topic_list = []
        if self.root_topic:
            topic_list.append(self.root_topic)
        topic_list.append(record.levelname)
        if topic:
            topic_list.append(topic)
        btopic = ".".join(topic_list).encode("utf8", "replace")
        self.socket.send_multipart([btopic, bmsg])


class TopicLogger(logging.Logger):
    def log(self, level, topic, msg, *args, **kwargs):
        logging.Logger.log(self, level, f"{topic}{TOPIC_DELIM}{msg}", *args, **kwargs)


for name in "debug warn warning error critical fatal".split():
    try:
        meth = getattr(logging.Logger, name)
    except AttributeError:
        continue
    setattr(
        TopicLogger,
        name,
        lambda self, level, topic, msg, *args, **kwargs: meth(
            self, level, topic + TOPIC_DELIM + msg, *args, **kwargs
        ),
    )
