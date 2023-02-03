import datetime
from dataclasses import dataclass
from functools import wraps
import logging
from typing import Callable, Iterable, Optional, Type


def logger_factory(name: str) -> logging.Logger:
    # returns logger instance configured with sqlite
    # connection
    log = logging.getLogger(name)
    if not log.hasHandlers():
        log.setLevel(logging.DEBUG)
        # log messages to the stderr, as usual
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            fmt=(
                "[%(asctime)s]::[%(name)s]"
                "::[%(threadName)s]"
                "::[%(levelname)s] - %(message)s"
            ),
            datefmt="%H:%M:%S",
        )
        console.setFormatter(formatter)
        log.addHandler(console)

    return log


for logger_name in ("nmon-parser", "pipe", "line-proto"):
    logger_factory(logger_name)


@dataclass
class TimestampTuple:
    code: str
    datetime: datetime.datetime


LineProtocol = Callable[[str, Optional[TimestampTuple]], Iterable[str]]


def protect_from(exc: Type[Exception], log_prefix: str):
    def protected(fn: LineProtocol) -> LineProtocol:
        log = logging.getLogger("line-proto")

        @wraps(fn)
        def _wrapper(*args, **kwds):
            try:
                yield from fn(*args, **kwds)
            except exc as e:
                log.error(f"{log_prefix} parsing error: {e}")

        return _wrapper

    return protected
