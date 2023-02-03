import datetime
from dataclasses import dataclass
from functools import wraps
import logging
from typing import Callable, Iterable, Optional, Type


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
