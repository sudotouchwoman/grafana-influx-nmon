import datetime
import logging
from typing import Dict, Iterable, Optional, Set

from .parsers import (
    nmon_cpu_mertic_collector,
    nmon_disk_metric_collector,
    nmon_mem_metric_collector,
)

from . import TimestampTuple, LineProtocol


class WithListeners:
    listeners: Dict[str, LineProtocol]

    def add_listener(self, prefix: str, collector: LineProtocol):
        self.listeners[prefix] = collector


class NmonParser(WithListeners):
    timestamp: Optional[TimestampTuple]
    timestamp_prefix: str

    def __init__(
        self,
        timestamp_prefix: str,
    ) -> None:
        self.log = logging.getLogger("nmon-parser")
        self.listeners = {}
        self.timestamp = None
        self.timestamp_prefix = timestamp_prefix

    def add_listener(self, prefix: str, collector: LineProtocol):
        self.log.debug(f"adding listener: {prefix}")
        return super().add_listener(prefix, collector)

    def parse(self, line: str) -> Iterable[str]:
        # self.log.info(f"given: {line}")
        prefix, _, arguments = line.partition(",")
        if prefix == self.timestamp_prefix:
            self.timestamp = self.timestamp_parser(arguments)
            return
        if prefix not in self.listeners:
            self.log.warning(f"omit unknown prefix: {prefix}")
            return
        yield from self.listeners[prefix](arguments, self.timestamp)

    def timestamp_parser(self, line: str) -> Optional[TimestampTuple]:
        # nmon timestamps utilize the following format:
        # ZZZZ,T0004,15:50:44,02-FEB-2023
        # this method recieves line without the first column
        try:
            index, _, encoded_date = line.partition(",")
            return TimestampTuple(index, parse_nmon_date(encoded_date))
        except ValueError as e:
            self.log.error(f"timestamp parsing ({line}): {e}")


def parse_nmon_date(line: str) -> datetime.datetime:
    time, date = line.split(",", 2)
    day, month, year = date.split("-", 3)
    month = month.capitalize()  # i.e., map JAN to Jan
    return datetime.datetime.strptime(
        f"{time} {day}-{month}-{year}", "%H:%M:%S %d-%b-%Y"
    )


class NmonHeaderParser:
    """
    Registers collectors (metric parsers) for the main parser
    based on nmon's headers
    """

    __registered: Set[str]

    def __init__(self, parser: WithListeners, measurement: str, run_id: str):
        self.log = logging.getLogger("nmon-parser")
        self.parser = parser
        self.measurement = measurement
        self.run_id = run_id
        self.__registered = set()

    @property
    def registered_all(self):
        return self.__registered == {"CPU", "MEM", "DISK"}

    def parse(self, line: str):
        if self.registered_all:
            self.log.debug("header parsing completed")
            return True
        if line.startswith("CPU") and "CPU" not in self.__registered:
            cpu_id, *_ = line.partition(",")
            self.add_cpu_listener(cpu_id)
            if cpu_id == "CPU_ALL":
                self.__registered.add("CPU")
            return
        if line.startswith("MEM") and "MEM" not in self.__registered:
            self.add_mem_listener()
            self.__registered.add("MEM")
            return
        if line.startswith("DISK") and "DISK" not in self.__registered:
            _, _, *disk_ids = line.split(",")
            self.add_disk_listeners(disk_ids)
            self.__registered.add("DISK")

    def add_cpu_listener(self, cpu_id: str):
        # self.log.debug(f"adds cpu listener {cpu_id}")
        self.parser.add_listener(
            cpu_id,
            nmon_cpu_mertic_collector(
                f"cpu-{self.measurement}",
                self.run_id,
                cpu_id,
            ),
        )

    def add_mem_listener(self):
        # self.log.debug("adds mem listener")
        self.parser.add_listener(
            "MEM",
            nmon_mem_metric_collector(f"mem-{self.measurement}", self.run_id),
        )

    def add_disk_listeners(self, disk_ids: Iterable[str]):
        for mode, prefix in zip("rwb", ("DISKREAD", "DISKWRITE", "DISKBUSY")):
            # self.log.debug(f"adds {prefix} listener")
            self.parser.add_listener(
                prefix,
                nmon_disk_metric_collector(
                    f"disk-{self.measurement}", self.run_id, disk_ids, mode
                ),
            )
