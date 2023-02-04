import datetime
from typing import Iterable, Optional
from datetime import timezone

from influxdb_client.client.write.point import EPOCH

from . import LineProtocol, TimestampTuple, protect_from, logger_factory

log = logger_factory("line-proto")


def to_influx_timestamp(dt: datetime.datetime) -> int:
    """Converts python datetime.datetime object
    to a nanosecond precision timestep consumable by influxdb

    :param dt: datetime to convert
    :type dt: datetime.datetime
    :return: timestep as integer value
    :rtype: int
    """
    time = (dt.astimezone(timezone.utc) - EPOCH).total_seconds() * 1e9
    return int(time)


def nmon_cpu_mertic_collector(
    measurement: str, run_id: str, cpu_id: str
) -> LineProtocol:
    """Factory utility for CPU metric collection
    Creates a line protocol for parsing nmon outputs

    :param measurement: measurement name in influx
    :type measurement: str
    :param run_name: tag specifying run parameters
    :type run_name: str
    :param cpu_id: core number/core average
    :type cpu_id: str
    :return: line protocol for cpu metric collection
    :rtype: LineProtocol
    """

    @protect_from(ValueError, "cpu")
    def parse_cpu_all(
        line: str, ts: Optional[TimestampTuple]
    ) -> Iterable[str]:
        if ts is None:
            log.warning(f"cpu={cpu_id}: no ts specified")
            return
        # CPU_ALL,CPU Total username,User%,Sys%,Wait%,Idle%,Steal%,Busy,CPUs
        # CPU_ALL,T0001,2.4,1.0,0.3,96.4,0.0,,12
        index, *metrics = map(lambda x: x if x else 0, line.split(",", 8))
        if index != ts.code:
            # timestep got shifted for some reason
            return
        user, sys, wait, idle, steal, *_ = map(float, metrics)

        yield (
            f"{measurement},"
            f"run={run_id},cpus={cpu_id}"
            f" user={user},sys={sys},wait={wait},idle={idle},steal={steal}"
            f" {to_influx_timestamp(ts.datetime)}"
        )

    return parse_cpu_all


def nmon_mem_metric_collector(measurement: str, run_id: str) -> LineProtocol:
    @protect_from(ValueError, "mem")
    def parse_mem(line: str, ts: Optional[TimestampTuple]) -> Iterable[str]:
        if ts is None:
            log.warning("mem: no ts specified")
            return
        # MEM,Memory MB shitbarn,memtotal,hightotal,lowtotal,swaptotal,
        # memfree,highfree,lowfree,swapfree,memshared,
        # cached,active,bigfree,buffers,swapcached,inactive
        # MEM,T0001,13841.9,-0.0,-0.0,0.0,7762.2,-0.0,-0.0,0.0,188.6,1981.8,780.1,-1.0,172.1,0.0,4513.8
        index, *metrics = map(lambda x: x if x else 0, line.split(",", 16))
        if index != ts.code:
            return
        (
            memtotal,
            hightotal,
            lowtotal,
            swaptotal,
            memfree,
            highfree,
            lowfree,
            swapfree,
            memshared,
            cached,
            active,
            bigfree,
            buffers,
            swapcached,
            inactive,
        ) = map(float, metrics)
        yield (
            f"{measurement},"
            f"run={run_id}"
            f" memtotal={memtotal},swaptotal={swaptotal},"
            f"memfree={memfree},swapfree={swapfree},"
            f"memshared={memshared},cached={cached},"
            f"active={active},buffers={buffers},"
            f"swapcached={swapcached},inactive={inactive}"
            f" {to_influx_timestamp(ts.datetime)}"
        )

    return parse_mem


def nmon_disk_metric_collector(
    measurement: str, run_id: str, disk_names: Iterable[str], mode: str
) -> LineProtocol:
    modes = {"r": "read", "w": "write", "b": "busy"}
    if mode not in modes:
        raise ValueError(f"invalid mode: {mode}. expected 'r', 'w', 'b'")
    mode = modes[mode]

    # nmon disk format is a bit challenging:
    # all system disks are listed as columns, thus we can't tell
    # in advance, how many metrics there are going to be
    # DISKBUSY,Disk %Busy username,
    # nvme0n1,nvme0n1p1,nvme0n1p2,nvme0n1p3,nvme0n1p4,nvme0n1p5,loop0
    # DISKBUSY,T0001,4.7,0.0,0.0,0.0,0.0,4.7,0.0

    @protect_from(ValueError, f"disk-{mode}")
    def parse_disk(line: str, ts: Optional[TimestampTuple]) -> Iterable[str]:
        if ts is None:
            log.warning("disk: no ts specified")
            return

        index, *disk_metrics = line.split(",")
        if index != ts.code:
            return

        timestamp = to_influx_timestamp(ts.datetime)
        for name, metric in zip(disk_names, disk_metrics):
            yield (
                f"{measurement},"
                f"run={run_id},disk={name},mode={mode}"
                f" value={metric}"
                f" {timestamp}"
            )

    return parse_disk
