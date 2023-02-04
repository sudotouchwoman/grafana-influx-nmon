import datetime
from typing import Iterable, Tuple

import pytest
import reactivex as rx
import reactivex.operators as ops

from src import TimestampTuple
from src.parsers import nmon_disk_metric_collector, nmon_mem_metric_collector
from src.scraper import NmonHeaderParser, WithListeners


@pytest.fixture
def sample_nmon_output() -> Iterable[str]:
    def opener():
        with open("testing/data/sample_nmon_output.csv", "r") as f:
            yield from f

    return opener()


class MockParser(WithListeners):
    def __init__(self) -> None:
        self.listeners = {}


def test_header_parser(sample_nmon_output: Iterable[str]):
    """
    make sure that header parser adds listener to
    each of the topics (sample nmon output is read
    from file), thus working as expected
    """
    parser = MockParser()
    expected_listeners = set(
        # cpus 1-12, all cpus, memory and disk metrics
        [f"CPU{x:#03}" for x in range(1, 13)]
        + ["CPU_ALL", "MEM"]
        + [f"DISK{x}" for x in ("READ", "WRITE", "BUSY")]
    )

    header_parser = NmonHeaderParser(
        parser,
        measurement="host",
        run_id="0",
    )

    rx.from_iterable(sample_nmon_output).pipe(
        ops.map(header_parser.parse),
        ops.filter(lambda x: x is not None),
    ).subscribe()

    assert set(parser.listeners.keys()) == expected_listeners


@pytest.fixture
def sample_memory_metrics():
    with open("testing/data/sample_mem_metrics.csv") as f:
        return f.read()


def exhausted(i: Iterable[str]):
    # check if given generator is empty
    stub = object()
    return next(iter(i), stub) is stub


def test_mem_metric_parser(sample_memory_metrics: str):
    measurement, run_id = "test", "0"
    parser = nmon_mem_metric_collector(measurement, run_id)

    timestep = sample_memory_metrics.split(",")[0]
    date_info = datetime.datetime.today()
    time_info = TimestampTuple(timestep, date_info)

    # parser should not yield if there was an error
    assert exhausted(parser(sample_memory_metrics, None))
    assert exhausted(parser("invalid", time_info))
    assert not exhausted(parser(sample_memory_metrics, time_info))

    # convert generator to list for easier inspection
    lines_emitted = list(parser(sample_memory_metrics, time_info))

    assert len(lines_emitted) == 1
    line = lines_emitted[0]
    assert len(line.split(" ")) == 3

    measurement_, fields, ts = line.split(" ")
    assert measurement in measurement_
    assert f'run={run_id}' in measurement_
    for field in (
        "memtotal",
        "swaptotal",
        "memfree",
        "swapfree",
        "memshared",
        "cached",
        "active",
        "buffers",
        "swapcached",
        "inactive",
    ):
        assert f"{field}=" in fields
    assert ts.isdigit()


@pytest.fixture
def sample_disk_usage():
    return (
        "nvme0n1,nvme0n1p1,nvme0n1p2,nvme0n1p3,nvme0n1p4,nvme0n1p5",
        "T0004,2.0,0.0,0.0,0.0,0.0,1.0",
    )


def test_disk_metric_parser(sample_disk_usage: Tuple[str, str]):
    disk_names, usage = sample_disk_usage
    disk_names = disk_names.split(",")

    assert len(disk_names) == 6

    timestep = usage.split(",")[0]
    date_info = datetime.datetime.today()
    time_info = TimestampTuple(timestep, date_info)
    disk_modes = {"r": "read", "w": "write", "b": "busy"}

    for mode, parser in map(
        lambda m: (
            disk_modes[m],
            nmon_disk_metric_collector("test", "000", disk_names, mode=m),
        ),
        disk_modes.keys(),
    ):
        assert exhausted(parser(usage, None))
        assert exhausted(parser("invalid-string", time_info))
        assert not exhausted(parser(usage, time_info))

        for disk, emit in zip(disk_names, parser(usage, time_info)):
            assert len(emit.split(" ")) == 3
            measurement, fields, ts = emit.split(" ")
            assert f"mode={mode}" in measurement
            assert f"disk={disk}" in measurement
            assert ts.isdigit()
            assert "value=" in fields
