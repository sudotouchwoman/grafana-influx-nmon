import multiprocessing

import reactivex.operators as ops
from reactivex.observable import Observable
from reactivex.scheduler import ThreadPoolScheduler

from .scraper import NmonHeaderParser, NmonParser


def nmon_parsing_pipeline(source: Observable[str], run_id: str):
    parser = NmonParser(timestamp_prefix="ZZZZ")
    header_parser = NmonHeaderParser(parser, "perf-metrics", run_id)

    optimal_thread_count = multiprocessing.cpu_count() * 5
    pool_scheduler = ThreadPoolScheduler(optimal_thread_count)

    collectors_registered = source.pipe(
        ops.observe_on(pool_scheduler),
        ops.take_while(
            lambda _: not header_parser.registered_all,
            inclusive=True,
        ),
        ops.map(header_parser.parse),
        ops.last(),
    )
    collectors_registered.subscribe()

    line_proto_stream = source.pipe(
        ops.observe_on(pool_scheduler),
        ops.map(parser.parse),
        ops.flat_map(lambda x: x),
    )
    return line_proto_stream
