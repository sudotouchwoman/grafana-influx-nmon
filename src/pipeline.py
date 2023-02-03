import reactivex.operators as ops
from reactivex.observable import Observable

from .scraper import NmonHeaderParser, NmonParser


def nmon_parsing_pipeline(source: Observable[str], run_id: str):
    parser = NmonParser(timestamp_prefix="ZZZZ")
    header_parser = NmonHeaderParser(parser, "shitbarn", run_id)

    collectors_registered = source.pipe(
        ops.take_while(lambda _: not header_parser.registered_all),
        ops.map(header_parser.parse),
        ops.filter(lambda x: x is not None),
    )

    line_proto_stream = source.pipe(
        ops.skip_until(collectors_registered),
        ops.map(parser.parse),
        ops.filter(lambda x: x is not None),
        ops.flat_map(lambda x: x),
    )
    return line_proto_stream
