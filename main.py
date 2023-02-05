import os
from datetime import datetime

import reactivex as rx
import reactivex.operators as ops

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, WriteOptions

from src import logger_factory
from src.client import stream_subprocess_stdout
from src.pipeline import nmon_parsing_pipeline


def prefix_filter(line: str) -> bool:
    # skip odd lines
    # this list can be modified, however, e.g.
    # if preamble data should be parsed (like os
    # version/configuration)
    return not line.startswith(
        (
            "AAA",
            "BBBP",
            "DISKBSIZE",
            "JFSFILE",
            "DISKXFER",
            "NET",
            "VM",
            "PROC",
        )
    )


def main():
    load_dotenv("influx.env")
    log = logger_factory("main")

    stream = stream_subprocess_stdout(["sh", "scripts/nmon-to-stdout.sh"])

    def interceptor(x: str):
        log.debug(f"nmon: {x}")
        return x

    stream = rx.from_iterable(stream).pipe(
        ops.filter(prefix_filter),
        ops.map(lambda l: l.rstrip("\n")),
        ops.map(interceptor),
    )

    run_id = f"nmon-shitbarn-{datetime.now().isoformat()}"
    data = nmon_parsing_pipeline(source=stream, run_id=run_id)

    with InfluxDBClient(
        url=os.getenv("INFLUX_API_URL", "http://localhost:8086"),
        token=os.getenv("INFLUX_API_TOKEN", None),
        org=os.getenv("INFLUX_ORG", "my-org"),
    ) as client:
        with client.write_api(
            write_options=WriteOptions(batch_size=1)
        ) as write_api:
            write_api.write(
                bucket=os.getenv("INFLUX_BUCKET_NAME", "performance-metrics"),
                record=data,
            )


if __name__ == "__main__":
    main()
