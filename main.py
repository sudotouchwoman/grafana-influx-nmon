from datetime import datetime
import reactivex as rx
import reactivex.operators as ops

from src import logger_factory
from src.client import stream_subprocess_stdout
from src.pipeline import nmon_parsing_pipeline


def prefix_filter(line: str) -> bool:
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
    stream = stream_subprocess_stdout(["sh", "scripts/nmon-to-stdout.sh"])
    log = logger_factory("main")

    def interceptor(x: str):
        log.debug(f"nmon: {x}")
        return x

    stream = rx.from_iterable(stream).pipe(
        ops.filter(prefix_filter),
        ops.map(lambda l: l.rstrip("\n")),
        ops.map(interceptor),
    )

    run_id = f"nmon-shitbarn-{datetime.now().isoformat()}"
    nmon_parsing_pipeline(source=stream, run_id=run_id).subscribe(
        on_next=lambda x: log.info(f"{x}"),
        on_error=lambda x: log.error(f"error: {x}"),
        on_completed=lambda: log.info("completed"),
    )


if __name__ == "__main__":
    main()
