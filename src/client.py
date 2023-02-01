import subprocess
from typing import Iterable, List, Tuple

import reactivex as rx
import reactivex.operators as ops


def produce_outputs(args: List[str]) -> Iterable[str]:
    """
    Creates an iterator over stdout of subprocess
    called with given executable path
    """
    with subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        universal_newlines=True,
    ) as proc:
        if proc.stdout is None:
            return
        yield from proc.stdout


def parse_outputs(stream: Iterable[str]):
    lines = 0

    def count_lines(x: str) -> Tuple[int, str]:
        nonlocal lines
        lines += 1
        return (lines, x)

    def log_lines(lineno, line):
        print(f"{lineno}: {line}")

    rx.from_iterable(stream).pipe(ops.map(count_lines)).subscribe(
        lambda x: log_lines(*x)
    )


def parse_nmon_csv(stream: Iterable[str]):
    # this is just a simple example
    # one might use a more elaborate
    # logic to dispatch the observables
    rx.from_iterable(stream).pipe(
        ops.filter(lambda l: not l.startswith(("BBBP", "AAA")))
    ).subscribe(lambda l: print(l, end=""))
