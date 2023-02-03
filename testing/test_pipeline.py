import reactivex as rx
import reactivex.operators as ops


def test_toy_pipeline():
    """Make sure that several pipes can be connected to
    a single source with reactivex
    """
    source = (1, 2, 3)
    a_got, a_expected = [], [x * 10 for x in source]
    b_got, b_expected = [], [x for x in source if x % 2 == 0]

    source = rx.from_iterable(source)

    source.pipe(
        ops.map(lambda x: x * 10),
    ).subscribe(lambda l: a_got.append(l))

    source.pipe(
        ops.filter(lambda x: x % 2 == 0),
    ).subscribe(lambda l: b_got.append(l))

    assert a_got == a_expected
    assert b_got == b_expected


def test_flat_map_syntax():
    sequences = (
        ("i", "love", "cheese"),
        ("log", "level", "info"),
    )

    expected = [word for line in sequences for word in line]
    got = []
    rx.from_iterable(sequences).pipe(
        ops.flat_map(lambda x: x),
    ).subscribe(lambda x: got.append(x))

    assert got == expected
