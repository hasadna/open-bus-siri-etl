from open_bus_siri_etl.common import chunked


def test_chunked_empty():
    assert list(chunked([])) == []


def test_chunked_smaller_than_size():
    assert list(chunked([1, 2, 3], size=500)) == [[1, 2, 3]]


def test_chunked_exact_multiple():
    assert list(chunked(range(6), size=2)) == [[0, 1], [2, 3], [4, 5]]


def test_chunked_with_remainder():
    assert list(chunked(range(7), size=2)) == [[0, 1], [2, 3], [4, 5], [6]]


def test_chunked_accepts_a_set():
    # the real call sites pass sets of fetch-keys, not lists
    result = list(chunked(set(range(1000)), size=500))
    assert len(result) == 2
    assert sorted(result[0] + result[1]) == list(range(1000))
