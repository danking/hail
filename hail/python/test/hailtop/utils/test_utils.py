from hailtop.utils import partition


def test_partition_zero_empty():
    assert list(partition(0, [])) == []


def test_partition_even_small():
    assert list(partition(3, range(3))) == [[0], [1], [2]]


def test_partition_even_big():
    assert list(partition(3, range(9))) == [[0, 1, 2], [3, 4, 5], [6, 7, 8]]


def test_partition_uneven_big():
    assert list(partition(2, range(9))) == [[0, 1, 2, 3, 4], [5, 6, 7, 8]]


def test_partition_toofew():
    assert list(partition(6, range(3))) == [[0], [1], [2], [], [], []]
