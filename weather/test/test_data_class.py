"""Test the data class objects."""
from weather.utilities.data_class import DataClass


def test_data_class_construction():
    args = dict(name='something', value=2)
    d = DataClass(**args)
    assert d.name == 'something'
    assert d.value == 2


def test_data_class_contains():
    args = dict(name='something', value=2)
    d = DataClass(**args)
    for key in args:
        assert key in d


def test_data_class_dir():
    args = dict(name='something', value=2)
    d = DataClass(**args)

    assert all(key in dir(d) for key in args)


def test_data_class_str():
    args = dict(name='something', value=2)
    d = DataClass(**args)

    assert str(d) == "DataClass(name='something', value=2)"


def test_data_class_repr():
    args = dict(name='something', value=2)
    d = DataClass(**args)

    assert repr(d) == "DataClass(name='something', value=2)"