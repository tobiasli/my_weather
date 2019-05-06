"""Test the netatmo identifiers."""

from weather.data_sources.netatmo.netatmo_identifiers import (create_ts_id, create_ts_query, parse_ts_query, parse_ts_id,
                                                              NetatmoUrlParseError)


def test_create_ts_id():
    expected = 'netatmo://?device_name=Someplace&module_name=Somewhere&data_type=Earthquake'
    result = create_ts_id(device_name='Someplace', module_name='Somewhere', data_type='Earthquake')
    assert result == expected


def test_parse_ts_query():
    kwargs = {'device_name': 'device1',
              'module_name': 'Somewhere',
              'data_type': 'Earthquake'}
    ts_id = create_ts_id(**kwargs)
    result = parse_ts_id(ts_id=ts_id)
    assert kwargs == result


def test_parse_ts_query_wrong_repo():
    """Should fail when scheme does not match repo name."""
    ts_query = 'bogus://?device_name=Someplace&module_name=Somewhere&data_type=Earthquake'
    try:
        parse_ts_query(ts_query=ts_query)
        assert False
    except NetatmoUrlParseError as e:
        assert 'scheme does not match repository' in str(e)


def test_create_ts_query():
    expected = 'netatmo://?device_name=Somewhere&module_name=&data_type=Earthquake'
    result = create_ts_query(device_name='Somewhere', data_type='Earthquake')
    assert result == expected


