from weather.data_collection.netatmo import NetatmoRepository, NetatmoRepositoryError
from shyft.api import Calendar, UtcPeriod, StringVector
import os
import sys
import pytest
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler()
    ])

# Get credentials:
if not 'CONFIG_DIRECTORY' in os.environ:
    raise EnvironmentError('Cannot find path netatmo configs in env var CONFIG_DIRECTORY.')

sys.path.append(os.environ['CONFIG_DIRECTORY'])

from netatmo_config import login, config


@pytest.fixture
def net():
    """Return a netatmo instance."""
    return NetatmoRepository(**login, **config)


@pytest.fixture
def net_no_login_w_call_limits():
    """Return a netatmo instance without login to check handling of api rate limits."""
    net = NetatmoRepository(**login, api_limits={
        '10 seconds': {'calls': 10, 'timespan': 10, 'wait': 0.0001},
        '100 seconds': {'calls': 500, 'timespan': 100, 'wait': 0.001},
    }, direct_login=False)
    return net


def test_construction(net):
    assert net.stations


def test__get_measurements_from_station(net):
    tsvec = net._get_measurements_from_station(station_name='Eftasåsen', measurement_types=['Temperature'])

    assert tsvec[0].values.to_numpy().all()


def test_get_measurements_from_station(net):
    # This method uses a period longer than 1024 values, utilizing a rate limiter to not trip Netatmo api limits.
    cal = Calendar('Europe/Oslo')
    period = UtcPeriod(cal.time(2019, 2, 18), cal.time(2019, 2, 24))
    tsvec = net.get_measurements_from_station(station_name='Eftasåsen', measurement_types=['Temperature'],
                                              utc_period=period)

    assert tsvec[0].values.to_numpy().all()


def test_create_ts_id():
    expected = 'netatmo://Somewhere/Earthquake'
    result = NetatmoRepository.create_ts_id(station_name='Somewhere', data_type='Earthquake')
    assert result == expected


def test_parse_ts_query():
    kwargs = {'station_name': 'Somewhere',
              'data_type': 'Earthquake'}
    ts_id = NetatmoRepository.create_ts_id(**kwargs)
    result = NetatmoRepository.parse_ts_id(ts_id=ts_id)
    assert kwargs == result


def test_parse_ts_query_wrong_repo():
    """Should fail when scheme does not match repo name."""
    ts_id = 'bogus://Somewhere/Earthquake'
    try:
        NetatmoRepository.parse_ts_id(ts_id=ts_id)
        assert False
    except NetatmoRepositoryError as e:
        assert 'scheme does not match repository' in str(e)


def test_create_ts_query():
    expected = 'netatmo://Somewhere/Earthquake'
    result = NetatmoRepository.create_ts_query(station_name='Somewhere', data_type='Earthquake')
    assert result == expected


def test_read_callback(net):
    ts_id = net.create_ts_query(station_name='Somewhere', data_type='Earthquake')

    tsvec = net.read_callback(ts_ids=StringVector([ts_id]), read_period=UtcPeriod(Calendar().time(2019, 2, 20), Calendar().time(2019, 2, 22)))

    assert tsvec
