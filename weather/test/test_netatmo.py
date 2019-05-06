from shyft.api import Calendar, UtcPeriod, StringVector
import os
import sys
import pytest
import logging

from weather.data_sources.netatmo.netatmo import NetatmoRepository
from weather.data_sources.netatmo.netatmo_domain import types, NetatmoDomain
from weather.data_sources.netatmo.netatmo_identifiers import create_ts_id, create_ts_query
from weather.test.bin.netatmo_test_data import MOCK_STATION_CONFIG

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


@pytest.fixture()
def domain_mock():
    """Create an instance of the Netatmo domain model."""
    return NetatmoDomain(device_metadata=MOCK_STATION_CONFIG)


@pytest.fixture()
def domain():
    """Create an instance of the Netatmo domain model."""
    return NetatmoDomain(**login)


@pytest.fixture
def net_no_login_w_call_limits():
    """Return a netatmo instance without login to check handling of api rate limits."""
    net = NetatmoRepository(**login, api_limits={
        '10 seconds': {'calls': 10, 'timespan': 10, 'wait': 0.0001},
        '100 seconds': {'calls': 500, 'timespan': 100, 'wait': 0.001},
    }, direct_login=False)
    return net


def test_construction(net):
    assert net.domain


def test__get_measurements_block(net, domain):
    measurement = domain.get_measurement(device_name='Stua', data_type='Temperature')
    tsvec = net._get_measurements_block(device_id=measurement.device_id,
                                        module_id=measurement.module_id,
                                        measurements=[measurement.data_type.name])

    assert tsvec[0].values.to_numpy().all()


def test_get_measurements(net, domain):
    # This method uses a period longer than 1024 values, utilizing a rate limiter to not trip Netatmo api limits.
    period = UtcPeriod(Calendar().time(2019, 3, 1), Calendar().time(2019, 3, 8))
    measurement = domain.get_measurement(device_name='Stua', data_type=types.temperature.name)
    tsvec = net.get_measurements(device_id=measurement.device_id,
                                 module_id=measurement.module_id,
                                 measurements=[measurement.data_type.name, types.humidity.name],
                                 utc_period=period)

    assert tsvec[0].values.to_numpy().all()


def test_read_callback(net):
    ts_id = create_ts_id(device_name='Stua', data_type=types.temperature)
    cal = Calendar()
    tsvec = net.read_callback(ts_ids=StringVector([ts_id]),
                              read_period=UtcPeriod(cal.time(2019, 2, 20), cal.time(2019, 2, 22)))

    assert tsvec


def test_find_callback(net):
    ts_query = create_ts_query(device_name='Stua', data_type=types.humidity)
    tsiv = net.find_callback(query=ts_query)

    assert tsiv
