from shyft.time_series import Calendar, UtcPeriod, StringVector
import pytest
import logging

from weather.data_sources.netatmo.repository import NetatmoRepository, NetatmoEncryptedEnvVarConfig
from weather.data_sources.netatmo.domain import types, NetatmoDomain
from weather.data_sources.netatmo.identifiers import create_ts_id, create_ts_query
from weather.test.bin.netatmo_test_data import MOCK_STATION_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler()
    ])

# Get credentials:
@pytest.fixture()
def config(pytestconfig):
    """Return an instance of the NetatmoConfig."""
    try:
        return NetatmoEncryptedEnvVarConfig(
            username_var='NETATMO_USER',
            password_var='NETATMO_PASS',
            client_id_var='NETATMO_ID',
            client_secret_var='NETATMO_SECRET',
            password=pytestconfig.getoption("password"),
            salt=pytestconfig.getoption("salt"),
        )
    except EnvironmentError as e:
        return None

@pytest.fixture
def net(config):
    """Return a netatmo instance."""
    if config is None:
        return None
    return NetatmoRepository(**config)


@pytest.fixture()
def domain_mock():
    """Create an instance of the Netatmo domain model."""
    return NetatmoDomain(device_metadata=MOCK_STATION_CONFIG)


@pytest.fixture()
def domain(config):
    """Create an instance of the Netatmo domain model."""
    if config is None:
        return None
    return NetatmoDomain(username=config.username,
                         password=config.password,
                         client_id=config.client_id,
                         client_secret=config.client_secret)


@pytest.fixture
def net_no_login_w_call_limits(config):
    """Return a netatmo instance without login to check handling of api rate limits."""
    if config is None:
        pytest.skip(f'Netatmo is not properly configured.')
    net = NetatmoRepository(
        username=config.username,
        password=config.password,
        client_id=config.client_id,
        client_secret=config.client_secret,
        api_limits={
            '10 seconds': {'calls': 10, 'timespan': 10, 'wait': 0.0001},
            '100 seconds': {'calls': 500, 'timespan': 100, 'wait': 0.001},
        }, direct_login=False)
    return net


def test_construction(net):
    if net is None:
        pytest.skip(f'Netatmo is not properly configured.')
    assert net.domain


def test__get_measurements_block(net, domain):
    if domain is None:
        pytest.skip(f'Netatmo is not properly configured.')
    measurement = domain.get_measurement(device_name='Stua', data_type='Temperature')
    tsvec = net._get_measurements_block(device_id=measurement.device_id,
                                        module_id=measurement.module_id,
                                        measurements=[measurement.data_type.name])

    assert tsvec[0].values.to_numpy().all()


def test_get_measurements(net, domain):
    if domain is None:
        pytest.skip(f'Netatmo is not properly configured.')
    # This method uses a period longer than 1024 values, utilizing a rate limiter to not trip Netatmo api limits.
    period = UtcPeriod(Calendar().time(2019, 3, 1), Calendar().time(2019, 3, 8))
    measurement = domain.get_measurement(device_name='Stua', data_type=types.temperature.name)
    tsvec = net.get_measurements(device_id=measurement.device_id,
                                 module_id=measurement.module_id,
                                 measurements=[measurement.data_type.name, types.humidity.name],
                                 utc_period=period)

    assert tsvec[0].values.to_numpy().all()


def test_read_callback(net):
    if net is None:
        pytest.skip(f'Netatmo is not properly configured.')

    ts_id = create_ts_id(device_name='Stua', data_type=types.temperature)
    cal = Calendar()
    tsvec = net.read_callback(ts_ids=StringVector([ts_id]),
                              read_period=UtcPeriod(cal.time(2019, 2, 20), cal.time(2019, 2, 22)))

    assert tsvec


def test_find_callback(net):
    if net is None:
        pytest.skip(f'Netatmo is not properly configured.')

    ts_query = create_ts_query(device_name='Stua', data_type=types.humidity)
    tsiv = net.find_callback(query=ts_query)

    assert tsiv
