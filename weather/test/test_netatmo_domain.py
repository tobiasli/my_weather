"""Tests for the netadmo domain classes.

Assumes that pytest is run with the following arguments:
-q --password="password" --salt="salt"
"""
import pytest

from weather.data_sources.netatmo.domain import NetatmoDomain, NetatmoStation
from weather.data_sources.netatmo.repository import NetatmoEncryptedEnvVarConfig
from weather.test.bin.netatmo_test_data import MOCK_STATION_CONFIG


def pytest_addoption(parser):
    parser.addoption("--password", action="store", default="password for decrypting env vars")
    parser.addoption("--salt", action="store", default="salt for decrypting env vars")

@pytest.fixture
def password(request):
    return request.config.getoption("--password")


@pytest.fixture
def salt(request):
    return request.config.getoption("--salt")

# Get credentials:
@pytest.fixture()
def config(password, salt):
    """Return an instance of the NetatmoConfig."""
    try:
        return NetatmoEncryptedEnvVarConfig(
            username_var='NETATMO_USER',
            password_var='NETATMO_PASS',
            client_id_var='NETATMO_ID',
            client_secret_var='NETATMO_SECRET',
            password=password,
            salt=salt,
        )
    except EnvironmentError as e:
        return None

def test_station_mock_config():
    station = NetatmoStation(**MOCK_STATION_CONFIG['station:mock:id:1'])  # Create an instance of the first device.

    assert station.station_name == 'Superstation'

    module = station.modules[0]
    assert module.module_name == 'Livingroom'
    assert module.station.station_name == 'Superstation'
    assert module.measurements[0].data_type.name == 'Temperature'
    assert module.measurements[0].measurement_name == r'Superstation\Livingroom\Temperature'
    assert module.measurements[0].module.name == r'Livingroom'

    module = station.modules[1]
    assert module.module_name == 'Basement'
    assert module.station.station_name == 'Superstation'
    assert module.measurements[0].data_type.name == 'Temperature'
    assert module.measurements[0].measurement_name == r'Superstation\Basement\Temperature'
    assert module.measurements[0].module.name == r'Basement'


def test_domain_login_mock():
    domain = NetatmoDomain(device_metadata=MOCK_STATION_CONFIG)

    assert domain.stations[0].name == 'Superstation'
    meas = domain.get_measurement(station_name='Superstation', module_name='Basement', data_type='Temperature')
    assert meas.station.id == 'bogus:station:id:1'


def test_domain_login(config):
    if config is None:
        pytest.skip(f'Netatmo is not properly configured.')
    domain = NetatmoDomain(
        username=config.username,
        password=config.password,
        client_id=config.client_id,
        client_secret=config.client_secret)
    assert domain
