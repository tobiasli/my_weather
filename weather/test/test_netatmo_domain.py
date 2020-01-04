"""Tests for the netadmo domain classes."""
import pytest

from weather.data_sources.netatmo.netatmo_domain import NetatmoDomain, NetatmoDevice
from weather.data_sources.netatmo.netatmo import NetatmoEncryptedEnvVarConfig
from weather.test.bin.netatmo_test_data import MOCK_STATION_CONFIG

# Get credentials:
@pytest.fixture()
def config(pytestconfig):
    """Return an instance of the NetatmoConfig."""
    return NetatmoEncryptedEnvVarConfig(
        username_var='NETATMO_USER',
        password_var='NETATMO_PASS',
        client_id_var='NETATMO_ID',
        client_secret_var='NETATMO_SECRET',
        password=pytestconfig.getoption("password"),
        salt=pytestconfig.getoption("salt"),
    )


def test_station_mock_config():
    device = NetatmoDevice(**MOCK_STATION_CONFIG['station:mock:id:1'])  # Create an instance of the first device.

    assert device.name == 'Livingroom'
    assert device.measurements[0].data_type.name == 'Temperature'
    assert device.measurements[0].name == r'Superstation\Livingroom\Temperature'

    module = device.modules[0]
    assert module.module_name == 'Basement'
    assert module.device.name == 'Livingroom'
    assert module.measurements[0].data_type.name == 'Temperature'
    assert module.measurements[0].name == r'Superstation\Basement\Temperature'
    assert module.measurements[0].source_name == r'Superstation\Basement'


def test_domain_login_mock():
    domain = NetatmoDomain(device_metadata=MOCK_STATION_CONFIG)

    assert domain.devices[0].name == 'Livingroom'
    meas = domain.get_measurement(device_name='Livingroom', module_name='Basement', data_type='Temperature')
    assert meas.device_id == 'bogus:station:id:1'


def test_domain_login(config):
    domain = NetatmoDomain(
        username=config.username,
        password=config.password,
        client_id=config.client_id,
        client_secret=config.client_secret)
    assert domain
