"""Tests for the netadmo domain classes."""
from weather.data_sources.netatmo.netatmo_domain import NetatmoDomain, NetatmoDevice
from weather.data_sources.netatmo import get_netatmo_domain
from weather.test.bin.netatmo_test_data import MOCK_STATION_CONFIG
import os
import sys

# Get credentials:
if not 'CONFIG_DIRECTORY' in os.environ:
    raise EnvironmentError('Cannot find path netatmo configs in env var CONFIG_DIRECTORY.')

sys.path.append(os.environ['CONFIG_DIRECTORY'])

from netatmo_config import login


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


def test_domain_login():
    domain = get_netatmo_domain()
    assert domain


