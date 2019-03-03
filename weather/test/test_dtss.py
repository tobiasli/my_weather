from contextlib import closing
import socket
import os
import sys
import pytest
from shyft.api import Calendar, UtcPeriod, StringVector, TsVector
from weather.service.dtss import DtssHost

# Get credentials:
if not 'CONFIG_DIRECTORY' in os.environ:
    raise EnvironmentError('Cannot find path to app authentication codes.')

sys.path.append(os.environ['CONFIG_DIRECTORY'])

from dtss_config import test_configs

if socket.gethostname() not in test_configs:
    raise Exception(f"Can't find configuration for machine {socket.gethostname()}")
DTSS_TEST_CONFIG = test_configs[socket.gethostname()]


def find_free_port() -> int:
    """
    from SO https://stackoverflow.com/questions/1365265/on-localhost-how-to-pick-a-free-port-number
    Returns:
         An available port number for use.
    """
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


def test_dtss_construction():
    dtss = DtssHost(**DTSS_TEST_CONFIG, dtss_port_num=find_free_port())


def test_dtss_start_service():
    dtss = DtssHost(**DTSS_TEST_CONFIG, dtss_port_num=find_free_port())

    dtss.start()
    dtss.stop()


@pytest.fixture(scope="session")
def dtss() -> DtssHost:
    return DtssHost(**DTSS_TEST_CONFIG, dtss_port_num=find_free_port())


def test_read_callback_success(dtss):
    cal = Calendar()
    result = dtss.read_callback(ts_ids=StringVector(['mock://something', 'mock://something_else', 'mock2://something_strange']),
                                read_period=UtcPeriod(cal.time(0), cal.time(5)))
    assert isinstance(result, TsVector)
