from contextlib import closing
import socket
import os
import sys
import pytest
from shyft.api import Calendar, UtcPeriod, StringVector, TsVector, DtsClient, TimeSeries
from weather.service.dtss import DtssHost, create_heartbeat_request
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler()
    ])

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
    ts_ids = ['mock1://something/1', 'mock2://something_else/2', 'mock1://something_strange/3']
    expected = [1, 2, 3]
    result = dtss.read_callback(ts_ids=StringVector(ts_ids),
                                read_period=UtcPeriod(cal.time(0), cal.time(5)))
    assert isinstance(result, TsVector)
    for ts, value in zip(result, expected):
        assert ts.values.to_numpy()[0] == value


def test_find_callback_success(dtss):
    query = 'mock1://something/1'
    tsiv = dtss.find_callback(query=query)
    assert tsiv[0].name == query


def test_dts_client(dtss):
    dtss.start()
    timeseries = ['mock1://something/1', 'mock2://something_else/2', 'mock1://something_strange/3']
    cal = Calendar(3600)
    try:
        c = DtsClient(dtss.address)
        tsv_in = TsVector([TimeSeries(ts_id) for ts_id in timeseries])
        period = UtcPeriod(cal.time(2019, 3, 1), cal.time(2019, 3, 3))
        tsv = c.evaluate(tsv_in, period)
        assert tsv
    finally:
        dtss.stop()


def test_dts_client_heartbeat(dtss):
    dtss.start()
    heartbeat = create_heartbeat_request()
    cal = Calendar(3600)
    try:
        c = DtsClient(dtss.address)
        tsv_in = TsVector([TimeSeries(heartbeat)])
        period = UtcPeriod(cal.time(2019, 3, 1), cal.time(2019, 3, 3))
        tsv = c.evaluate(tsv_in, period)
        assert tsv
        tsiv = c.find(heartbeat)
        assert tsiv[0].name == 'heartbeat'
    finally:
        dtss.stop()
