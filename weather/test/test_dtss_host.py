import logging
import socket
import os
import sys
import pytest
import tempfile

from contextlib import closing

from shyft.time_series import Calendar, UtcPeriod, StringVector, TsVector, DtsClient, TimeSeries, time

from weather.service.dtss_host import DtssHost
from weather.data_sources.heartbeat import create_heartbeat_request
from weather.test.utilities import MockRepository1, MockRepository2

# noinspection PyArgumentList
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler()
    ])

def find_free_port() -> int:
    """
    from SO https://stackoverflow.com/questions/1365265/on-localhost-how-to-pick-a-free-port-number
    Returns:
         An available port number for use.
    """
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


def test_dtss_start_service():
    dtss = DtssHost(port_num=find_free_port(),
                    container_directory=tempfile.mkdtemp(prefix='dtss_store_'),
                    data_collection_repositories=[(MockRepository1, dict()),
                                                  (MockRepository2, dict())])
    dtss.start()
    dtss.stop()


@pytest.fixture(scope="session")
def dtss() -> DtssHost:
    return DtssHost(port_num=find_free_port(),
                    container_directory=tempfile.mkdtemp(prefix='dtss_store_'),
                    data_collection_repositories=[(MockRepository1, dict()),
                                                  (MockRepository2, dict())])


def test_read_callback_success(dtss):
    cal = Calendar()
    ts_ids = ['mock1://something/1', 'mock2://something_else/2', 'mock1://something_strange/3']
    expected = [1, 2, 3]
    result = dtss.read_callback(ts_ids=StringVector(ts_ids),
                                read_period=UtcPeriod(time(0), time(5)))
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
    try:
        c = DtsClient(dtss.address)
        tsv_in = TsVector([TimeSeries(ts_id) for ts_id in timeseries])
        period = UtcPeriod(time(0), time(10))
        tsv = c.evaluate(tsv_in, period)
        assert tsv
    finally:
        dtss.stop()


def test_dts_client_heartbeat(dtss):
    dtss.start()

    try:
        c = DtsClient(dtss.address)
        heartbeat = create_heartbeat_request('test')
        tsv_in = TsVector([TimeSeries(heartbeat)])
        period = UtcPeriod(time(0), time(10))
        tsv = c.evaluate(tsv_in, period)
        assert tsv
        tsiv = c.find(heartbeat)
        assert tsiv[0].name == 'heartbeat: test'



    finally:
        dtss.stop()
