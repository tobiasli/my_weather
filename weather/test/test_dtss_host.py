import logging
import socket
import pytest
import tempfile
import os

from contextlib import closing

import numpy as np

from shyft.time_series import UtcPeriod, StringVector, TsVector, DtsClient, TimeSeries, time
import shyft.time_series as st

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


def test_dts_client_store():


    dtss = st.DtsServer()
    dtss.set_listening_port(34386)
    path = tempfile.mkdtemp(prefix='dtss_store_')
    dtss.set_container('test', os.path.join(path, 'test'))
    dtss.set_container('foo', os.path.join(path, 'foo'))
    dtss.start_async()

    try:
        c = DtsClient('localhost:34386')

        # Store a timeseries
        ts = st.TimeSeries(st.TimeAxis(time(0), 1, 3), [1, 2, 3], st.POINT_AVERAGE_VALUE)
        names = ['shyft://test/test1', 'shyft://foo/bar']
        store = st.TsVector([st.TimeSeries(name, ts) for name in names])
        c.store_ts(store)

        # Store an extension of an existing timeseries:
        ts = st.TimeSeries(st.TimeAxis(time(3), 1, 3), [4, 5, 6], st.POINT_AVERAGE_VALUE)
        store = st.TsVector([st.TimeSeries(names[0], ts)])
        c.store_ts(store, overwrite_on_write=False)

        # Check that the timeseries has the expected length of union(ts1, ts2), and single write to other container:
        data = c.evaluate(st.TsVector([st.TimeSeries(name) for name in names]), st.UtcPeriod(time(-1), time(7)))
        assert np.all(data[0].values.to_numpy() == np.array([1, 2, 3, 4, 5, 6]))
        assert np.all(data[1].values.to_numpy() == np.array([1, 2, 3]))

    finally:
        dtss.stop_web_api()
        dtss.clear()
        del dtss
