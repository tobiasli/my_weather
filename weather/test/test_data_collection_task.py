"""Tests for the DataCollectionSerice classes."""
import socket
import pytest
import tempfile
import logging
from contextlib import closing

import shyft.time_series as st

from weather.service.dtss_host import DtssHost, DtsClient
from weather.service.data_collection_task import (
    DataCollectionPeriodRelative, DataCollectionPeriodAbsolute, DataCollectionTask
)
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


@pytest.fixture(scope="session")
def dtss() -> DtssHost:
    return DtssHost(port_num=find_free_port(),
                    container_directory=tempfile.mkdtemp(prefix='dtss_store_'),
                    data_collection_repositories=[(MockRepository1, dict()),
                                                  (MockRepository2, dict())])


def test_data_collection_period_relative():
    collection = DataCollectionPeriodRelative(
        start_offset=3600,
        wait_time=10,
        end_offset=3600 / 2
    )

    assert collection.period().start - st.utctime_now() - 3600 < 0.01
    assert collection.period().end - st.utctime_now() - 3600 / 2 < 0.01


def test_data_collection_period_absolute():
    cal = st.Calendar()
    collection = DataCollectionPeriodAbsolute(
        start=cal.time(2019, 1, 1),
        wait_time=10,
        end=cal.time(2019, 2, 1)
    )

    assert collection.period() == st.UtcPeriod(cal.time(2019, 1, 1), cal.time(2019, 2, 1))


def test_read_and_store(dtss):
    dtss.start()
    client = DtsClient(dtss.address)
    try:
        collection = DataCollectionTask(
            task_name='coll_test_serv',
            read_dtss_address=dtss.address,
            read_ts=[st.TimeSeries('mock1://test/1'), st.TimeSeries('mock2://test/24')],
            # Ask for data from two different repositories.
            read_period=DataCollectionPeriodAbsolute(start=0, end=3600, wait_time=0.5),
            store_dtss_address=dtss.address,
            store_ts_ids=['shyft://mock1/1/1', 'shyft://mock2/2/24'])

        collection.collect_data()

        data = client.evaluate(
            st.TsVector([st.TimeSeries(name) for name in ['shyft://mock1/1/1', 'shyft://mock2/2/24']]),
            st.UtcPeriod(0, 7200))

        # Length of data should match seconds of 2 hours minus 10 minutes == 6600 s
        assert len(data[0].values.to_numpy()) == 3600
        assert data[0].time_axis.time_points_double[0] == 0
        assert data[0].time_axis.time_points_double[-1] == 3600

        # Define new read period to resemble another query.
        collection.read_period = DataCollectionPeriodAbsolute(start=3600, end=7200, wait_time=0.5)

        collection.collect_data()

        # Collect the rest of the data.
        data = client.evaluate(
            st.TsVector([st.TimeSeries(name) for name in ['shyft://mock1/1/1', 'shyft://mock2/2/24']]),
            st.UtcPeriod(0, 7200))

        # Length of data should match seconds of 2 hours == 7200 s
        assert len(data[0].values.to_numpy()) == 7200
        assert data[0].time_axis.time_points_double[0] == 0
        assert data[0].time_axis.time_points_double[-1] == 7200

    finally:
        dtss.stop()
