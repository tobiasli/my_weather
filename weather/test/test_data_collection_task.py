"""Tests for the DataCollectionSerice classes."""
import os
import sys
import socket
import pytest
import time
import logging
from contextlib import closing

import shyft.time_series as st

from weather.service.dtss_host import DtssHost, DtsClient
from weather.service.data_collection_task import DataCollectionPeriod, DataCollectionTask
from weather.service.service_manager import Service

# Get credentials:
if not 'CONFIG_DIRECTORY' in os.environ:
    raise EnvironmentError('Cannot find path to app authentication codes.')

sys.path.append(os.environ['CONFIG_DIRECTORY'])

from dtss_config import test_configs

if socket.gethostname() not in test_configs:
    raise Exception(f"Can't find configuration for machine {socket.gethostname()}")
DTSS_TEST_CONFIG = test_configs[socket.gethostname()]

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
    return DtssHost(**DTSS_TEST_CONFIG, dtss_port_num=find_free_port())


def test_read_and_store(dtss):
    dtss.start()
    try:
        collection = DataCollectionTask(
            task_name='coll_test_serv',
            read_dtss_address=dtss.address,
            read_ts=[st.TimeSeries('mock1://test/1'), st.TimeSeries('mock2://test/24')],  # Ask for data from two different repositories.
            read_period=DataCollectionPeriod(start_offset=3600*2, end_offset=60*10, wait_time=0.5),
            store_dtss_address=dtss.address,
            store_ts_ids=['shyft://mock1/1/1', 'shyft://mock2/2/24'])

        collection.collect_data()

        # Verify that TimeSeries now exists in dtss.store:
        client = DtsClient(dtss.address)
        tsiv = client.find(r'shyft://mock1/\d/\d+')
        assert len(tsiv) == 1
        tsiv = client.find(r'shyft://mock2/\d/\d+')
        assert len(tsiv) == 1

    finally:
        dtss.stop()