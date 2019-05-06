"""Tests for the DataCollectionSerice classes."""
import os
import sys
import socket
import pytest
import time
import logging
from contextlib import closing

from weather.service.dtss_host import DtssHost, DtsClient
from weather.service.data_collection_service import DataCollectionPeriod, DataCollectionService
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
    os.mkdir(os.path.join(dtss.container_directory, 'test'))  # Create container for test data.
    dtss.start()
    try:
        collection = DataCollectionService(
            service_name='coll_test_serv',
            read_dtss_address=dtss.address,
            read_ts_ids=['mock1://test/1'],
            read_period=DataCollectionPeriod(start_offset=3600*2, end_offset=60*10, wait_time=0.5),
            store_dtss_address=dtss.address,
            store_ts_ids=['shyft://test/1'])
        collection.start()

        # Wait for an amount of time. Should complete 4 calls.
        time.sleep(1)

        collection.stop()

        # Verify that TimeSeries now exists in dtss.store:
        client = DtsClient(dtss.address)
        tsi = client.find('shyft://test/\d')
        assert len(tsi) == 1

    finally:
        dtss.stop()

import threading
import time

def test_thread():

    def something():
        t = threading.current_thread()
        while getattr(t, 'continue_loop'):
            print('hello')
            time.sleep(0.25)
        print('done')

    t = threading.Thread(target=something)
    t.continue_loop = True

    t.start()
    time.sleep(2)
    t.continue_loop = False
