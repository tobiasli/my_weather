"""Script for starting the data collection service."""

import os
import sys
import socket
import time
import logging

from weather.service.data_collection_service import DataCollectionService, DataCollectionPeriod, \
    DataCollectionServiceSet
from weather.data_sources.netatmo import get_netatmo_domain

# Get configs from config directory:
if not 'CONFIG_DIRECTORY' in os.environ:
    raise EnvironmentError('Cannot find path to app authentication codes.')
sys.path.append(os.environ['CONFIG_DIRECTORY'])
from dtss_config import configs

# Get config for current machine:
if socket.gethostname() not in configs:
    raise Exception(f"Can't find configuration for machine {socket.gethostname()}")
DTSS_CONFIG = configs[socket.gethostname()]

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler()
    ])

# Get the timeseries we want this instance to read:
domain = get_netatmo_domain()

# Get all timeseries:
measurements = [measurement for source in domain.data_source_list for measurement in source.measurements]
read_timeseries = [measurement.time_series for measurement in measurements]
store_timeseries = [measurement.ts_store_id for measurement in measurements]

# Initialize DataCollectionServices:
services = DataCollectionServiceSet()

# Initialize netatmo collection:
read_dtss_address = f'{socket.gethostname()}:{DTSS_CONFIG["dtss_port_num"]}'

services.add_service(DataCollectionService(service_name='netatmo_short',
                                           read_dtss_address=read_dtss_address,
                                           read_ts=read_timeseries,
                                           read_period=DataCollectionPeriod(
                                               start_offset=24 * 3600 * 2,  # Two days.
                                               wait_time=30),  # Every 30 seconds
                                           store_dtss_address=read_dtss_address,
                                           store_ts_ids=store_timeseries
                                           ))
services.add_service(DataCollectionService(service_name='netatmo_long',
                                           read_dtss_address=read_dtss_address,
                                           read_ts=read_timeseries,
                                           read_period=DataCollectionPeriod(
                                               start_offset=365*24*3600,  # One year.
                                               wait_time=24*3600),  # Every day.
                                           store_dtss_address=read_dtss_address,
                                           store_ts_ids=store_timeseries
                                           ))

services.start()
logging.info('Services started')
print('Services started')
try:
    time.sleep(24 * 3600 * 3)
finally:
    services.stop()
