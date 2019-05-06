"""Script for starting the data collection service."""

import os
import sys
import socket
import time

from weather.service.data_collection_service import DataCollectionService, DataCollectionPeriod
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

# Get the timeseries we want this instance to read:
domain = get_netatmo_domain()

# Get all timeseries:
measurements = [measurement for source in domain.data_source_list for measurement in source.measurements]
read_timeseries = [measurement.time_series for measurement in measurements]
store_timeseries = [measurement.ts_store_id for measurement in measurements]


# Initialize DataCollectionService:

# Initialize netatmo collection:
netatmo = DataCollectionService(service_name='netatmo_collection',
                                read_dtss_address=f'{socket.gethostname()}:{DTSS_CONFIG["dtss_port_num"]}',
                                read_ts=read_timeseries,
                                read_period=DataCollectionPeriod(start_offset=24*3600*2, wait_time=5*60),
                                store_dtss_address=f'{socket.gethostname()}:{DTSS_CONFIG["dtss_port_num"]}',  # Store to same as read.
                                store_ts_ids=store_timeseries
                                )
netatmo.start()



time.sleep(24*3600*3)

netatmo.stop()