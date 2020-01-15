"""Script for starting the data collection service that collects the entire Netatmo history once and then stops.
It contains an extra layer of rate limiters to prevent being kicked out of the Netatmo API."""

import os
import sys
import socket
import logging
from logging.handlers import TimedRotatingFileHandler

import shyft.time_series as st

from weather.service.data_collection_task import DataCollectionTask, DataCollectionPeriodAbsolute
from weather.service.dtss_host import DtssHostEnvironmentVariablesConfig
from weather.data_sources.netatmo.domain import NetatmoDomain
from weather.data_sources.netatmo.repository import NetatmoEncryptedEnvVarConfig
from weather.data_sources.netatmo.identifiers import create_ts_netatmo


# Get password and salt for decrypting environment variables.
env_pass = sys.argv[1]
env_salt = sys.argv[2]

netatmo_config = NetatmoEncryptedEnvVarConfig(
    username_var='NETATMO_USER',
    password_var='NETATMO_PASS',
    client_id_var='NETATMO_ID',
    client_secret_var='NETATMO_SECRET',
    password=env_pass,
    salt=env_salt,
)

dtss_config = DtssHostEnvironmentVariablesConfig(
    port_num_var='DTSS_PORT_NUM',
    container_directory_var='DTSS_CONTAINER_DIR',
    log_directory_var='DTSS_LOG_DIR',
    data_collection_repositories=[]
)

if __name__ == '__main__':
    # Initiate log
    # noinspection PyArgumentList
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
        handlers=[
            logging.StreamHandler(),
            TimedRotatingFileHandler(filename=os.path.join(dtss_config.log_directory, 'data_collection'),
                                     when="d",
                                     interval=1,
                                     backupCount=10)
        ])


    # Get the timeseries we want this instance to read:
    domain = NetatmoDomain(
        username=netatmo_config.username,
        password=netatmo_config.password,
        client_id=netatmo_config.client_id,
        client_secret=netatmo_config.client_secret)

    # Create a list of ts_ids to read and a corresponding list of ts_ids to store.:
    measurements = [measurement for source in domain.data_source_list for measurement in source.measurements]
    read_timeseries = [create_ts_netatmo(measurement) for measurement in measurements]
    store_ts_ids = [measurement.ts_id for measurement in measurements]

    # Initialize netatmo collection:
    read_dtss_address = f'{socket.gethostname()}:{os.environ["DTSS_PORT_NUM"]}'

    cal = st.Calendar('Europe/Oslo')

    netatmo_complete = DataCollectionTask(
            task_name='netatmo_complete',
            read_dtss_address=read_dtss_address,
            read_ts=read_timeseries,
            read_period=DataCollectionPeriodAbsolute(
                start=cal.time(2019, 3, 1),  # For start of operation. No specified end means now.
                wait_time=24 * 3600),  # Every day.
            store_dtss_address=read_dtss_address,
            store_ts_ids=store_ts_ids
        )

    # Perform data collection:
    logging.info(f'Starting complete data collection for {netatmo_complete.name} for period {netatmo_complete.read_period}')
    netatmo_complete.collect_data()
    logging.info(f'Done with complete data collection for {netatmo_complete.name}')