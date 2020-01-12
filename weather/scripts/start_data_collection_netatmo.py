"""Script for starting the data collection service."""

import os
import sys
import socket
import logging
from logging.handlers import TimedRotatingFileHandler

from weather.service.data_collection_task import DataCollectionTask, DataCollectionPeriod
from weather.service.service_manager import Service, ServiceManager
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

    # Initialize DataCollectionServices:

    # Initialize netatmo collection:
    read_dtss_address = f'{socket.gethostname()}:{os.environ["DTSS_PORT_NUM"]}'

    netatmo_short = DataCollectionTask(
        task_name='netatmo_short',
        read_dtss_address=read_dtss_address,
        read_ts=read_timeseries,
        read_period=DataCollectionPeriod(
            start_offset=24 * 3600 * 2,  # Two days.
            wait_time=5 * 60),  # Every 5 minutes
        store_dtss_address=read_dtss_address,
        store_ts_ids=store_ts_ids
    )
    netatmo_long = DataCollectionTask(
            task_name='netatmo_long',
            read_dtss_address=read_dtss_address,
            read_ts=read_timeseries,
            read_period=DataCollectionPeriod(
                start_offset=365 * 24 * 3600,  # One year.
                wait_time=24 * 3600),  # Every day.
            store_dtss_address=read_dtss_address,
            store_ts_ids=store_ts_ids
        )

    services = ServiceManager()
    services.add_service(
        Service(name=netatmo_short.name,
                task=netatmo_short.collect_data,
                task_interval=netatmo_short.read_period.wait_time,
                health_check_action=netatmo_short.health_check,
                restart_action=netatmo_short.restart_clients)
    )
    services.add_service(
        Service(name=netatmo_long.name,
                task=netatmo_long.collect_data,
                task_interval=netatmo_long.read_period.wait_time,
                health_check_action=netatmo_long.health_check,
                restart_action=netatmo_long.restart_clients)
    )

    services.start_services()
