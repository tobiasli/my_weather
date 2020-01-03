"""This script file starts the DtssHost service on a port specified in ENV."""
import os
import sys
import socket
import time
import logging

from weather.data_sources.netatmo.netatmo import NetatmoRepository
from weather.service.dtss_host import DtssHost, DtssHostEnvironmentVariablesConfig
from weather.data_sources.heartbeat import create_heartbeat_request
from weather.service.service_manager import Service, ServiceManager
from shyft.api import DtsClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler()
    ])

if not 'CONFIG_DIRECTORY' in os.environ:
    raise EnvironmentError('Cannot find path to app authentication codes.')
sys.path.append(os.environ['CONFIG_DIRECTORY'])
from netatmo_config import config as netatmo_config


heartbeat_interval = 60 * 30
dtss_config = DtssHostEnvironmentVariablesConfig(
    port_num_var='DTSS_PORT_NUM',
    container_directory_var='DTSS_CONTAINER_DIR',
    data_collection_repositories=[
        (NetatmoRepository, netatmo_config)
    ]
)

if __name__ == '__main__':
    # Initialize DtssHost:
    host = DtssHost(**dtss_config)
    host.start()
    # Create a ServiceManager to monitor the health of the Dtss every 30 minutes.
    # health_check_action performs a dummy find-request and expects a non-empty response.
    sm = ServiceManager(services=[
        Service(name='dtss_maintainer',
                health_check_action=
                lambda: bool(DtsClient(host.address).find(
                    create_heartbeat_request(f'Startup script check every {heartbeat_interval} s'))),
                restart_action=host.restart
                )], health_check_frequency=heartbeat_interval)

    sm.start_services()
    try:
        while True:
            time.sleep(5)
    finally:
        sm.stop_services()
        host.stop()
        del host
        del sm
