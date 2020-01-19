"""This script file starts the DtssHost service on a port specified in ENV."""
import sys
import os
import time
import logging
from logging.handlers import TimedRotatingFileHandler

from weather.data_sources.netatmo.repository import NetatmoRepository, NetatmoEncryptedEnvVarConfig
from weather.service.dtss_host import DtssHost, DtssHostEnvironmentVariablesConfig
from weather.data_sources.heartbeat import create_heartbeat_request
from weather.service.service_manager import Service, ServiceManager
from shyft.time_series import DtsClient

# Get password and salt for decrypting environment variables.
env_pass = sys.argv[1]
env_salt = sys.argv[2]

api_limits = {
        # name of limit: {max calls within span, timespan in sec, seconds wait when limit is met.
        '10 seconds 50 actions': {'action_limit': 50, 'timespan': 10, 'wait_time': 1},  # Max netatmo api calls 50 pr 10 seconds.
        '1 hour 500 actions': {'action_limit': 500, 'timespan': 3600, 'wait_time': 5*60},  # Max netatmo api calls 500 pr hour.
    }

netatmo_config = NetatmoEncryptedEnvVarConfig(
    username_var='NETATMO_USER',
    password_var='NETATMO_PASS',
    client_id_var='NETATMO_ID',
    client_secret_var='NETATMO_SECRET',
    password=env_pass,
    salt=env_salt,
    rate_limiters=api_limits
)


heartbeat_interval = 60 * 30
dtss_config = DtssHostEnvironmentVariablesConfig(
    port_num_var='DTSS_PORT_NUM',
    container_directory_var='DTSS_CONTAINER_DIR',
    log_directory_var='DTSS_LOG_DIR',
    data_collection_repositories=[
        (NetatmoRepository, netatmo_config)
    ]
)

if __name__ == '__main__':
    # Initialize log:
    # noinspection PyArgumentList
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
        handlers=[
            logging.StreamHandler(),
            TimedRotatingFileHandler(filename=os.path.join(dtss_config.log_directory, 'dtss'),
                                     when="d",
                                     interval=1,
                                     backupCount=10)
        ])

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
