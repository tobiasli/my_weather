"""This script file starts the DtssHost service on a port specified in ENV."""
import os
import sys
import socket
import time
from weather.service.dtss_host import DtssHost
from weather.data_sources.heartbeat import create_heartbeat_request
from weather.service.service_manager import Service, ServiceManager
from shyft.api import DtsClient

# Get configs from config directory:
if not 'CONFIG_DIRECTORY' in os.environ:
    raise EnvironmentError('Cannot find path to app authentication codes.')
sys.path.append(os.environ['CONFIG_DIRECTORY'])
from dtss_config import configs

# Get config for current machine:
if socket.gethostname() not in configs:
    raise Exception(f"Can't find configuration for machine {socket.gethostname()}")
DTSS_CONFIG = configs[socket.gethostname()]

heartbeat_interval = 60

if __name__ == '__main__':
    # Initialize DtssHost:
    host = DtssHost(**DTSS_CONFIG)
    host.start()

    # Create a ServiceManager to monitor the health of the Dtss every 30 minutes.
    # health_check_action performs a dummy find-request and expects a non-empty response.
    sm = ServiceManager(services=[
        Service(name='dtss_maintainer',
                health_check_action=
                lambda: bool(DtsClient(host.address).find(create_heartbeat_request(f'Startup script check every {heartbeat_interval} s'))),
                restart_action=host.restart
                )], health_check_frequency=60*30)

    sm.start_services()
    try:
        while True:
            time.sleep(5)
    finally:
        sm.stop_services()
        host.stop()
        del host
        del sm
