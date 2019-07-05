"""This script file starts the DtssHost service on a port specified in ENV."""
import os
import sys
import socket
import time
from weather.service.dtss_host import DtssHost, create_heartbeat_request
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

# Initialize DtssHost:
host = DtssHost(**DTSS_CONFIG)

heartbeat_interval = 60

if __name__ == '__main__':
    # Start DtssHost:
    host.start()

    # Stay alive loop, with heartbeat at regular intervals.
    try:
        client = DtsClient(host.address)
        while True:
            response = client.find(create_heartbeat_request(f'Startup script check every {heartbeat_interval} s'))
            if not response:
                host.stop()
                del host
                host = DtssHost(**DTSS_CONFIG)
                host.start()
            time.sleep(heartbeat_interval)
    finally:
        del client
        host.stop()
