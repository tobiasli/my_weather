"""This script file starts the DtssHost service on a port specified in ENV."""
import os
import sys
import socket
import time
from weather.service.dtss import DtssHost, create_heartbeat_request
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

# Start DtssHost:
host.start()

# Stay alive loop, with heartbeat every minute.
try:
    client = DtsClient(host.address)
    while True:
        a = client.find(create_heartbeat_request('start_dtss.py'))
        if not a:
            break
        time.sleep(60)
finally:
    del client
    host.stop()
