"""This script file starts the DtssHost service on a port specified in ENV."""
import os
import sys
import socket
from weather.service.dtss import DtssHost

# Get credentials:
if not 'CONFIG_DIRECTORY' in os.environ:
    raise EnvironmentError('Cannot find path to app authentication codes.')

sys.path.append(os.environ['CONFIG_DIRECTORY'])

from dtss_config import configs

if socket.gethostname() not in configs:
    raise Exception(f"Can't find configuration for machine {socket.gethostname()}")
DTSS_CONFIG = configs[socket.gethostname()]

host = DtssHost(**DTSS_CONFIG)

host.start()

