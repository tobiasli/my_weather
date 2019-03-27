"""This script file starts the DtssHost service on a port specified in ENV."""
import os
import sys
import socket
import time
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
try:
    """Test an actual dtss call towards the Netatmo api."""
    from shyft.api import DtsClient, TimeSeries, TsVector, UtcPeriod, Calendar
    from weather.data_collection.netatmo_identifiers import create_ts_id

    ts = TimeSeries(create_ts_id(device_name='Stua', module_name='', data_type='Temperature'))
    tsv = TsVector([ts])
    c = DtsClient('localhost:20001')
    c.evaluate(tsv, UtcPeriod(Calendar().time(2019, 3, 1), Calendar().time(2019, 3, 8)))
    #
    # while True:
    #     time.sleep(1)
finally:
    host.stop()
