"""This script creates a simple static plot of data from the DtssHost via a DtsClient."""
import os
import sys
import logging
import socket

import shyft.time_series as st

from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.plotting import figure, ColumnDataSource

from visual.utils import bokeh_time_from_timestamp, get_xy
from weather.data_sources.netatmo.domain import NetatmoDomain
from weather.data_sources.netatmo.repository import NetatmoEncryptedEnvVarConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler()
    ])

env_pass = sys.argv[1]
env_salt = sys.argv[2]

config = NetatmoEncryptedEnvVarConfig(
    username_var='NETATMO_USER',
    password_var='NETATMO_PASS',
    client_id_var='NETATMO_ID',
    client_secret_var='NETATMO_SECRET',
    password=env_pass,
    salt=env_salt,
)

# Get measurements form domain:
domain = NetatmoDomain(
    username=config.username,
    password=config.password,
    client_id=config.client_id,
    client_secret=config.client_secret
)

client = st.DtsClient(f'{os.environ["DTSS_SERVER"]}:{os.environ["DTSS_PORT_NUM"]}')

cal = st.Calendar('Europe/Oslo')


def dashboard(doc):
    now_source = ColumnDataSource({'temp': [], 'time': [], 'color': []})
    period = st.UtcPeriod(st.utctime_now() - st.Calendar.DAY, st.utctime_now())
    fig = figure(
        title='Show current outdoor temperature!',
        x_axis_type='datetime',
        sizing_mode='scale_both',
        #     y_range=[15, 35],
    )
    fig.circle(source=now_source, x='time', y='temp', color='color', size=10)

    # fig.x_range = Range1d(bokeh_time_from_timestamp(cal, period.start), bokeh_time_from_timestamp(cal, period.end))

    def update():
        # Update dashboard data.
        period = st.UtcPeriod(st.utctime_now() - st.Calendar.DAY, st.utctime_now())
        data = client.evaluate(st.TsVector(
            [
                domain.get_measurement(
                    station_name='Eftasåsen',
                    module_name='Ute',
                    data_type='Temperature'
                ).time_series
            ]),
            period)
        time, temp = get_xy(cal, data[0])

        new = {'temp': [temp[-1]],
               'time': [bokeh_time_from_timestamp(cal, st.utctime_now())],
               'color': ['blue']
               }
        now_source.stream(new, 100)
        # fig.x_range = Range1d(bokeh_time_from_timestamp(cal, period.start), bokeh_time_from_timestamp(cal, period.end))

    update()

    doc.add_periodic_callback(update, 1000)
    doc.title = "Now with live updating!"
    doc.add_root(fig)


apps = {'/test': Application(FunctionHandler(dashboard))}

port = 5000
server = Server(apps,
                port=port,
                # address=socket.gethostbyname(socket.gethostname()),
                # host=f'localhost:{port}',
                allow_websocket_origin=[
                    f'localhost:{port}',  # Localhost by name
                    f'{socket.gethostbyname(socket.gethostname())}:{port}'  # Localhost by IP
                ])
server.io_loop.start()
