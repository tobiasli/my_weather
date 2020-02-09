"""This script creates a simple static plot of data from the DtssHost via a DtsClient."""
import sys
import os
from tempfile import NamedTemporaryFile
import logging
import socket

from shyft.time_series import DtsClient, UtcPeriod, Calendar, TsVector, utctime_now, TimeSeries
from bokeh.plotting import figure, show, output_file
from bokeh.models import DatetimeTickFormatter, Range1d, LinearAxis

from visual.utils import bokeh_time_from_timestamp, get_xy
from weather.data_sources.netatmo.domain import NetatmoDomain, types
from weather.data_sources.netatmo.repository import NetatmoEncryptedEnvVarConfig
from weather.data_sources.heartbeat import create_heartbeat_request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler()
    ])

heartbeat = TimeSeries(create_heartbeat_request('static_plot'))

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
station = 'Eftasåsen'
module = 'Stua'
plot_data = [
    {'data': domain.get_measurement(station_name=station, data_type=types.temperature.name, module_name=module),
     'color': '#E64C3E'},  # red
    {'data': domain.get_measurement(station_name=station, data_type=types.co2.name, module_name=module),
     'color': '#B0CA55'},  # green
    {'data': domain.get_measurement(station_name=station, data_type=types.humidity.name, module_name=module),
     'color': '#0F2933'},  # dark green
]
# ('Pressure', 'mbar', point_fx.POINT_INSTANT_VALUE, '#33120F'),  # brown
# ('Noise', 'db', point_fx.POINT_INSTANT_VALUE, '#E39C30'),  # yellow
# ('Rain', 'mm', point_fx.POINT_INSTANT_VALUE, '#448098'),  # light blue
# ('WindStrength', 'km / h', point_fx.POINT_INSTANT_VALUE, '#8816AB'),  # purple

# Get timeseries from measurements:
client = DtsClient(f'{os.environ["DTSS_SERVER"]}:{os.environ["DTSS_PORT_NUM"]}')
# client = DtsClient(f'{socket.gethostname()}:{os.environ["DTSS_PORT_NUM"]}')
tsv = TsVector([meas['data'].time_series for meas in plot_data])
cal = Calendar('Europe/Oslo')
epsilon = 0.1

now = utctime_now()
period = UtcPeriod(now - cal.DAY, now)
data = client.evaluate(tsv, period)

try:
    fig = figure(title=f'Demo plot {cal.to_string(now)}', height=400, width=1400, x_axis_type='datetime')
    fig.line([1, 2, 3, 4, 5], [5, 3, 4, 2, 1])

    fig.yaxis.visible = False
    fig.xaxis.formatter = DatetimeTickFormatter(
        months=["%Y %b"],
        days=["%F %H:%M"],
        hours=["%a %H:%M"],
        minutes=["%H:%M"]
    )

    axis_switch = ['left', 'right']

    # Create axes:
    for variable in plot_data:
        axis_side = axis_switch[0]
        axis_switch.reverse()
        fig.extra_y_ranges[variable['data'].data_type.name_lower] = Range1d()
        fig.add_layout(
            obj=LinearAxis(
                y_range_name=variable['data'].data_type.name_lower,
                axis_label=f"{variable['data'].data_type.name} [{variable['data'].data_type.unit}]",
                major_label_text_color=variable['color'],
                major_tick_line_color=variable['color'],
                minor_tick_line_color=variable['color'],
                axis_line_color=variable['color'],
                axis_label_text_color=variable['color']
            ),
            place=axis_side
        )

    # Plot data:
    x_ranges = []
    for ts, variable in zip(data, plot_data):
        x, y = get_xy(cal, ts)
        x_ranges.extend([min(x), max(x)])
        fig.line(x=x, y=y,
                 color=variable['color'],
                 legend=variable['data'].data_type.name,
                 y_range_name=variable['data'].data_type.name_lower)
        fig.extra_y_ranges[variable['data'].data_type.name_lower].start = min(y) - epsilon * (max(y) - min(y))
        fig.extra_y_ranges[variable['data'].data_type.name_lower].end = max(y) + epsilon * (max(y) - min(y))

    fig.x_range = Range1d(bokeh_time_from_timestamp(cal, period.start), bokeh_time_from_timestamp(cal, period.end))

    output_file(NamedTemporaryFile(prefix='netatmo_demo_plot_', suffix='.html').name)
    show(fig)
finally:
    del client
