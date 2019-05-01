"""This script creates a simple static plot of data from the DtssHost via a DtsClient."""
import sys
import os
from tempfile import NamedTemporaryFile
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler()
    ])

from shyft.api import DtsClient, UtcPeriod, Calendar, TsVector, utctime_now, TimeSeries, point_interpretation_policy
from bokeh.plotting import figure, show, output_file
from bokeh.models import DatetimeTickFormatter, Range1d, LinearAxis
import numpy as np

from weather.data_collection.netatmo_domain import NetatmoDomain, types
from weather.service.dtss import create_heartbeat_request

heartbeat = TimeSeries(create_heartbeat_request('static_plot'))

sys.path.append(os.environ['CONFIG_DIRECTORY'])
from netatmo_config import login

# Get measurements form domain:
domain = NetatmoDomain(**login)
device = 'Stua'
module = ''
plot_info = [
    {'type': types.temperature, 'color': 'crimson', 'axis_side': 'left'},
    {'type': types.co2, 'color': 'black', 'axis_side': 'right'},
    {'type': types.humidity, 'color': 'forestgreen', 'axis_side': 'left'}
]

measurements = [
    domain.get_measurement(device_name=device, data_type=types.temperature.name, module_name=module),
    domain.get_measurement(device_name=device, data_type=types.co2.name, module_name=module),
    domain.get_measurement(device_name=device, data_type=types.humidity.name, module_name=module)
]

# Get timeseries from measurements:
client = DtsClient('localhost:20001')
tsv = TsVector([meas.time_series for meas in measurements])
cal = Calendar('Europe/Oslo')
now = utctime_now()
period = UtcPeriod(now - cal.DAY, now)
data = client.evaluate(tsv, period)


# Plotting:
def get_xy(ts: TimeSeries) -> np.array:
    """Method for extracting xy-data from TimeSeries"""
    if ts.point_interpretation() == point_interpretation_policy.POINT_INSTANT_VALUE:
        return [float(t * 1000 + cal.tz_info.base_offset()) for t in
                ts.time_axis.time_points_double[0:-1]], ts.values.to_numpy()
    elif ts.point_interpretation() == point_interpretation_policy.POINT_AVERAGE_VALUE:
        values = []
        time = []
        for v, t1, t2 in zip(ts.values, ts.time_axis.time_points_double[0:-1], ts.time_axis.time_points_double[1:]):
            time.append(float(t1 * 1000 + cal.tz_info.base_offset()))
            values.append(v)
            time.append(float(t2 * 1000 + cal.tz_info.base_offset()))
            values.append(v)
        return np.array(time), np.array(values)


fig = figure(title='Static Plot', height=400, width=800, x_axis_type='datetime')
fig.yaxis.visible = False
fig.xaxis.formatter = DatetimeTickFormatter(
    days=["%d.%m.%y"],
    months=["%d.%m.%y %H:%M"],
    hours=["%d.%m.%y %H:%M"],
    minutes=["%d.%m %H:%M"]
)
for variable in plot_info:
    fig.extra_y_ranges[variable['type'].name_lower] = Range1d()
    fig.add_layout(
        obj=LinearAxis(
            y_range_name=variable['type'].name_lower,
            axis_label=f'{variable["type"].name} [{variable["type"].unit}]',
            major_label_text_color=variable['color'],
            major_tick_line_color=variable['color'],
            minor_tick_line_color=variable['color'],
            axis_line_color=variable['color'],
            axis_label_text_color=variable['color']
        ),
        place=variable["axis_side"]
    )

for ts, variable in zip(data, plot_info):
    x, y = get_xy(ts)
    fig.line(x=x, y=y,
             color=variable['color'],
             legend=variable['type'].name,
             y_range_name=variable['type'].name_lower)
    fig.extra_y_ranges[variable['type'].name_lower].start = min(y)-0.1*(max(y)-min(y))
    fig.extra_y_ranges[variable['type'].name_lower].end = max(y)+0.1*(max(y)-min(y))

output_file(NamedTemporaryFile(prefix='netatmo_demo_plot_', suffix='.html').name)
show(fig)
