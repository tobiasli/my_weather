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
from weather.service.dtss_host import create_heartbeat_request

heartbeat = TimeSeries(create_heartbeat_request('static_plot'))

sys.path.append(os.environ['CONFIG_DIRECTORY'])
from netatmo_config import login

# Get measurements form domain:
domain = NetatmoDomain(**login)
device = 'Stua'
module = ''
plot_data = [
    {'data': domain.get_measurement(device_name=device, data_type=types.temperature.name, module_name=module),
     'color': '#E64C3E'},  # red
    {'data': domain.get_measurement(device_name=device, data_type=types.co2.name, module_name=module),
     'color': '#B0CA55'},  # green
    {'data': domain.get_measurement(device_name=device, data_type=types.humidity.name, module_name=module),
     'color': '#0F2933'},  # dark green
]
# ('Pressure', 'mbar', point_fx.POINT_INSTANT_VALUE, '#33120F'),  # brown
# ('Noise', 'db', point_fx.POINT_INSTANT_VALUE, '#E39C30'),  # yellow
# ('Rain', 'mm', point_fx.POINT_INSTANT_VALUE, '#448098'),  # light blue
# ('WindStrength', 'km / h', point_fx.POINT_INSTANT_VALUE, '#8816AB'),  # purple

# Get timeseries from measurements:
client = DtsClient('localhost:20001')
tsv = TsVector([meas['data'].time_series for meas in plot_data])
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


fig = figure(title=f'Demo plot {cal.to_string(now)}', height=400, width=800, x_axis_type='datetime')
fig.yaxis.visible = False
fig.xaxis.formatter = DatetimeTickFormatter(
    months=["%Y %b"],
    days=["%F %H:%M"],
    hours=["%a %H:%M"],
    minutes=["%H:%M"]
)
axis_switch = ['left', 'right']
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

for ts, variable in zip(data, plot_data):
    x, y = get_xy(ts)
    fig.line(x=x, y=y,
             color=variable['color'],
             legend=variable['data'].data_type.name,
             y_range_name=variable['data'].data_type.name_lower)
    fig.extra_y_ranges[variable['data'].data_type.name_lower].start = min(y) - 0.1 * (max(y) - min(y))
    fig.extra_y_ranges[variable['data'].data_type.name_lower].end = max(y) + 0.1 * (max(y) - min(y))

output_file(NamedTemporaryFile(prefix='netatmo_demo_plot_', suffix='.html').name)
show(fig)
