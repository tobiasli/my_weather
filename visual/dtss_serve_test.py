import typing as ty
import random
from abc import abstractmethod

import numpy as np

import shyft.time_series as st

from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.plotting import figure, ColumnDataSource
from bokeh.models import Button, BoxAnnotation, Span, Range1d, Widget, Annotation
from bokeh.layouts import column, row

"""This script creates a simple static plot of data from the DtssHost via a DtsClient."""
import sys
import os
from tempfile import NamedTemporaryFile
import logging
import socket

from shyft.time_series import DtsClient, UtcPeriod, Calendar, TsVector, utctime_now, TimeSeries, \
    point_interpretation_policy
from bokeh.plotting import figure, show, output_file
from bokeh.models import DatetimeTickFormatter, Range1d, LinearAxis
import numpy as np

from weather.data_sources.netatmo.domain import NetatmoDomain, types
from weather.data_sources.netatmo.repository import NetatmoEncryptedEnvVarConfig
from weather.data_sources.heartbeat import create_heartbeat_request

Number = ty.Union[int, float]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler()
    ])


def bokeh_time_from_timestamp(cal: Calendar, timestamp) -> float:
    """Create a localized ms timestamp from a shyft utc timestamp."""
    return float((timestamp + cal.tz_info.base_offset()) * 1000)


def get_xy(cal: st.Calendar, ts: TimeSeries) -> np.array:
    """Method for extracting xy-data from TimeSeries"""
    if ts.point_interpretation() == point_interpretation_policy.POINT_INSTANT_VALUE:
        return [bokeh_time_from_timestamp(cal, t) for t in
                ts.time_axis.time_points_double[0:-1]], ts.values.to_numpy()
    elif ts.point_interpretation() == point_interpretation_policy.POINT_AVERAGE_VALUE:
        values = []
        time = []
        for v, t1, t2 in zip(ts.values, ts.time_axis.time_points_double[0:-1], ts.time_axis.time_points_double[1:]):
            time.append(bokeh_time_from_timestamp(cal, t1))
            values.append(v)
            time.append(bokeh_time_from_timestamp(cal, t2))
            values.append(v)
        return np.array(time), np.array(values)


class DtssData:
    def __init__(self, client: st.DtsClient, time_series: st.TimeSeries):
        self.client = client
        self.time_series = time_series

    def get_data(self, period: st.UtcPeriod) -> st.TimeSeries:
        return self.client.evaluate(st.TsVector([self.time_series]), period)[0]


class DashboardBase:
    def __init__(self,
                 source: ColumnDataSource,
                 source_key: str
                 ) -> None:
        self.source = source
        self.source_value_key = source_key

    @property
    @abstractmethod
    def layout(self) -> Widget:
        pass


class DashboardIcon(DashboardBase):
    _color_lookup = {
        'blue': 'primary',
        'red': 'danger',
        'yellow': 'warning',
        'green': 'success'
    }

    def __init__(self,
                 source: ColumnDataSource,
                 source_value_key: str,
                 value_formatter: ty.Callable[[ColumnDataSource, str], Number] = None,
                 color_formatter: ty.Callable[[Number], str] = lambda value: 'blue',
                 text_formatter: ty.Callable[[Number], str] = lambda value: str(value),
                 **kwargs) -> None:
        self.value_formatter = value_formatter or self._default_value_formatter
        self.color_selector = color_formatter
        self.text_formatter = text_formatter
        super(DashboardIcon, self).__init__(source, source_value_key)

        self.button = Button(label=self.text_formatter(np.nan),
                             button_type=self._get_color_type(self.color_selector(np.nan)),
                             **kwargs)

    def _get_color_type(self, color: str) -> str:
        return self._color_lookup[color]

    @staticmethod
    def _default_value_formatter(source: ColumnDataSource, source_key: str) -> Number:
        return source.data[source_key][0][-1]

    def update(self) -> None:
        value = self.value_formatter(self.source, self.source_value_key)
        self.button.label = self.text_formatter(value)
        self.button.button_type = self._get_color_type(self.color_selector(value))

    @property
    def layout(self):
        return self.button


class DashboardTimeSeriesMini(DashboardBase):
    def __init__(self,
                 source: ColumnDataSource,
                 source_value_key: str,
                 source_time_key: str,
                 source_color_key: str,
                 additional_annotations: ty.Sequence[Annotation],
                 minimum_range: ty.Sequence[Number],
                 axis_color: str = 'lightgrey',
                 **kwargs) -> None:
        self.source_time_key = source_time_key
        self.source_color_key = source_color_key
        self.additional_annotations = additional_annotations
        self.minimum_range = minimum_range or list()
        super(DashboardTimeSeriesMini, self).__init__(source, source_value_key)

        self.fig = figure(
            x_axis_type='datetime',
            **kwargs
        )
        self.fig.multi_line(source=self.source,
                      xs=self.source_time_key,
                      ys=self.source_value_key,
                      color=self.source_color_key,
                      # size=2,
                      )
        for annotation in self.additional_annotations:
            self.fig.add_layout(annotation)
        if self.minimum_range:
            self.fig.y_range = Range1d(*self.minimum_range)
        self.fig.toolbar.logo = None
        self.fig.toolbar_location = None
        self.fig.xaxis.axis_line_color = axis_color
        self.fig.xaxis.major_tick_line_color = axis_color
        self.fig.xaxis.minor_tick_line_color = axis_color
        self.fig.yaxis.axis_line_color = axis_color
        self.fig.yaxis.major_tick_line_color = axis_color
        self.fig.yaxis.minor_tick_line_color = axis_color

    def update(self) -> None:
        maximum = np.nanmax(self.source.data[self.source_value_key])
        minimum = np.nanmin(self.source.data[self.source_value_key])

        if self.minimum_range:
            upper = np.nanmax([np.ceil(maximum + 0.2 * abs(maximum-minimum)), self.minimum_range[1]])
            lower = np.nanmin([np.floor(minimum - 0.2 * abs(maximum-minimum)), self.minimum_range[0]])

            self.fig.y_range.start = lower
            self.fig.y_range.end = upper

        # self.fig.x_range.start = np.nanmin(self.source.data[self.source_time_key])
        # self.fig.x_range.end = np.nanmax(self.source.data[self.source_time_key]) + 2*3600*1000  # Pad with 2 hours at end of series.

    @property
    def layout(self) -> Widget:
        return self.fig


class TestApp:

    def __init__(self) -> None:
        self.cal = Calendar('Europe/Oslo')
        self.client = client = DtsClient(f'{os.environ["DTSS_SERVER"]}:{os.environ["DTSS_PORT_NUM"]}')
        self.hist_length = self.cal.DAY * 2

        env_pass = sys.argv[2]
        env_salt = sys.argv[3]

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
        self.temp_indoor = DtssData(client=client,
                                    time_series=domain.get_measurement(station_name=station, data_type=types.temperature.name,
                                                                module_name=module).time_series)
        self.temp_outdoor = DtssData(client=client,
                                    time_series=domain.get_measurement(station_name=station, data_type=types.temperature.name,
                                                                module_name='Ute').time_series)
        self.co2 = DtssData(client=client,
                            time_series=domain.get_measurement(station_name=station, data_type=types.co2.name,
                                                               module_name=module).time_series)

        self.temp_indoor_source = ColumnDataSource({'time': [], 'value': [], 'color': []})
        self.temp_outdoor_source = ColumnDataSource({'time': [], 'value': [], 'color': []})
        self.co2_source = ColumnDataSource({'time': [], 'value': [], 'color': []})

        def temp_icon_color(value: Number) -> str:
            if value > 0:
                return 'red'
            else:
                return 'blue'

        self.temp_indoor_icon = DashboardIcon(
            source=self.temp_indoor_source,
            source_value_key='value',
            height=100,
            width=100,
            color_formatter=temp_icon_color,
            text_formatter=lambda value: f'\n{value:0.2f} °C'
        )

        self.temp_indoor_ts = DashboardTimeSeriesMini(
            source=self.temp_indoor_source,
            source_value_key='value',
            source_time_key='time',
            source_color_key='color',
            height=125,
            width=200,
            additional_annotations=[
                BoxAnnotation(bottom=0, fill_alpha=0.1, fill_color='red'),
                BoxAnnotation(top=0, fill_alpha=0.1, fill_color='blue'),
            ],
            minimum_range=[-5, 5]
        )

        self.temp_outdoor_icon = DashboardIcon(
            source=self.temp_outdoor_source,
            source_value_key='value',
            height=100,
            width=100,
            color_formatter=temp_icon_color,
            text_formatter=lambda value: f'\n{value:0.2f} °C'
        )

        self.temp_outdoor_ts = DashboardTimeSeriesMini(
            source=self.temp_outdoor_source,
            source_value_key='value',
            source_time_key='time',
            source_color_key='color',
            height=125,
            width=200,
            additional_annotations=[
                BoxAnnotation(bottom=0, fill_alpha=0.1, fill_color='red'),
                BoxAnnotation(top=0, fill_alpha=0.1, fill_color='blue'),
            ],
            minimum_range=[-5, 5]
        )

        def co2_icon_color(value: Number) -> str:
            if value > 1000:
                return 'red'
            elif value > 600:
                return 'yellow'
            else:
                return 'green'

        self.co2_icon = DashboardIcon(
            source=self.co2_source,
            source_value_key='value',
            height=100,
            width=100,
            color_formatter=co2_icon_color,
            text_formatter=lambda value: f'{value:0.2f} ppm'
        )

        self.co2_ts = DashboardTimeSeriesMini(
            source=self.co2_source,
            source_value_key='value',
            source_time_key='time',
            source_color_key='color',
            height=125,
            width=200,
            additional_annotations=[
                BoxAnnotation(bottom=1000, fill_alpha=0.1, fill_color='red'),
                BoxAnnotation(bottom=600, top=1000, fill_alpha=0.1, fill_color='orange'),
                BoxAnnotation(top=600, fill_alpha=0.1, fill_color='green'),
            ],
            minimum_range=[300, 700]
        )

    def refresh_data(self) -> None:
        period = st.UtcPeriod(st.utctime_now() - self.hist_length, st.utctime_now())
        temp = self.temp_indoor.get_data(period=period)
        t, v = get_xy(self.cal, temp)
        new = {'value': [v],
               'time': [t],
               'color': ['grey']
               # 'color': [self.temp_icon.color_selector(v[-1])]
               }
        self.temp_indoor_source.data = new

        period = st.UtcPeriod(st.utctime_now() - self.hist_length, st.utctime_now())
        temp = self.temp_outdoor.get_data(period=period)
        t, v = get_xy(self.cal, temp)
        new = {'value': [v],
               'time': [t],
               'color': ['grey']
               # 'color': [self.temp_icon.color_selector(v[-1])]
               }
        self.temp_outdoor_source.data = new

        co2 = self.co2.get_data(period=period)
        t, v = get_xy(self.cal, co2)
        new = {'value': [v],
               'time': [t],
               'color': ['grey']
               # 'color': [self.co2_icon.color_selector(v[-1])]
               }
        self.co2_source.data = new

    def update(self):
        self.refresh_data()
        self.temp_indoor_ts.update()
        self.temp_indoor_icon.update()
        self.temp_outdoor_ts.update()
        self.temp_outdoor_icon.update()
        self.co2_icon.update()
        self.co2_ts.update()

    def __call__(self, doc):
        doc.add_periodic_callback(self.update, 1000)

        doc.title = "Bokeh Dashboard test app."
        # layout = fig
        doc.add_root(column(
            row(self.temp_indoor_icon.layout, self.temp_indoor_ts.layout),
            row(self.co2_icon.layout, self.co2_ts.layout),
            row(self.temp_outdoor_icon.layout, self.temp_outdoor_ts.layout),
        )
        )

        return doc


apps = {'/test': Application(FunctionHandler(TestApp()))}

server = Server(apps, port=5000, log_level='debug', allow_websocket_origin=['10.0.0.26:5000'])
server.io_loop.start()
server.show('/test')
