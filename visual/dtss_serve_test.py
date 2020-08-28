import sys
import os
import logging
import socket
import typing as ty
from abc import abstractmethod

import numpy as np

import shyft.time_series as st

from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.plotting import ColumnDataSource, figure, Document
from bokeh.models import Button, BoxAnnotation, Widget, Annotation, Range1d, LayoutDOM, Div, DatetimeTickFormatter
from bokeh.layouts import column, row

from rdp import rdp

from weather.data_sources.netatmo.domain import NetatmoDomain, types, NetatmoMeasurement
from weather.data_sources.netatmo.repository import NetatmoEncryptedEnvVarConfig

Number = ty.Union[int, float]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler()
    ])


def bokeh_time_from_timestamp(cal: st.Calendar, timestamp) -> float:
    """Create a localized ms timestamp from a shyft utc timestamp."""
    return float((timestamp + cal.tz_info.base_offset()) * 1000)


def get_xy(cal: st.Calendar, ts: st.TimeSeries) -> np.array:
    """Method for extracting xy-data from TimeSeries"""
    if ts.point_interpretation() == st.point_interpretation_policy.POINT_INSTANT_VALUE:
        return [bokeh_time_from_timestamp(cal, t) for t in
                ts.time_axis.time_points_double[0:-1]], ts.values.to_numpy()
    elif ts.point_interpretation() == st.point_interpretation_policy.POINT_AVERAGE_VALUE:
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
        self.fig.xaxis.formatter = DatetimeTickFormatter(
            days=["%d/%m"],
            # months=["%m/%d %H:%M"],
            hours=[''],
            # minutes=["%m/%d %H:%M"]
        )
        self.fig.yaxis.axis_line_color = axis_color
        self.fig.yaxis.major_tick_line_color = axis_color
        self.fig.yaxis.minor_tick_line_color = axis_color

    def update(self) -> None:
        maximum = np.nanmax(self.source.data[self.source_value_key])
        minimum = np.nanmin(self.source.data[self.source_value_key])

        if self.minimum_range:
            upper = np.nanmax([np.ceil(maximum + 0.2 * abs(maximum - minimum)), self.minimum_range[1]])
            lower = np.nanmin([np.floor(minimum - 0.2 * abs(maximum - minimum)), self.minimum_range[0]])

            self.fig.y_range.start = lower
            self.fig.y_range.end = upper

        # self.fig.x_range.start = np.nanmin(self.source.data[self.source_time_key])
        # self.fig.x_range.end = np.nanmax(self.source.data[self.source_time_key]) + 2*3600*1000  # Pad with 2 hours at end of series.

    @property
    def layout(self) -> Widget:
        return self.fig


class CompactDataWidget:
    def __init__(self,
                 dtss: st.DtsClient,
                 measurement: NetatmoMeasurement,
                 height: int,
                 width: int,
                 color_formatter: ty.Callable[[Number], str],
                 text_formatter: ty.Callable[[Number], str],
                 additional_annotations: ty.Sequence[BoxAnnotation],
                 minimum_range: ty.Sequence[Number],
                 text_width: int,
                 ) -> None:
        self.source = ColumnDataSource({'time': [], 'value': [], 'color': []})

        self.measurement = measurement
        self.dtss_data = DtssData(dtss, measurement.time_series)

        self.icon = DashboardIcon(
            source=self.source,
            source_value_key='value',
            height=height,
            width=width,
            color_formatter=color_formatter,
            text_formatter=text_formatter,
            tags=['icon']
        )

        self.time_series = DashboardTimeSeriesMini(
            source=self.source,
            source_value_key='value',
            source_time_key='time',
            source_color_key='color',
            height=int(height * 1.25),
            width=int(width * 2),
            additional_annotations=additional_annotations,
            minimum_range=minimum_range,
            tags = ['time_series']
        )

        self.text = Div(
            text=f'<p style = "font-family:courier new,courier;font-size:20px;font-style:strong;">{self.measurement.module.name.upper()}</p>',
            height=int(height*0.9),
            width=text_width,
            align=('center', 'center'),
            # margin=(0,0,int(height*0.60*-1),0,
            tags=['text']
        )

    def refresh_data(self, cal: st.Calendar, hist_length: st.time) -> None:
        """Put new data into datasource for icon and plot."""
        period = st.UtcPeriod(st.utctime_now() - hist_length, st.utctime_now())
        temp = self.dtss_data.get_data(period=period)
        t, v = get_xy(cal, temp)

        points = [(time, value) for time, value in zip(t, v)]
        ideal_number_of_points = 10
        epsilon = (len(points) / (3 * ideal_number_of_points)) * 2  # https://stackoverflow.com/questions/57052434/can-i-guess-the-appropriate-epsilon-for-rdp-ramer-douglas-peucker
        reduced = rdp(points, epsilon=epsilon)
        lists = list(map(list, zip(*reduced)))
        t = lists[0]
        v = lists[1]

        new = {'value': [v],
               'time': [t],
               'color': ['grey']
               # 'color': [self.icon.color_selector(v[-1])]
               }
        self.source.data = new

    def update(self) -> None:
        """Plot and icon according to data in datasource."""
        self.icon.update()
        self.time_series.update()

    @property
    def layout(self) -> LayoutDOM:
        return row(self.text, self.icon.layout, self.time_series.layout)


class TestApp:

    def __init__(self, doc: Document) -> None:
        self.cal = st.Calendar('Europe/Oslo')
        self.client = st.DtsClient(f'{os.environ["DTSS_SERVER"]}:{os.environ["DTSS_PORT_NUM"]}')
        self.hist_length = self.cal.DAY * 2

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

        def temp_icon_color(value: Number) -> str:
            if value > 0:
                return 'red'
            else:
                return 'blue'

        def co2_icon_color(value: Number) -> str:
            if value > 1000:
                return 'red'
            elif value > 600:
                return 'yellow'
            else:
                return 'green'

        self.widgets = [
            CompactDataWidget(
                dtss=self.client,
                measurement=domain.get_measurement(
                    station_name='Eftasåsen',
                    module_name='Stua',
                    data_type=types.temperature.name
                ),
                height=100,
                width=100,
                color_formatter=temp_icon_color,
                text_formatter=lambda value: f'\n{value:0.2f} °C',
                additional_annotations=[
                    BoxAnnotation(bottom=0, fill_alpha=0.1, fill_color='red'),
                    BoxAnnotation(top=0, fill_alpha=0.1, fill_color='blue'),
                ],
                minimum_range=[-5, 5],
                text_width=50,
            ),
            CompactDataWidget(
                dtss=self.client,

                measurement=domain.get_measurement(
                    station_name='Eftasåsen',
                    module_name='Ute',
                    data_type=types.temperature.name,
                ),
                height=100,
                width=100,
                color_formatter=temp_icon_color,
                text_formatter=lambda value: f'\n{value:0.2f} °C',
                additional_annotations=[
                    BoxAnnotation(bottom=0, fill_alpha=0.1, fill_color='red'),
                    BoxAnnotation(top=0, fill_alpha=0.1, fill_color='blue'),
                ],
                minimum_range=[-5, 5],
                text_width=50,
            ),
            CompactDataWidget(
                dtss=self.client,
                measurement=domain.get_measurement(
                    station_name='Eftasåsen',
                    module_name='Stua',
                    data_type=types.co2.name,
                ),
                height=100,
                width=100,
                color_formatter=co2_icon_color,
                text_formatter=lambda value: f'{value:0.2f} ppm',
                additional_annotations=[
                    BoxAnnotation(bottom=1000, fill_alpha=0.1, fill_color='red'),
                    BoxAnnotation(bottom=600, top=1000, fill_alpha=0.1, fill_color='orange'),
                    BoxAnnotation(top=600, fill_alpha=0.1, fill_color='green'),
                ],
                minimum_range=[300, 700],
                text_width=50,
            )
        ]

        self.add_to_doc(doc)

    def update(self):
        for widget in self.widgets:
            widget.refresh_data(self.cal, self.hist_length)
            widget.update()

    def add_to_doc(self, doc):
        doc.add_periodic_callback(self.update, 10000)

        doc.title = "Bokeh Dashboard test app."
        doc.add_root(column(*[w.layout for w in self.widgets]))

        self.update()

        return doc


apps = {'/test': Application(FunctionHandler(TestApp))}
port = 5000
server = Server(apps, port=port, log_level='debug',
                allow_websocket_origin=[f'{socket.gethostbyname(socket.gethostname())}:{port}'])
print(f'http://{socket.gethostbyname(socket.gethostname())}:{port}/test')
server.io_loop.start()
server.show('/test')
