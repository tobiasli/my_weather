import typing as ty
import random
from abc import abstractmethod

import numpy as np

import shyft.time_series as st

from bokeh.server.server import Server
from bokeh.application import Application
from bokeh.application.handlers.function import FunctionHandler
from bokeh.plotting import figure, ColumnDataSource
from bokeh.models import Button, Span, Range1d, Widget, Annotation
from bokeh.layouts import column, row

Number = ty.Union[int, float]


class DummyData:
    def __init__(self, start=0, inc=1):
        self.inc = inc
        self.hist = [start]

    def next(self) -> ty.Dict[str, Number]:
        """Return temperature data."""
        self.hist.append(self.hist[-1] + random.random()*self.inc - 0.5*self.inc)
        out = dict(
            current=self.hist[-1],
            time=float(st.utctime_now()) * 1000,
            max=max(self.hist),
            min=min(self.hist),
        )
        return out


TEMP_DATA = DummyData()
CO2_DATA = DummyData(start=600, inc=100)


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
        return source.data[source_key][-1]

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
                 **kwargs) -> None:
        self.source_time_key = source_time_key
        self.source_color_key = source_color_key
        self.additional_annotations = additional_annotations
        super(DashboardTimeSeriesMini, self).__init__(source, source_value_key)

        self.fig = figure(
            x_axis_type='datetime',
            **kwargs
        )
        self.fig.circle(source=self.source,
                        x=self.source_time_key,
                        y=self.source_value_key,
                        color=self.source_color_key,
                        size=2)
        for annotation in self.additional_annotations:
            self.fig.add_layout(annotation)
        self.fig.y_range = Range1d(-1, 1)
        self.fig.toolbar.logo = None
        self.fig.toolbar_location = None

    def update(self) -> None:
        maximum = max(self.source.data[self.source_value_key])
        minimum = min(self.source.data[self.source_value_key])
        upper = max(np.ceil(maximum + 0.1 * abs(maximum)), 1)
        lower = min(np.floor(minimum - 0.1 * abs(minimum)), -1)

        self.fig.y_range.start = lower
        self.fig.y_range.end = upper

    @property
    def layout(self) -> Widget:
        return self.fig


class TestApp:

    def __init__(self) -> None:
        self.temp_source = ColumnDataSource({'time': [], 'value': [], 'color': []})
        self.co2_source = ColumnDataSource({'time': [], 'value': [], 'color': []})

        def temp_icon_color(value: Number) -> str:
            if value > 0:
                return 'red'
            else:
                return 'blue'

        self.temp_icon = DashboardIcon(
            source=self.temp_source,
            source_value_key='value',
            height=100,
            width=100,
            color_formatter=temp_icon_color,
            text_formatter=lambda value: f'\n{value:0.2f} °C'
        )

        self.temp_ts = DashboardTimeSeriesMini(
            source=self.temp_source,
            source_value_key='value',
            source_time_key='time',
            source_color_key='color',
            height=125,
            width=200,
            additional_annotations=[Span(dimension='width', location=0)]
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
                Span(dimension='width', location=1000),
                Span(dimension='width', location=600),
            ]
        )

    def refresh_data(self) -> None:
        temp = TEMP_DATA.next()
        new = {'value': [temp['current']],
               'time': [temp['time']],
               'color': [self.temp_icon.color_selector(temp['current'])]
               }
        self.temp_source.stream(new, 100)

        co2 = CO2_DATA.next()
        new = {'value': [co2['current']],
               'time': [co2['time']],
               'color': [self.co2_icon.color_selector(co2['current'])]
               }
        self.co2_source.stream(new, 100)

    def update(self):
        self.refresh_data()
        self.temp_ts.update()
        self.temp_icon.update()
        self.co2_icon.update()
        self.co2_ts.update()

    def __call__(self, doc):
        doc.add_periodic_callback(self.update, 1000)

        doc.title = "Bokeh Dashboard test app."
        # layout = fig
        doc.add_root(column(
            row(self.temp_icon.layout, self.temp_ts.layout),
            row(self.co2_icon.layout, self.co2_ts.layout)
        )
        )

        return doc


apps = {'/test': Application(FunctionHandler(TestApp()))}

server = Server(apps, port=5000, log_level='debug', allow_websocket_origin=['10.0.0.26:5000'])
server.io_loop.start()
server.show('/test')
