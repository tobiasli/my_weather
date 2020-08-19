"""This script creates a simple static plot of data from the DtssHost via a DtsClient."""
import typing as ty
import sys
import os
import logging

import shyft.time_series as st
import numpy as np

from weather.data_sources.netatmo.domain import NetatmoDomain, types
from weather.data_sources.netatmo.repository import NetatmoEncryptedEnvVarConfig
from weather.data_sources.heartbeat import create_heartbeat_request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler()
    ])

Number = ty.Union[float, int]


class NetatmoData:
    def __init__(self, type: str):
        self.type = type

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
        self.domain = NetatmoDomain(
            username=config.username,
            password=config.password,
            client_id=config.client_id,
            client_secret=config.client_secret
        )
        station = 'Eftasåsen'
        module = 'Stua'
        self.plot_data = [
            {'data': self.domain.get_measurement(station_name=station, data_type=types.temperature.name, module_name=module),
             'color': '#E64C3E'},  # red
            {'data': self.domain.get_measurement(station_name=station, data_type=types.co2.name, module_name=module),
             'color': '#B0CA55'},  # green
            {'data': self.domain.get_measurement(station_name=station, data_type=types.humidity.name, module_name=module),
             'color': '#0F2933'},  # dark green
        ]
        # ('Pressure', 'mbar', point_fx.POINT_INSTANT_VALUE, '#33120F'),  # brown
        # ('Noise', 'db', point_fx.POINT_INSTANT_VALUE, '#E39C30'),  # yellow
        # ('Rain', 'mm', point_fx.POINT_INSTANT_VALUE, '#448098'),  # light blue
        # ('WindStrength', 'km / h', point_fx.POINT_INSTANT_VALUE, '#8816AB'),  # purple

        # Get timeseries from measurements:

        # client = DtsClient(f'{socket.gethostname()}:{os.environ["DTSS_PORT_NUM"]}')
        self.tsv = st.TsVector([meas['data'].time_series for meas in self.plot_data])
        self.cal = st.Calendar('Europe/Oslo')
        epsilon = 0.1

    def next(self) -> ty.Dict[str, Number]:
        self.client = st.DtsClient(f'{os.environ["DTSS_SERVER"]}:{os.environ["DTSS_PORT_NUM"]}')
        now = st.utctime_now()
        period = st.UtcPeriod(now - self.cal.HOUR, now)
        data = self.client.evaluate(self.tsv, period)

        if self.type == 'temp':
            ts = data[0]
        elif self.type == 'co2':
            ts = data[1]
        else:
            ts = data[2]

        values = ts.values.to_numpy()
        times = ts.time_axis.time_points_double
        out = dict(
            current=values[-1] if len(values) else np.nan,
            time=float(st.utctime_now()) * 1000,
            # time=float(times[0]) * 1000,
            max=max(values),
            min=min(values),
        )
        # print('data fetch success')
        return out