from data_collection.netatmo import NetatmoTsRepository, RateLimiter
from shyft.api import Calendar, UtcPeriod, utctime_now, time
import os
import sys
import pytest
from typing import List
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.StreamHandler()
    ])

# Get credentials:
if not 'CONFIG_DIRECTORY' in os.environ:
    raise EnvironmentError('Cannot find path to app authentication codes.')

sys.path.append(os.environ['CONFIG_DIRECTORY'])

from netatmo_authentication import login


def convert_seconds_to_now(seconds: List[int]) -> List[time]:
    now = utctime_now() - max(seconds) - 1  # We want seconds leading up to now.
    return [now + sec for sec in seconds]


@pytest.fixture
def net():
    """Return a netatmo instance."""
    return NetatmoTsRepository(**login)


@pytest.fixture
def net_no_login_w_call_limits():
    net = NetatmoTsRepository(**login, api_limits={
        '10 seconds': {'calls': 10, 'timespan': 10, 'wait': 0.0001},
        '100 seconds': {'calls': 500, 'timespan': 100, 'wait': 0.001},
    }, direct_login=False)
    return net


def test_construction(net):
    assert net.stations


def test__get_measurements_from_station(net):
    tsvec = net._get_measurements_from_station(station_name='Eftasåsen', measurement_types=['Temperature'])

    assert tsvec[0].values.to_numpy().all()


def test_get_measurements_from_station(net):
    # This method uses a period longer than 1024 values, utilizing a rate limiter to not trip Netatmo api limits.
    cal = Calendar('Europe/Oslo')
    period = UtcPeriod(cal.time(2019, 2, 18), cal.time(2019, 2, 24))
    tsvec = net.get_measurements_from_station(station_name='Eftasåsen', measurement_types=['Temperature'],
                                              utc_period=period)

    assert tsvec[0].values.to_numpy().all()
