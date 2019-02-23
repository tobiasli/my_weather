from data_collection.netatmo import NetatmoTsRepository
from shyft.api import Calendar, UtcPeriod
import os
import sys
import pytest
import numpy

# Get credentials:
if not 'WEATHER_AUTH' in os.environ:
    raise EnvironmentError('Cannot find path to app authentication codes.')

sys.path.append(os.environ['WEATHER_AUTH'])

from authentication import login


@pytest.fixture
def net():
    """Return a netatmo instance."""
    return NetatmoTsRepository(**login)


def test_netatmo_callbacks_construction(net):
    assert net.stations


def test_netatmo_get_measurements_from_station(net):
    tsvec = net.get_measurements_from_station('Eftasåsen', 'Temperature')

    assert tsvec[0].values.to_numpy().all()
