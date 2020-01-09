"""Simple method for creating placeholder TimeSeries."""
from typing import Union
import random
from math import ceil

from shyft.time_series import (UtcPeriod, TimeSeries, TimeAxis, TimeAxisByPoints, UtcTimeVector,
                            point_interpretation_policy, time)

Number = Union[int, float]
Time = Union[time, int, float]


def create_ts(value: Number = random.random(), read_period: UtcPeriod = None, dt: Time = 1) -> TimeSeries:
    """Function for creating arbitrary timeseries."""
    if read_period:
        ta = TimeAxis(read_period.start, dt, ceil(read_period.end-read_period.start)/dt)
    else:
        ta = TimeAxisByPoints(UtcTimeVector([1, 2, 3]))

    return TimeSeries(ta, value, point_interpretation_policy.POINT_INSTANT_VALUE)