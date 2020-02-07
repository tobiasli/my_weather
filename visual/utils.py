import numpy as np
from shyft.time_series._time_series import Calendar, TimeSeries, point_interpretation_policy


def bokeh_time_from_timestamp(cal: Calendar, timestamp) -> float:
    """Create a localized ms timestamp from a shyft utc timestamp."""
    return float((timestamp + cal.tz_info.base_offset()) * 1000)


def get_xy(cal: Calendar, ts: TimeSeries) -> np.array:
    """Method for extracting bokeh xy-data from TimeSeries"""
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