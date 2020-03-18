"""Tests for bokeh visualization tools."""
import shyft.time_series as st

from visual.dtss_serve_test import get_xy


def test_get_xy():
    data = st.TimeSeries(st.TimeAxis())