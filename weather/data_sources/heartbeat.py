"""The HeartbeatRepository is a dummy repository used to check if the DtssHost's DtsServer is running correctly on the
correct port."""

from typing import Sequence, Dict
from weather.interfaces.data_collection_repository import DataCollectionRepository
from weather.utilities.create_ts import create_ts
from shyft.api import TimeSeries, StringVector, TsVector, TsInfo, TsInfoVector, UtcPeriod, POINT_INSTANT_VALUE
import numpy as np
import logging


