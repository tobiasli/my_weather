"""weather.test.utilities are different functions and classes that are needed for tests."""
from weather.interfaces.data_collection_repository import DataCollectionRepository
from shyft.api import (StringVector, UtcPeriod, TsVector, TsInfoVector, TimeSeries, TimeAxisByPoints, UtcTimeVector,
                       point_interpretation_policy, TsInfo, utctime_now, TimeAxis, time)
import random
import urllib
from typing import Dict, Union, List, Sequence
from math import ceil

Number = Union[int, float]
Time = Union[time, int, float]


def create_ts(value: Number = random.random(), read_period: UtcPeriod = None, dt: Time = 1) -> TimeSeries:
    """Function for creating arbitrary timeseries."""
    if read_period:
        ta = TimeAxis(read_period.start, dt, ceil(read_period.end-read_period.start)/dt)
    else:
        ta = TimeAxisByPoints(UtcTimeVector([1, 2, 3]))
    return TimeSeries(ta, value, point_interpretation_policy.POINT_INSTANT_VALUE)


class MockRepositoryError(Exception):
    """Errors raised by the MockRepository."""
    pass


class MockRepository(DataCollectionRepository):
    """Test repository that provides arbitrary data for tests."""

    name = 'mock'

    def __init__(self, repo_name_override: str = 'mock') -> None:
        """A mock repository that returns arbitrary timeseries and has a name property. Overwrite name property with
        the one defined as arg."""

        self.name = repo_name_override

    @classmethod
    def create_ts_id(cls, ts_name: str) -> str:
        """Create a valid ts_id url string that is identifiable for the read_callback of the
        DataCollectionRepository."""
        return f'{cls.name}://{ts_name}'

    @classmethod
    def parse_ts_id(cls, *, ts_id: str) -> Dict[str, str]:
        """Get relevant information from the ts_id."""
        parse = urllib.parse.urlparse(ts_id)
        if parse.scheme != cls.name:
            raise MockRepositoryError(f'ts_id scheme does not match repository name: '
                                         f'ts_id={parse.scheme}, {cls.__name__}={cls.name}')

        return {'station_name': parse.netloc}

    @classmethod
    def create_ts_query(cls, *, ts_name: str) -> str:
        """Create a valid query url string that is identifiable for the find_callback of the
        DataCollectionRepository."""
        return cls.create_ts_id(ts_name=ts_name)

    @classmethod
    def parse_ts_query(cls, *, query: str) -> Dict[str, str]:
        """Get relevant information from the ts_query"""
        return cls.parse_ts_id(ts_id=query)

    def read_callback(self, *, ts_ids: StringVector, read_period: UtcPeriod) -> TsVector:
        """This callback is passed as the default read_callback for a shyft.api.DtsServer.

        Args:
            ts_ids: A sequence of strings identifying specific timeseries available from the netatmo login. Matches the
                    formatting provided by DataCollectionRepository.create_ts_id()
            read_period: A period defined by a utc timestamp for the start and end of the analysis period.

        Returns:
            A TsVector containing the resulting timeseries containing data enough to cover the query period.
        """
        tsv = TsVector()
        for value, ts_id in enumerate(ts_ids):
            tsv.append(create_ts(value, read_period=read_period))

        return tsv

    def find_callback(self, *, query: str) -> TsInfoVector:
        """This callback is passed as the default find_callback for a shyft.api.DtsServer.

        Args:
            query: The url representing a relevant query for this DataCollectionRepository. Matches the formatting
                   provided by DataCollectionRepository.create_ts_query()

        Returns:
            A sequence of results matching the query.
        """
        ts = create_ts(0)
        tsi = TsInfo(
            name=query,
            point_fx=point_interpretation_policy.POINT_INSTANT_VALUE,
            delta_t=0,
            olson_tz_id='',
            data_period=ts.time_axis.total_period(),
            created=utctime_now(),
            modified=utctime_now()
            )

        return TsInfoVector([tsi])

    def read(self, list_of_ts_id: Sequence[str], period: UtcPeriod) -> Dict[str, TimeSeries]:
        """Take a sequence of strings identifying specific timeseries and get data from these series according to
        the timespan defined in period.

        Note: The reac_callback is a less specialized funciton than the TsRepository.read, so this method just calls
        the read_callback.

        Args:
            list_of_ts_id: A sequence of strings identifying specific timeseries available from the netatmo login.
            period: A period defined by a utc timestamp for the start and end of the analysis period.

        Returns:
            A TsVector containing the resulting timeseries containing data enough to cover the query period.
        """

        return {ts_id: ts for ts_id , ts in zip(list_of_ts_id, self.read_callback(ts_ids=StringVector([list_of_ts_id]),
                                                                                  read_period=period))}

    def read_forecast(self, list_of_fc_id: List[str], period: UtcPeriod):
        """
        read and return the newest forecast that have the biggest overlap with specified period
        note that we should check that the semantic of this is reasonable
        """
        raise NotImplementedError("read_forecast")

    def store(self, timeseries_dict: Dict[str, TimeSeries]):
        """ Store the supplied time-series to the underlying db-system.
            Parameters
            ----------
            timeseries_dict: dict string:timeseries
                the keys are the wanted ts(-path) names
                and the values are shyft api.time-series.
                If the named time-series does not exist, create it.
        """
        raise NotImplementedError("read_forecast")




