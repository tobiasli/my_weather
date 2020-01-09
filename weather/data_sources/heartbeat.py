"""The HeartbeatRepository is a dummy repository used to check if the DtssHost's DtsServer is running correctly on the
correct port."""
import urllib
import numpy as np
import logging
from typing import Sequence, Dict, TYPE_CHECKING

from shyft.time_series import TimeSeries, StringVector, TsVector, TsInfo, TsInfoVector, UtcPeriod, POINT_INSTANT_VALUE

from weather.interfaces.data_collection_repository import DataCollectionRepository
from weather.utilities.create_ts import create_ts
if TYPE_CHECKING:  # Prevents import at runtime so we don't do cyclic imports.
    from weather.service.dtss_host import DtssHost


class HeartbeatRepository(DataCollectionRepository):
    """The HeartbeatRepository is a dummy repository used to check if the DtssHost's DtsServer is running correctly on
    the correct port. The read_ and find_callbacks always return something, just so we can verify that the DtsServer is
    running."""

    name = 'heartbeat'

    def __init__(self, host: "DtssHost"):
        """Heartbeat callbacks that return arbitrary responses to verify that the DtssHost is accepting calls as
        intended."""

        self.host = host

    def read(self, list_of_ts_id: Sequence[str], period: UtcPeriod) -> Dict[str, TimeSeries]:
        """Read accepts any sequence of ts_ids and returns data for the ts_ids at least covering period.

        Note: The read_callback is a less specialized function than the TsRepository.read, so this method just calls
        the read_callback.

        Args:
            list_of_ts_id: A sequence of strings identifying specific timeseries available from the netatmo login.
            period: A period defined by a utc timestamp for the start and end of the analysis period.

        Returns:
            A TsVector containing the resulting timeseries containing data enough to cover the query period.
        """
        return {ts_id: ts for ts_id, ts in zip(list_of_ts_id, self.read_callback(ts_ids=StringVector([list_of_ts_id]),
                                                                                 read_period=period))}

    def read_callback(self, ts_ids: StringVector, read_period: UtcPeriod) -> TsVector:
        """This callback is passed as the default read_callback for a shyft.time_series.DtsServer.

        Args:
            ts_ids: A sequence of strings identifying specific timeseries available from the netatmo login. Matches the
                    formatting provided by DataCollectionRepository.create_ts_id()
            read_period: A period defined by a utc timestamp for the start and end of the analysis period.

        Returns:
            A TsVector containing the resulting timeseries containing data enough to cover the query period.
        """
        logging.info(f'DtssHost Heartbeat read_callback at {self.host.address}.')
        # noinspection PyArgumentList
        tsv = TsVector()
        for _ in ts_ids:
            tsv.append(create_ts(read_period=read_period, value=1))

        return tsv

    def find(self, query: str) -> TsInfoVector:
        """Check if data matching the query exists in the data source.

        Args:
            query: The url representing a relevant query for this DataCollectionRepository.

        Returns:
            A sequence of results matching the query.
        """
        return self.find_callback(query)

    def find_callback(self, query: str) -> TsInfoVector:
        """This callback is passed as the default find_callback for a shyft.time_series.DtsServer.

        Args:
            query: The url representing a relevant query for this DataCollectionRepository. Matches the formatting
                   provided by DataCollectionRepository.create_ts_query()

        Returns:
            A sequence of results matching the query.
        """
        message = parse_heartbeat(query=query)
        logging.info(f'DtssHost Heartbeat find_callback at {self.host.address}: {message}')
        # noinspection PyArgumentList
        tsi = TsInfo(
            name=f'heartbeat: {message}',
            point_fx=POINT_INSTANT_VALUE,
            delta_t=np.nan,
            olson_tz_id='Some/Timezone',
            data_period=UtcPeriod(0, 1),
            created=0,
            modified=0
        )

        # noinspection PyArgumentList
        tsiv = TsInfoVector()
        tsiv.append(tsi)
        return tsiv

    def read_forecast(self, list_of_fc_id, period):
        """
        read and return the newest forecast that have the biggest overlap with specified period
        note that we should check that the semantic of this is reasonable
        """
        raise NotImplementedError("read_forecast")

    def store(self, timeseries_dict):
        """ Store the supplied time-series to the underlying db-system.
            Parameters
            ----------
            timeseries_dict: dict string:timeseries
                the keys are the wanted ts(-path) names
                and the values are shyft api.time-series.
                If the named time-series does not exist, create it.
        """
        raise NotImplementedError("read_forecast")


def create_heartbeat_request(message: str = '') -> str:
    """Create a valid if checking if read_callbacks work as intended."""
    return f'heartbeat://callback/{message}'


def parse_heartbeat(*, query: str) -> str:
    """Create a valid ts url from a netatmo device_name, module_name and data_type to identify a timeseries."""
    parse = urllib.parse.urlparse(query)
    return parse.path.split('/')[1]
