"""A DataCollectionService is a service that communicates with a DtssHost and stores data to the DtssHost according
to a set of ts_ids, timespans and intervals."""
import logging
from typing import Sequence, Union, Optional
from abc import ABC, abstractmethod

import shyft.time_series as st

from weather.data_sources.heartbeat import create_heartbeat_request

Number = Union[float, int]
TimeType = Union[st.time, Number]


class DataCollectionServiceError(Exception):
    """Errors raised by a DataCollectionService."""
    pass


class DataCollectionPeriodBase(ABC):
    """Template for DataCollectionPeriods"""

    @abstractmethod
    def period(self) -> st.UtcPeriod:
        """The period we want DataCollectionTask to use."""
        pass


class DataCollectionPeriodRelative(DataCollectionPeriodBase):
    """Class that defines a period for which we collect data. Periods are defined as offsets from utc_now."""

    def __init__(self, start_offset: TimeType, wait_time: TimeType, end_offset: Optional[TimeType] = 0):
        """DataCollectionPeriods are used to define the period query pattern for a DataCollectionSerive. The
        DataCollectionPeriod defines the start, stop of every individual period, and the wait time between each query.

        Args:
            start_offset: The offset in seconds from now that defines the start of the queried data period.
            end_offset: The offset in seconds from now that defines the end of the queried data period. Default=0.
            wait_time: The wait time in seconds between individual queries.
        """
        self.wait_time = wait_time
        self.start_offset = start_offset
        self.end_offset = end_offset

    def period(self) -> st.UtcPeriod:
        """Return a UtcPeriod correctly defined for a data query right now."""
        now = st.utctime_now()
        return st.UtcPeriod(now - self.start_offset, now - self.end_offset)


class DataCollectionPeriodAbsolute(DataCollectionPeriodBase):
    """Class that defines a period for which we collect data. Periods are defined as offsets from utc_now."""

    def __init__(self, start: TimeType, wait_time: TimeType, end: Optional[TimeType] = st.utctime_now()):
        """DataCollectionPeriods are used to define the period query pattern for a DataCollectionSerive. The
        DataCollectionPeriod defines the start, stop of every individual period, and the wait time between each query.

        Args:
            start_offset: The offset in seconds from now that defines the start of the queried data period.
            end_offset: The offset in seconds from now that defines the end of the queried data period. Default=0.
            wait_time: The wait time in seconds between individual queries.
        """
        self.wait_time = wait_time
        self.start = start
        self.end = end

    def period(self) -> st.UtcPeriod:
        """Return a UtcPeriod correctly defined for a data query right now."""
        return st.UtcPeriod(self.start, self.end)


class DataCollectionTask:
    """A DataCollectionTask is a service that communicates with a DtssHost and stores data to the DtssHost according
to a set of ts_ids, timespans and intervals."""

    def __init__(self, task_name: str,
                 read_dtss_address: str,
                 read_ts: Sequence[st.TimeSeries],
                 read_period: DataCollectionPeriodBase,
                 store_dtss_address: str,
                 store_ts_ids: Sequence[str]
                 ) -> None:
        """A DataCollectionTask is a tool for regularly querying a dtss for data, and then storing the data in a
        corresponding dtss using the store function.

        Args:
            task_name: The name of the task, so it can be identified in logs.
            read_dtss_address: The address of the DtssHost service you want to read data from.
            read_ts: A list of unbound timeseries that can be found in the read_client.
            read_period: The relative period we want to read data from.
            store_dtss_address: The address of the DtssHost service you want to store the data in.
            store_ts_ids: A list of strings that we want to store the timeseries as in the store DtssHost.
        """
        self.name = task_name
        self.read_dtss_address = read_dtss_address
        self.read_ts = read_ts
        self.read_period = read_period
        self.store_dtss_address = store_dtss_address
        self.store_ts_ids = store_ts_ids
        self.read_client: st.DtsClient = None
        self.store_client: st.DtsClient = None

        self.restart_clients()

    def restart_clients(self) -> None:
        """Initiate DtsClients."""
        if self.read_client:
            del self.read_client
        self.read_client = st.DtsClient(self.read_dtss_address)

        if self.store_client:
            del self.store_client
        self.store_client = st.DtsClient(self.store_dtss_address)

    def perform_read(self) -> st.TsVector:
        """Perform a read query that returns a ts_vector with the queried data."""
        return self.read_client.evaluate(st.TsVector(self.read_ts),
                                         self.read_period.period())

    def perform_store(self, store_data: st.TsVector) -> None:
        """Perform a store query that stores the data in store_data according to the ts_names in store_ts_ids."""
        self.store_client.store_ts(
            st.TsVector(
                [st.TimeSeries(ts_id, ts) for ts_id, ts in zip(self.store_ts_ids, store_data)]
            )
        )

    def health_check(self) -> bool:
        """Simple query towards both read_client and store_client to verify integrity of DataCollectionTask."""
        request = create_heartbeat_request(f'Health check from {self.name}')
        try:
            return self.read_client.find(request) and self.store_client.find(request)
        except RuntimeError:
            return False

    def collect_data(self) -> None:
        """Perform a data query and store if data is found."""
        read_data = None
        try:
            read_data = self.perform_read()
        except Exception as e:
            logging.error(f'Read from read_client failed with exception: {e}')

        if read_data:
            try:
                self.perform_store(store_data=read_data)
                logging.info(
                    f'DataCollectionTask {self.name}: Read and store complete for {len(read_data)} timeseries.')
            except Exception as e:
                logging.error(f'Store to store_client failed with exception: {e}')
