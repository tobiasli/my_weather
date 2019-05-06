"""A DataCollectionService is a service that communicates with a DtssHost and stores data to the DtssHost according
to a set of ts_ids, timespans and intervals."""
from typing import Sequence, Union, Optional
import logging
import time
import threading

import shyft.time_series as st

Number = Union[float, int]
TimeType = Union[st.time, Number]


class DataCollectionPeriod:
    """Class that defines a period for which we collect data. Periods are defined as offsets from utc_now."""

    def __init__(self, start_offset: TimeType, wait_time: TimeType, end_offset: Optional[TimeType] = 0, ):
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


class DataCollectionService:
    """A DataCollectionService is a service that communicates with a DtssHost and stores data to the DtssHost according
to a set of ts_ids, timespans and intervals."""

    def __init__(self, service_name: str,
                 read_dtss_address: str,
                 read_ts: Sequence[str],
                 read_period: DataCollectionPeriod,
                 store_dtss_address: str,
                 store_ts_ids: Sequence[str]
                 ) -> None:
        """A DataCollectionService is a tool for regularly querying a dtss for data, and then storing the data in a
        corresponding dtss using the store function.

        Args:
            service_name: The name of the service, so it can be identified in logs.
            read_dtss_address: The address of the DtssHost service you want to read data from.
            read_ts_ids: A list of strings identifying timeseries in the read_dtss_address.
            read_period: The relative period we want to read data from.
            store_dtss_address: The address of the DtssHost service you want to store the data in.
            store_ts_ids: A list of strings that we want to store the timeseries as in the store DtssHost.
        """
        self.service_name = service_name
        self.read_dtss_address = read_dtss_address
        self.read_ts = read_ts
        self.read_period = read_period
        self.store_dtss_address = store_dtss_address
        self.store_ts_ids = store_ts_ids
        self.read_client: st.DtsClient = None
        self.store_client: st.DtsClient = None
        self.service_thread: threading.Thread = None

    def start(self) -> None:
        """Start the DataCollectionService. The first query is performed immediately after service startup."""
        self.read_client = st.DtsClient(self.read_dtss_address)
        self.store_client = st.DtsClient(self.store_dtss_address)

        self.service_thread = threading.Thread(name=self.service_name,
                                               target=ServiceLoop(service_name=self.service_name,
                                                                  read_client=self.read_client,
                                                                  read_ts=self.read_ts,
                                                                  read_period=self.read_period,
                                                                  store_client=self.store_client,
                                                                  store_ts_ids=self.store_ts_ids))
        self.service_thread.continue_loop = True
        self.service_thread.start()

    def stop(self) -> None:
        """Stop the DataCollectionService after current thread operation is done."""
        if isinstance(self.service_thread, threading.Thread):
            self.service_thread.continue_loop = False


class ServiceLoop:
    """Service loop is a container for the properties defining the actual operations of a DataCollectionService."""

    def __init__(self,
                 service_name: str,
                 read_client: st.DtsClient,
                 read_ts: Sequence[st.TimeSeries],
                 read_period: DataCollectionPeriod,
                 store_client: st.DtsClient,
                 store_ts_ids: Sequence[str], ) -> None:
        """A DataCollectionService is a tool for regularly querying a dtss for data, and then storing the data in a
        corresponding dtss using the store function.

        Args:
            service_name: The name of the service, so it can be identified in logs.
            read_client: The address of the DtssHost service you want to read data from.
            read_ts: A list of timeseries found in the read_dtss_address.
            read_period: The relative period we want to read data from.
            store_client: The address of the DtssHost service you want to store the data in.
            store_ts_ids: A list of strings identifying how we want to represent the timeseries as in the
                            DtssHost store.
        """
        self.service_name = service_name
        self.read_client = read_client
        self.read_ts = read_ts
        self.read_period = read_period
        self.store_client = store_client
        self.store_ts_ids = store_ts_ids

    def _log_msg(self, msg: str) -> str:
        """Format a logging message."""
        return f'DataCollectionService {self.service_name}: {msg}'

    def __call__(self) -> None:
        """Call performs a read and store operation at the interval defined by self.read_period."""

        def perform_read() -> st.TsVector:
            """Perform a read query that returns a ts_vector with the queried data."""
            return self.read_client.evaluate(st.TsVector(self.read_ts),
                                             self.read_period.period())

        def perform_store(store_data: st.TsVector) -> None:
            """Perform a store query that stores the data in store_data according to the ts_names in store_ts_ids."""
            self.store_client.store_ts(
                st.TsVector(
                    [st.TimeSeries(ts_id, ts) for ts_id, ts in zip(self.store_ts_ids, store_data)]
                )
            )

        t = threading.currentThread()
        # Loop when continue_loop==True.
        # In the first pass, continue loop might not have been set yet, so we assume True.
        while getattr(t, "continue_loop", True):
            read_data = None
            try:
                read_data = perform_read()
            except Exception as e:
                logging.error(self._log_msg(f'Read from read_client failed with exception: {e}'))

            if read_data:
                try:
                    perform_store(store_data=read_data)
                    logging.info(
                        f'DataCollectionService {self.service_name}: Read and store complete for {len(read_data)} timeseries.')
                except Exception as e:
                    logging.error(self._log_msg(f'Store to store_client failed with exception: {e}'))

            time.sleep(self.read_period.wait_time)

        logging.info(self._log_msg('_service_loop has stopped.'))
