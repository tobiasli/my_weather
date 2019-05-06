"""This file contains the Distributed TimeSeries Server (Dtss) host. This is a service that runs as a service and let's
me poll timeseries data either directly from the source (Netatmo API) or from the containers (local cache) available to
the Dtss. This lets gives me local storage of the data that can be queried freely."""

from typing import Dict, Any, Sequence, List, Tuple
import logging
import urllib
import os

import numpy as np
from shyft.api import (DtsServer, DtsClient, StringVector, TsVector, UtcPeriod, TsInfoVector, POINT_INSTANT_VALUE, TsInfo,
                       TimeSeries)

from weather.interfaces.data_collection_repository import DataCollectionRepository
from weather.data_collection.netatmo import NetatmoRepository
from weather.test.utilities import MockRepository1, MockRepository2  # Used for tests.
from weather.utilities.create_ts import create_ts



ConfigType = Dict[str, object]


_DEFAULT_DATA_COLLECTION_REPO_TYPES = (NetatmoRepository, MockRepository1, MockRepository2)
_DEFAULT_DATA_COLLECTION_REPO_TYPE_LOOKUP = {repo.name: repo for repo in _DEFAULT_DATA_COLLECTION_REPO_TYPES}


class DtssHostConfigurationError(Exception):
    """Exception raised by the DtssHost for configuration errors."""
    pass


class DtssHostError(Exception):
    """Exception raised by the DtssHost for runtime errors."""
    pass


class DtssHost:
    """DtssHost is a data service that accepts queries for TimeSeries data using url identifiers and UtcPeriods.
    The service handles calls both for source systems (i.e. Netatmo api) and data calls directed to a local
    container hosting the same data for faster queries."""

    def __init__(self,
                 dtss_port_num: int,
                 data_collection_repositories: Dict[str, Dict[str, Any]],
                 container_directory: str) -> None:
        """DtssHost constructor needs a port number for the service end point. The data collection repositories are for
        collecting the source data of interest, and the container directory is where the timeseries files are stored for
        the local database.

        Args:
            dtss_port_num: The listening port the DtsServer uses.
            data_collection_repositories: The data collection repositories that we are able to collect data from.
            container_directory: The disk location where we look for and store timeseries.
        """
        self.dtss_port_num = dtss_port_num

        # Build a dictionary containing every available repository.
        self.repos: Dict[str, DataCollectionRepository] = {
            name: _DEFAULT_DATA_COLLECTION_REPO_TYPE_LOOKUP[name](**config)
            for name, config in data_collection_repositories.items()
            if name in _DEFAULT_DATA_COLLECTION_REPO_TYPE_LOOKUP
            }
        # The HeartbeatRepository contains callbacks that return arbitrary responses to all calls.
        # This lets  ut verify that the service is running.
        self.repos[HeartbeatRepository.name] = HeartbeatRepository(host=self)

        self.container_directory = container_directory

        # Initialize and configure server:
        self.dtss: DtsServer = None

    def make_server(self) -> DtsServer:
        """Construct and configure our DtsServer."""
        dtss = DtsServer()
        dtss.set_listening_port(self.dtss_port_num)
        # dtss.set_auto_cache(True)
        dtss.cb = self.read_callback
        dtss.find_cb = self.find_callback

        # Set all container directories, with container names matching folders in container directory:
        # TODO: Auto-create container folders for each configured repository if they are not present.
        for container in os.listdir(self.container_directory):
            dtss.set_container(container, os.path.join(self.container_directory, container))

        return dtss

    def start(self) -> None:
        """Start the DtsServer service running at port self.dtss_port_num."""
        if self.dtss:
            logging.info('Attempted to start a server that is already running.')
        else:
            self.dtss = self.make_server()
            self.dtss.start_async()
            logging.info(f'DtssHost start at {self.address}. Repositories: {[repo for repo in self.repos]}')

        try:
            # Verify that server is running:
            c = DtsClient(self.address)
            response = c.find(create_heartbeat_request('startup verification'))
            del c
            if not response:
                raise DtssHostError('DtssHost is not responding to expected calls.')
        except DtssHostError:
            self.stop()

    def stop(self) -> None:
        """Stop the DtssHost service running at port self.dtss_port_num."""
        if not self.dtss:
            logging.info('DtssHost attempted to stop a server that isn''t running.')
        else:
            logging.info(f'DtssHost stop at port {self.dtss_port_num}.')
            self.dtss.clear()
            del self.dtss
            self.dtss = None

    @property
    def address(self) -> str:
        """Return the full service address of the DtssHost."""
        return f'localhost:{self.dtss_port_num}'
        # return f'{socket.gethostname()}:{self.dtss_port_num}'

    def read_callback(self, ts_ids: StringVector, read_period: UtcPeriod) -> TsVector:
        """DtssHost.read_callback accepts a set of urls identifying timeseries and a read period and returns bound
        TimeSeries in a TsVector that contain data at least covering the read_period.

        Args:
            ts_ids: A sequence of strings identifying specific timeseries available from the underlying
                    DataCollectionRepository's.
            read_period: A period defined by a utc timestamp for the start and end of the analysis period.

        Returns:
            A TsVector containing the resulting timeseries containing data enough to cover the query period.
        """
        logging.info(f'DtssHost received read_callback for {len(ts_ids)} ts_ids for period {read_period}.')

        data = dict()  # Group ts_ids by repo.name (scheme).
        for enum, ts_id in enumerate(ts_ids):
            repo_name = self.get_repo_name_from_url(ts_id)
            if repo_name not in data:
                data[repo_name] = []
            data[repo_name].append(dict(enum=enum, ts_id=ts_id, ts=None))

        for repo_name in data:
            tsvec = self.repos[repo_name].read_callback(
                ts_ids=StringVector([ts['ts_id'] for ts in data[repo_name]]),
                read_period=read_period)
            for index, ts in enumerate(tsvec):
                data[repo_name][index]['ts'] = ts

        # Collapse nested lists and sort by initial enumerate:
        transpose_data = []
        for items in data.values():
            transpose_data.extend(items)
        sort = sorted(transpose_data, key=lambda item: item['enum'])

        return TsVector([item['ts'] for item in sort])

    def find_callback(self, query: str) -> TsInfoVector:
        """DtssHost.find:callback accepts a query string and returns metadata for any timeseries found."""
        repo_name = self.get_repo_name_from_url(query)
        return self.repos[repo_name].find_callback(query=query)

    def get_repo_name_from_url(self, url: str) -> str:
        """Get the repo name (scheme) from a url, so that we can route it correctly."""
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in self.repos:
            raise DtssHostError(f'ts_id scheme {parsed.scheme} does not match any '
                                f'that are available for the DtssHost: '
                                f'{", ".join(scheme for scheme in self.repos)}')
        return parsed.scheme


class HeartbeatRepository(DataCollectionRepository):
    """The HeartbeatRepository is a dummy repository used to check if the DtssHost's DtsServer is running correctly on
    the correct port. The read_ and find_callbacks always return something, just so we can verify that the DtsServer is
    running."""

    name = 'heartbeat'

    def __init__(self, host: DtssHost):
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
        """This callback is passed as the default read_callback for a shyft.api.DtsServer.

        Args:
            ts_ids: A sequence of strings identifying specific timeseries available from the netatmo login. Matches the
                    formatting provided by DataCollectionRepository.create_ts_id()
            read_period: A period defined by a utc timestamp for the start and end of the analysis period.

        Returns:
            A TsVector containing the resulting timeseries containing data enough to cover the query period.
        """
        logging.info(f'DtssHost Heartbeat read_callback at {self.host.address}.')
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
        """This callback is passed as the default find_callback for a shyft.api.DtsServer.

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