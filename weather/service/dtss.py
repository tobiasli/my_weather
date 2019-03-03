"""This file contains the Distributed TimeSeries Server (Dtss) host. This is a service that runs as a service and let's
me poll timeseries data either directly from the source (Netatmo API) or from the containers (local cache) available to
the Dtss. This lets gives me local storage of the data that can be queried freely."""

from typing import List, Dict, Any, Sequence, Type
from shyft.api import DtsServer, StringVector, TsVector, UtcPeriod
from weather.data_collection.netatmo import NetatmoRepository
from weather.interfaces.data_collection_repository import DataCollectionRepository
from weather.test.utilities import MockRepository
import logging
import socket
import urllib

ConfigType = Dict[str, object]


_DEFAULT_DATA_COLLECTION_REPO_TYPES = (NetatmoRepository, MockRepository)
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
            data_collection_repos: A sequence of DataCollectionRepository that are available for the DtssHost.
        """
        self.dtss_port_num = dtss_port_num

        self.repos: List[DataCollectionRepository] = [_DEFAULT_DATA_COLLECTION_REPO_TYPE_LOOKUP[name](**config)
                                                      for name, config in data_collection_repositories
                                                      if name in _DEFAULT_DATA_COLLECTION_REPO_TYPE_LOOKUP]
        self.read_callbacks = {repo.name: repo.read_callback for repo in self.repos}
        self.find_callbacks = {repo.name: repo.find_callback for repo in self.repos}
        self.container_directory = container_directory

        # Initialize and configure server:
        self.dtss: DtsServer = None

    def make_server(self) -> DtsServer:
        """Construct and configure our DtsServer."""
        dtss = DtsServer()
        dtss.set_listening_port(self.dtss_port_num)
        dtss.set_auto_cache(True)
        dtss.cb = self.read_callback
        # self.dtss.find_cb = self.dtss_find_callback
        # self.dtss.store_ts_cb = self.dtss_store_callback

        return dtss

    def start(self) -> None:
        """Start the DtsServer service running at port self.dtss_port_num."""
        if self.dtss:
            logging.info('Attempted to start a server that is already running.')
        else:
            self.dtss = self.make_server()
            logging.info(f'DtsServer start at {self.dtss_port_num}.')
            self.dtss.start_async()

    def stop(self) -> None:
        """Stop the DtsServer service running at port self.dtss_port_num."""
        if not self.dtss:
            logging.info('Attempted to stop a server that isn''t running.')
        else:
            logging.info(f'DtsServer stop at port {self.dtss_port_num}.')
            self.dtss.clear()
            del self.dtss
            self.dtss = None

    @property
    def address(self) -> str:
        """Return the full service address of the DtsServer."""
        return f'{socket.gethostname()}:{self.dtss_port_num}'

    def read_callback(self, ts_ids: StringVector, read_period: UtcPeriod) -> TsVector:
        """DtssHost.read_callback accepts a set of urls identifying timeseries that in turn are """

        data = dict()  # Group ts_ids by repo.name (scheme).
        for enum, ts_id in enumerate(ts_ids):
            parsed = urllib.parse.urlparse(ts_id)
            if parsed.scheme not in self.read_callbacks:
                raise DtssHostError(f'ts_id scheme {parsed.scheme} does not match any '
                                    f'that are available for the DtssHost: '
                                    f'{", ".join(scheme for scheme in self.read_callbacks)}')
            if not parsed.scheme in data:
                data[parsed.scheme] = []
            data[parsed.scheme].append(dict(enum=enum, ts_id=ts_id, ts=None))

        for repo_name in data:
            tsvec = self.read_callbacks[repo_name](
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


