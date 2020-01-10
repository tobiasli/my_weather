"""This file contains the Distributed TimeSeries Server (Dtss) host. This is a service that runs as a service and let's
me poll timeseries data either directly from the source (Netatmo API) or from the containers (local cache) available to
the Dtss. This lets gives me local storage of the data that can be queried freely."""

import typing as ty
import logging
import urllib
import os
from abc import ABC, abstractmethod

from shyft.time_series import (DtsServer, DtsClient, StringVector, TsVector, UtcPeriod, TsInfoVector)

from weather.data_sources.heartbeat import HeartbeatRepository, create_heartbeat_request
from weather.interfaces.data_collection_repository import DataCollectionRepository
from weather.interfaces.config import RepositoryConfigBase, EnvVarConfig

ConfigType = ty.Dict[str, object]


class DtssHostConfigurationError(Exception):
    """Exception raised by the DtssHost for configuration errors."""
    pass


class DtssHostError(Exception):
    """Exception raised by the DtssHost for runtime errors."""
    pass


class DtssHostConfigBase(ABC, RepositoryConfigBase):
    _unpack_props = ['port_num', 'container_directory', 'data_collection_repositories']

    @property
    @abstractmethod
    def port_num(self) -> int:
        """This property must contain the port number for the DtssHost."""

    @property
    @abstractmethod
    def container_directory(self) -> str:
        """This property must contain the path to the container directory for the DtssHost."""

    @property
    @abstractmethod
    def data_collection_repositories(self) -> ty.Sequence[
        ty.Tuple[ty.Type[DataCollectionRepository], ty.Mapping[str, ty.Any]]]:
        """This property must contain the configured DataCollectionRepository's for the DtssHost."""

    @property
    @abstractmethod
    def log_directory(self) -> str:
        """Folder for containing log files from the services."""

class DtssHostEnvironmentVariablesConfig(DtssHostConfigBase, EnvVarConfig):
    """Netatmo config information fetched partially from environment_variables."""

    def __init__(self,
                 port_num_var: str,
                 container_directory_var: str,
                 log_directory_var: str,
                 data_collection_repositories: ty.Sequence[
                     ty.Tuple[ty.Type[DataCollectionRepository], ty.Mapping[str, ty.Any]]]
                 ) -> None:
        self.log_directory_var = log_directory_var
        self.dtss_port_num_var = self.verify_env_var(port_num_var)
        self.container_directory_var = self.verify_env_var(container_directory_var)

        self._data_collection_repositories = data_collection_repositories

    @property
    def port_num(self) -> int:
        """Return the username for the Netatmo instance."""
        return int(self.get_env_var(self.dtss_port_num_var))

    @property
    def container_directory(self) -> str:
        """Return path to the container directory for storing time series data."""
        return self.get_env_var(self.container_directory_var)

    @property
    def log_directory(self) -> str:
        """Return the log file directory for the services."""
        return self.get_env_var(self.log_directory_var)

    @property
    def data_collection_repositories(self) -> \
            ty.Sequence[ty.Tuple[ty.Type[DataCollectionRepository], ty.Mapping[str, ty.Any]]]:
        """Return the choice of direct login: If we initiate the Netatmo API on construction or if we wait."""
        return self._data_collection_repositories


class DtssHost:
    """DtssHost is a data service that accepts queries for TimeSeries data using url identifiers and UtcPeriods.
    The service handles calls both for source systems (i.e. Netatmo api) and data calls directed to a local
    container hosting the same data for faster queries."""

    def __init__(self,
                 port_num: int,
                 data_collection_repositories: ty.Sequence[
                     ty.Tuple[ty.Type[DataCollectionRepository], ty.Mapping[str, ty.Any]]],
                 container_directory: str) -> None:
        """DtssHost constructor needs a port number for the service end point. The data collection repositories are for
        collecting the source data of interest, and the container directory is where the timeseries files are stored for
        the local database.

        Args:
            port_num: The listening port the DtsServer uses.
            data_collection_repositories: The data collection repositories that we are able to collect data from.
            container_directory: The disk location where we look for and store timeseries.
        """

        self.port_num = port_num

        self.data_collection_repositories = data_collection_repositories
        self.repos = None
        self.initiate_repos()
        self.container_directory = container_directory

        # Initialize and configure server:
        self.dtss: DtsServer = None

    def initiate_repos(self) -> None:
        """Construct all the data collection repositories."""
        # Build a dictionary containing every available repository.
        repos = [repo(**config) for repo, config in self.data_collection_repositories]
        self.repos: ty.Dict[str, DataCollectionRepository] = {repo.name: repo for repo in repos}

        # The HeartbeatRepository contains callbacks that return arbitrary responses to all calls.
        # This lets  ut verify that the service is running.
        self.repos[HeartbeatRepository.name] = HeartbeatRepository(host=self)

    def make_server(self) -> DtsServer:
        """Construct and configure our DtsServer."""
        # noinspection PyArgumentList
        dtss = DtsServer()
        dtss.set_listening_port(self.port_num)
        # dtss.set_auto_cache(True)
        dtss.cb = self.read_callback
        dtss.find_cb = self.find_callback

        # TimeSeries storage.
        # Check if container directory exists, if not, create it.
        if not os.path.exists(self.container_directory):
            os.mkdir(self.container_directory)

        # Check if repo containers exists, if not, create them, then add as containers.
        for repo in self.repos:
            repo_container = os.path.join(self.container_directory, repo)
            if not os.path.exists(repo_container):
                os.mkdir(repo_container)
            dtss.set_container(repo, repo_container)

        return dtss

    def start(self) -> None:
        """Start the DtsServer service running at port self.dtss_port_num."""
        if self.dtss:
            logging.info('Attempted to start a server that is already running.')
        else:
            self.dtss = self.make_server()
            # noinspection PyArgumentList
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
            logging.info(f'DtssHost stop at port {self.port_num}.')
            self.dtss.clear()
            del self.dtss
            self.dtss = None

    def restart(self) -> None:
        """Restart the DtsServer."""
        self.stop()
        self.initiate_repos()  # Reinitialize repositories.
        self.start()

    @property
    def address(self) -> str:
        """Return the full service address of the DtssHost."""
        return f'localhost:{self.port_num}'
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
