"""This file contains the Distributed TimeSeries Server (Dtss) host. This is a service that runs as a service and let's me poll
timeseries data either directly from the source (Netatmo API) or from the containers (local cache) available to the
Dtss. This lets gives me local storage of the data that can be queried freely."""

from typing import Dict, Sequence
from shyft.repository.interfaces import TsRepository
from shyft.api import DtsServer
import logging
import socket

ConfigType = Dict[str, object]


class DtssHostConfigurationError(Exception):
    pass


class DtssHost:

    def __init__(self,
                 dtss_port_num: int,
                 data_collection_repositories: Sequence[TsRepository],
                 container_directory: str) -> None:
        """DtssHost is a data service that accepts queries for TimeSeries data using url identifiers and UtcPeriods.
        The service handles calls both for source systems (i.e. Netatmo api) and data calls directed to a local
        container hosting the same data for faster queries."""
        self.dtss_port_num = dtss_port_num
        self.callbacks = {repo.schema: repo for repo in data_collection_repositories}
        self.container_directory = container_directory

        # Initialize and configure server:
        self.dtss: DtsServer = None

    def make_server(self) -> DtsServer:
        """Construct and configure our DtsServer."""
        self.dtss = DtsServer()
        self.dtss.set_listening_port(self.dtss_port_num)
        self.dtss.set_auto_cache(True)
        # self.dtss.cb = self.dtss_read_callback
        # self.dtss.find_cb = self.dtss_find_callback
        # self. dtss.store_ts_cb = self.dtss_store_callback

    def start(self) -> None:
        """Start the DtsServer service running at port self.dtss_port_num."""
        if self.dtss:
            logging.info('Attempted to start a server that is allready running.')
        else:
            self.make_server()
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
