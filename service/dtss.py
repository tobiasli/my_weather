"""This file contains the Distributed TimeSeries Server (Dtss) host. This is a service that runs as a service and let's me poll
timeseries data either directly from the source (Netatmo API) or from the containers (local cache) available to the
Dtss. This lets gives me local storage of the data that can be queried freely."""

from typing import Dict, Sequence
from shyft.repository.interfaces import TsRepository

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
        self.callbacks = {repo.schema: repo() for repo in data_collection_repositories}
        self.container_directory = container_directory
