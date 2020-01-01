"""A DataCollectionRepository is a subclass of a TsRepository that communicates well with a DtssHost as a backend
callback provider. The read_callback and find_callback can used by a dtsServer directly as cb and find_cb
respectively."""

from abc import abstractmethod

from shyft.api import StringVector, UtcPeriod, TsVector, TsInfoVector
from shyft.repository.interfaces import TsRepository


class DataCollectionRepository(TsRepository):
    """DataCollectionRepository is an extension of the TsRepository that also provides callbacks for """

    @property
    @abstractmethod
    def name(self) -> str:
        """DataCollectionRepositories are identified by their name, which is an identifier string (isidentifier())
        property that is used to route Dtss read and write callbacks to the DataCollectionRepository."""
        pass

    @abstractmethod
    def read_callback(self, ts_ids: StringVector, read_period: UtcPeriod) -> TsVector:
        """This callback is passed as the default read_callback for a shyft.api.DtsServer.

        Args:
            ts_ids: A sequence of strings identifying specific timeseries available from the netatmo login. Matches the
                    formatting provided by DataCollectionRepository.create_ts_id()
            read_period: A period defined by a utc timestamp for the start and end of the analysis period.

        Returns:
            A TsVector containing the resulting timeseries containing data enough to cover the query period.
        """
        pass

    @abstractmethod
    def find_callback(self, query: str) -> TsInfoVector:
        """This callback is passed as the default find_callback for a shyft.api.DtsServer.

        Args:
            query: The url representing a relevant query for this DataCollectionRepository. Matches the formatting
                   provided by DataCollectionRepository.create_ts_query()

        Returns:
            A sequence of results matching the query.
        """

        pass
