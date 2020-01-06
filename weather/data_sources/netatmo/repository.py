"""This module contains NetatmoRepository which contains all necessary methods for a DtssHost to handle communications
with a Netatmo weather station service."""

import typing as ty
from abc import abstractmethod, ABC
import logging

import numpy as np
import lnetatmo
from shyft.api import (StringVector, UtcPeriod, TimeAxisByPoints, TimeSeries, POINT_INSTANT_VALUE, TsVector,
                       TsInfoVector,
                       TsInfo, time, utctime_now, Calendar)

from weather.data_sources.netatmo.domain import NetatmoDomain
from weather.interfaces.config import RepositoryConfigBase, EnvVarConfig, EncryptedEnvVarConfig
from weather.interfaces.data_collection_repository import DataCollectionRepository
from weather.data_sources.netatmo.identifiers import parse_ts_id, parse_ts_query
from weather.utilities import rate_limiter

DeviceMetadataType = ty.Dict[str, object]
TimeType = ty.Union[float, int, time]
Number = ty.Union[float, int]
NoneType = type(None)
NanType = type(np.nan)


class NetatmoRepositoryError(Exception):
    """Exceptions raised by the NetatmoRepository."""
    pass


class NetatmoRepository(DataCollectionRepository):
    """The NetatmoRepository contains methods complying to the the DataCollectionRepository standard."""
    name = 'netatmo'

    def __init__(self, username: str,
                 password: str,
                 client_id: str,
                 client_secret: str,
                 api_limits: ty.Dict[str, ty.Dict[str, int]] = None, direct_login: bool = True) -> None:
        """

        :param username: Netatmo login username.
        :param password: Netatmo login password.
        :param client_id: Client id for communicating with the netatmo api.
        :param client_secret: Client id for communicating with the netatmo api.
        :param api_limits: Parameters for the RateLimiters that make sure we do not trip api communication limits.
        :param direct_login: Bool dictating if the api connection is established on init or if we want to initiate
        the api connection later.
        """

        api_limits = api_limits

        # We don't want to breach the Netatmo API rate limiting policy, so we apply rate limiters to the functions
        # performing API calls to the Netatmo servers:
        self.rate_limiters = [rate_limiter.RateLimiter(**api_limit) for api_limit in api_limits.values()]
        for limiter in self.rate_limiters:
            self._get_measurements_block = limiter.rate_limit_decorator(self._get_measurements_block)

        self.auth: lnetatmo.ClientAuth = None
        self.device_data: lnetatmo.WeatherStationData = None
        self.domain: NetatmoDomain = None

        if direct_login:  # Sometimes we want to use methods that don't need login.
            self.auth = lnetatmo.ClientAuth(clientId=client_id, clientSecret=client_secret, username=username,
                                            password=password,
                                            scope='read_station')
            self.device_data = lnetatmo.WeatherStationData(self.auth)

            self.domain = NetatmoDomain(self.device_data.stations)

        # noinspection PyArgumentList
        self.utc = Calendar()

    def wait_for_rate_limiters(self) -> None:
        """Check with the rate limiters, and wait if needed."""
        for limiter in self.rate_limiters:
            limiter.check_next_and_wait()

    def add_action_timestamp_to_rate_limiters(self, timestamp: TimeType) -> None:
        """Add a current action to rate limiters, so they keep track of actions."""
        for limiter in self.rate_limiters:
            limiter.add_action_timestamp(timestamp)

    @staticmethod
    def set_none_to_nan(values: ty.Sequence[ty.Union[Number, NoneType]]) -> ty.Sequence[ty.Union[Number, NanType]]:
        """Method that replaces None with np.nan in a sequence."""
        return [np.nan if value is None else value for value in values]

    def _get_measurements_block(self, *,
                                device_id: str,
                                module_id: str,
                                measurements: ty.Sequence[str],
                                utc_period: UtcPeriod = None
                                ) -> TsVector:
        """Get data for a specific device and set of measurements. utc_period is the timespan for which we ask for
        data, but it is optional, as utc_period=None asks for the longest possible timespan of data.

        NB: Calls are limited to 1024 values. Must split to get all data in period (50 req pr sec, 500 req pr hour).

        Args:
            device_id: Unique identifier for the netatmo device.
            module_id: Unique identifier for the netatmo module (can be None, '').
            measurements: A ty.Sequence of strings representing the measurements we want to fetch.
            utc_period: Inclusive start/end. The period we want data for (if none, the longest possible period
                        (up to 1024 values).

        Returns:
            A TsVector with timeseries containing data for each measurement type, in the order of the input.
        """

        date_start = float(utc_period.start) if utc_period else None
        date_end = float(utc_period.end) if utc_period else None

        self.wait_for_rate_limiters()
        self.add_action_timestamp_to_rate_limiters(utctime_now())

        measurement_types_str = ','.join([m for m in measurements])

        data = self.device_data.getMeasure(
            device_id=device_id,
            module_id=module_id,
            scale='max',
            mtype=measurement_types_str,
            date_begin=date_start,
            date_end=date_end)

        if not data['body']:
            # noinspection PyArgumentList
            output = [TimeSeries() for _ in measurements]
        else:
            t = [float(timestamp) for timestamp in data['body'].keys()]
            # Add an additional timestep fmod(dt) forward in time to indicate the validness of the last value.
            dt_list = [t2 - t1 for t1, t2 in zip(t[0:-2], t[1:-1])]
            dt_mode = max(set(dt_list), key=dt_list.count)
            ta = TimeAxisByPoints(t + [t[-1] + dt_mode])

            values_pr_time = [value for value in data['body'].values()]
            values = list(map(list, zip(*values_pr_time)))

            # Remove nan:
            output = [TimeSeries(ta, self.set_none_to_nan(vector), POINT_INSTANT_VALUE) for vector in values]

        return TsVector(output)

    def get_measurements(self, *,
                         device_id: str,
                         module_id: str,
                         measurements: ty.Sequence[str],
                         utc_period: UtcPeriod
                         ) -> TsVector:
        """Get data for a specific device and set of measurements. utc_period defines the data period.
        Netatmo only returns data in 1024 point chunks, so this method performs multiple calls to retrieve all queried
        data.

        Args:
            device_id: Unique identifier for the netatmo device.
            module_id: Unique identifier for the netatmo module (can be None, '').
            measurements: A ty.Sequence of strings representing the measurements we want to fetch.
            utc_period: Inclusive start/end. The period we want data for (if none, the longest possible period
                        (up to 1024 values).

        Returns:
            A TsVector with timeseries containing data for each measurement type, in the order of the input.
        """

        result_end = utc_period.start
        # noinspection PyArgumentList
        output = [TimeSeries() for _ in measurements]
        while result_end < utc_period.end:
            utc_period = UtcPeriod(result_end, utc_period.end)  # Define a UtcPeriod for the remaining data.

            result = self._get_measurements_block(device_id=device_id,
                                                  module_id=module_id,
                                                  measurements=measurements,
                                                  utc_period=utc_period)

            if not any([bool(ts) for ts in result]):  # None data in period. Return blank.
                break
            for ind, res in enumerate(result):
                if res:
                    if not output[ind]:
                        output[ind] = res
                    else:
                        output[ind] = output[ind].extend(res)

            result_end = result[0].time_axis.time_points_double[-1]  # Set the start of the new calls UtcPeriod.
            # noinspection PyArgumentList
            logging.info(f'Got {len(result[0])} data points from '
                         f'{self.utc.to_string(result[0].time_axis.time_points_double[0])} to '
                         f'{self.utc.to_string(result_end)}')

        return TsVector(output)

    def read(self, list_of_ts_id: ty.Sequence[str], period: UtcPeriod) -> ty.Dict[str, TimeSeries]:
        """Take a sequence of strings identifying specific timeseries and get data from these series according to
        the timespan defined in period.

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
            ts_ids: A sequence of strings identifying specific timeseries available from the netatmo login.
            read_period: A period defined by a utc timestamp for the start and end of the analysis period.

        Returns:
            A TsVector containing the resulting timeseries containing data enough to cover the query period.
        """
        data = dict()  # Group ts_ids by repo.name (scheme).
        for enum, ts_id in enumerate(ts_ids):
            ts_id_props = parse_ts_id(ts_id=ts_id)
            measurement = self.domain.get_measurement(**ts_id_props)

            if measurement.source_name not in data:
                data[measurement.source_name] = []
            data[measurement.source_name].append(
                dict(enum=enum, ts=None, measurement=measurement))

        for source_name in data:
            measurements = [meas['measurement'] for meas in data[source_name]]

            device_id = measurements[0].device.id
            module_id = measurements[0].module.id if measurements[0].module else None
            measurements = [m.data_type.name for m in measurements]
            tsvec = self.get_measurements(device_id=device_id,
                                          module_id=module_id,
                                          measurements=measurements,
                                          utc_period=read_period
                                          )
            for index, ts in enumerate(tsvec):
                data[source_name][index]['ts'] = ts

        # Collapse nested lists and sort by initial enumerate:
        transpose_data = []
        for items in data.values():
            transpose_data.extend(items)
        sort = sorted(transpose_data, key=lambda item: item['enum'])

        return TsVector([item['ts'] for item in sort])

    def find_callback(self, query: str) -> TsInfoVector:
        """This callback is passed as the default find_callback for a shyft.api.DtsServer.

        Args:
            query: The url representing a relevant query for this DataCollectionRepository.

        Returns:
            A sequence of results matching the query.
        """

        return self.find(query)

    def find(self, query: str) -> TsInfoVector:
        """Check if data matching the query exists in the data source.

        Args:
            query: The url representing a relevant query for this DataCollectionRepository.

        Returns:
            A sequence of results matching the query.
        """
        info = parse_ts_query(ts_query=query)

        meas = self.domain.get_measurement(**info)

        # noinspection PyArgumentList
        tsi = TsInfo(
            name=meas.ts_id,
            point_fx=meas.data_type.point_interpretation,
            delta_t=np.nan,
            olson_tz_id=meas.device.place['timezone'],
            data_period=UtcPeriod(meas.source.date_setup, meas.source.dashboard_data['time_utc']),
            created=meas.source.date_setup,
            modified=meas.source.dashboard_data['time_utc']
        )

        # noinspection PyArgumentList
        tsiv = TsInfoVector()
        tsiv.append(tsi)
        return tsiv

    def read_forecast(self, list_of_fc_id: ty.List[str], period: UtcPeriod):
        """
        read and return the newest forecast that have the biggest overlap with specified period
        note that we should check that the semantic of this is reasonable
        """
        raise NotImplementedError("read_forecast")

    def store(self, timeseries_dict: ty.Dict[str, TimeSeries]):
        """ Store the supplied time-series to the underlying db-system.
            Parameters
            ----------
            timeseries_dict: dict string:timeseries
                the keys are the wanted ts(-path) names
                and the values are shyft api.time-series.
                If the named time-series does not exist, create it.
        """
        raise NotImplementedError("read_forecast")


class NetatmoConfigBase(ABC, RepositoryConfigBase):
    """Metaclass defining the properties that are expected from a NetatmoConfig."""

    _unpack_props = ['username', 'password', 'client_id', 'client_secret', 'api_limits', 'direct_login']

    @property
    @abstractmethod
    def username(self) -> str:
        """This property must contain the username for Netatmo."""

    @property
    @abstractmethod
    def password(self) -> str:
        """This property must contain the password for Netatmo."""

    @property
    @abstractmethod
    def client_id(self) -> str:
        """This property must contain the client_id for Netatmo."""

    @property
    @abstractmethod
    def client_secret(self) -> str:
        """This property must contain the client_secret for Netatmo."""

    @property
    @abstractmethod
    def api_limits(self) -> ty.Dict[str, ty.Dict[str, int]]:
        """This property must contain the optional api_limits for Netatmo."""

    @property
    @abstractmethod
    def direct_login(self) -> bool:
        """This property must contain the optional direct_login for Netatmo."""


class NetatmoEnvVarConfig(NetatmoConfigBase, EnvVarConfig):
    """Netatmo config information fetched partially from environment_variables."""

    def __init__(self,
                 username_var: str,
                 password_var: str,
                 client_id_var: str,
                 client_secret_var: str,
                 api_limits: ty.Dict[str, ty.Dict[str, int]] = None,
                 direct_login: bool = True
                 ) -> None:
        self.username_var = self.verify_env_var(username_var)
        self.password_var = self.verify_env_var(password_var)
        self.client_id_var = self.verify_env_var(client_id_var)
        self.client_secret_var = self.verify_env_var(client_secret_var)

        self._direct_login = direct_login
        self._api_limits = api_limits or {}

    @property
    def username(self) -> str:
        """Return the username for the Netatmo instance."""
        return self.get_env_var(self.username_var)

    @property
    def password(self) -> str:
        """Return the username for the Netatmo instance."""
        return self.get_env_var(self.password_var)

    @property
    def client_id(self) -> str:
        """Return the username for the Netatmo instance."""
        return self.get_env_var(self.client_id_var)

    @property
    def client_secret(self) -> str:
        """Return the client_secret for the Netatmo instance."""
        return self.get_env_var(self.client_secret_var)

    @property
    def direct_login(self) -> bool:
        """Return the choice of direct login: If we initiate the Netatmo API on construction or if we wait."""
        return self._direct_login

    @property
    def api_limits(self) -> ty.Dict[str, ty.Dict[str, int]]:
        """Return the api limits for the RateLimiter for the Netatmo instance."""
        return self._api_limits


class NetatmoEncryptedEnvVarConfig(NetatmoConfigBase, EncryptedEnvVarConfig):
    """Netatmo config information fetched partially from environment_variables."""

    def __init__(self,
                 username_var: str,
                 password_var: str,
                 client_id_var: str,
                 client_secret_var: str,
                 password: str,
                 salt: str,
                 api_limits: ty.Dict[str, ty.Dict[str, int]] = None,
                 direct_login: bool = True
                 ) -> None:
        self.username_var = self.verify_env_var(username_var)
        self.password_var = self.verify_env_var(password_var)
        self.client_id_var = self.verify_env_var(client_id_var)
        self.client_secret_var = self.verify_env_var(client_secret_var)

        self._direct_login = direct_login
        self._api_limits = api_limits or {}

        super(NetatmoEncryptedEnvVarConfig, self).__init__(password=password, salt=salt)

    @property
    def username(self) -> str:
        """Return the username for the Netatmo instance."""
        return self.get_env_var(self.username_var)

    @property
    def password(self) -> str:
        """Return the username for the Netatmo instance."""
        return self.get_env_var(self.password_var)

    @property
    def client_id(self) -> str:
        """Return the username for the Netatmo instance."""
        return self.get_env_var(self.client_id_var)

    @property
    def client_secret(self) -> str:
        """Return the client_secret for the Netatmo instance."""
        return self.get_env_var(self.client_secret_var)

    @property
    def direct_login(self) -> bool:
        """Return the choice of direct login: If we initiate the Netatmo API on construction or if we wait."""
        return self._direct_login

    @property
    def api_limits(self) -> ty.Dict[str, ty.Dict[str, int]]:
        """Return the api limits for the RateLimiter for the Netatmo instance."""
        return self._api_limits
