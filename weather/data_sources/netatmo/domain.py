"""Definitions of what we can expect of information from a Netatmo configuration."""
from weather.utilities import data_class, camel_converter
from weather.data_sources.netatmo.identifiers import create_ts_query, create_ts_store_id
from typing import List, Union, Iterable, Dict, Any
from shyft.time_series import time, Calendar, point_interpretation_policy as point_fx, TimeSeries
import lnetatmo

TimeType = Union[float, int, time]
Number = Union[float, int]


class NetatmoDomainError(Exception):
    pass


class Time(time):
    """Time acts exactly the same as shyft.time_series.time, but has a human legible __repr__ in UTC-time."""

    def __repr__(self) -> str:
        utc = Calendar()
        return f'Time({repr(utc.to_string(self))})'


class NetatmoMeasurementType:
    """Class for representing a Netatmo measurement."""
    name: str
    unit: str

    def __init__(self, name: str, unit: str, point_interpretation: point_fx) -> None:
        self.name = name
        self.unit = unit
        self.point_interpretation = point_interpretation
        self.name_lower = camel_converter.convert(name)

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(name="{self.name}", unit="{self.unit}")'

    def __eq__(self, other: "NetatmoMeasurementType") -> bool:
        if not isinstance(other, NetatmoMeasurementType):
            return False
        return (self.name, self.unit, self.point_interpretation) == (other.name, other.unit, other.point_interpretation)


class NetatmoMeasurementTypes(data_class.DataClass):
    """Handle a set of NetatmoMeasurements"""

    def __init__(self, measurement_types: Iterable[NetatmoMeasurementType]) -> None:
        super(NetatmoMeasurementTypes, self).__init__()
        self.dict = {meas.name_lower: meas for meas in measurement_types}
        self.dir = sorted([meas.name_lower for meas in measurement_types] + ['get_measurement'])

    def get_measurement(self, name: str) -> NetatmoMeasurementType:
        """Return a NetatmoMeasurement given the measurements full name."""
        return next(meas for meas in self.dict.values() if meas.name == name)


class NetatmoMeasurement:
    """Class defining a specific Netatmo measurement."""
    data_type: NetatmoMeasurementType
    module: 'NetatmoModule'
    all_measurements: List["NetatmoMeasurement"] = list()  # An index of all instances of NetatmoMeasurement.

    def __init__(self, *, station: 'NetatmoStation', data_type: NetatmoMeasurementType,
                 module: 'NetatmoModule' = None) -> None:
        """Build a Netatmo measurement using the module and the type of measurement needed."""
        self.station = station
        self.module = module
        self.data_type = data_type
        self.all_measurements.append(self)

    @property
    def measurement_name(self) -> str:
        """A representation of the name of the measurement."""
        return f'{self.station.name}\\{self.module.name}\\{self.data_type.name}'

    @property
    def module_id(self) -> Union[str, None]:
        """Return the id of the module that this measurement comes from."""
        return self.module.id if self.module else None

    @property
    def ts_id(self) -> str:
        """Create the proper ts_id for the measurement."""
        return create_ts_store_id(station_name=self.station.name,
                                  module_name=self.module.name,
                                  data_type=self.data_type.name)

    @property
    def ts_query(self) -> str:
        """Create the proper ts_query for the measurement."""
        return create_ts_query(station_name=self.station.name, module_name=self.module.name,
                               data_type=self.data_type.name)

    @property
    def time_series(self) -> TimeSeries:
        """Return a TimeSeries representation of measurement."""
        return TimeSeries(self.ts_id)


_measurements = [
    ('Temperature', '°C', point_fx.POINT_INSTANT_VALUE),
    ('CO2', 'ppm', point_fx.POINT_INSTANT_VALUE),
    ('Humidity', '%', point_fx.POINT_INSTANT_VALUE),
    ('Pressure', 'mbar', point_fx.POINT_INSTANT_VALUE),
    ('Noise', 'db', point_fx.POINT_INSTANT_VALUE),
    ('Rain', 'mm', point_fx.POINT_INSTANT_VALUE),
    ('WindStrength', 'km / h', point_fx.POINT_INSTANT_VALUE),
    ('WindAngle', 'angles', point_fx.POINT_INSTANT_VALUE),
    ('Guststrength', 'km / h', point_fx.POINT_INSTANT_VALUE),
    ('GustAngle', 'angles', point_fx.POINT_INSTANT_VALUE)
]

types = NetatmoMeasurementTypes([NetatmoMeasurementType(*item) for item in _measurements])

PROPERTIES_THAT_ARE_DATES = ['last_setup', 'last_message', 'last_seen', 'last_status_store', 'last_upgrade',
                             'date_setup']


class NetatmoModule(data_class.DataClass):
    """Represent all values present in a Netatmo Module"""
    id: str
    name: str
    type: str
    module_name: str
    data_type: List[str]
    last_setup: Time
    reachable: bool
    dashboard_data: Dict[str, Any]
    firmware: Number
    last_message: Time
    last_seen: Time
    rf_status: Number
    battery_vp: Number
    battery_percent: Number
    station: "NetatmoStation"
    measurements: List[NetatmoMeasurement]

    def __init__(self, *, station: "NetatmoStation", **kwargs) -> None:
        for key, value in kwargs.items():
            if key in PROPERTIES_THAT_ARE_DATES:
                kwargs[key] = Time(value)
        super(NetatmoModule, self).__init__(**kwargs)

        self.station = station
        self.measurements = [NetatmoMeasurement(station=self.station,
                                                module=self,
                                                data_type=types.get_measurement(name)) for name in self.data_type]

    def get_measurement_by_name(self, *, name: str) -> NetatmoMeasurement:
        """Get a measurement by the name of the measurement."""
        return next(
            (measurement
             for measurement in self.measurements
             if measurement.data_type.name == name),
            None
        )

    @property
    def id(self) -> str:
        """Represent the object id without the underscore."""
        return self._id

    @property
    def name(self) -> str:
        """Represent object name by module name."""
        return self.module_name


class NetatmoStation(data_class.DataClass):
    """Represent metadata for a Netatmo Device. A NetatmoDevice has all the same properties as a module, but has
    some additional properties as well. It has measurements and metadata like a Module, so we subclass it from
    NetatmoModule."""
    id: str
    name: str
    last_status_store: Time
    last_upgrade: Time
    wifi_status: int
    co2_calibrating: bool
    station_name: str
    date_setup: int
    place: Dict[str, Any]
    dashboard_data: Dict[str, Any]
    modules: List[NetatmoModule]

    def __init__(self, **kwargs) -> None:
        """Contain data about the Netatmo Device (station). The Netatmo device is actually a subclass of a module, as it
        contains all the information we would expect of a module. To create a more consistent structure we remove these
        properties and add them as a separate module"""
        modules = kwargs.pop('modules')
        device_module = dict()
        for key in modules[0].keys():
            if key in kwargs:
                device_module[key] = kwargs.pop(key)

        device_module['last_seen'] = kwargs['last_status_store']
        kwargs['_id'] = device_module['_id']  # Need to keep _id property.
        kwargs['modules'] = [NetatmoModule(**module, station=self) for module in [device_module] + modules]
        for key, value in kwargs.items():
            if key in PROPERTIES_THAT_ARE_DATES:
                kwargs[key] = Time(value)
        super(NetatmoStation, self).__init__(**kwargs)

    def get_module_by_name(self, *, name: str) -> Union[NetatmoModule, None]:
        """Get a NetatmoDevice object by referencing he name of the device."""
        return next(
            (module
             for module in self.modules
             if module.name == name),
            None)

    @property
    def id(self) -> str:
        """Represent the object id without the underscore."""
        return self._id

    @property
    def name(self) -> str:
        """Represent object name by module name."""
        return self.station_name


class NetatmoDomain:
    """NetatmoDomain is a container for all devices in a Netatmo login."""

    def __init__(self,
                 device_metadata: Dict[str, Any] = None,
                 username: str = None,
                 password: str = None,
                 client_id: str = None,
                 client_secret: str = None) -> None:
        """Represent the metadata for all devices, modules and measurements in a Netatmo Login. Can accept either the
        actual metadata (device_metadata) or the login information for an account so the information is fetched from
        the Netatmo Api."""
        if not (device_metadata or (username and password and client_id and client_secret)):
            raise NetatmoDomainError(f'{NetatmoDomain.__name__} needs either device_metadata directly or complete'
                                     f'login information.')

        if device_metadata:
            metadata = device_metadata
        else:
            auth = lnetatmo.ClientAuth(clientId=client_id,
                                       clientSecret=client_secret,
                                       username=username,
                                       password=password,
                                       scope='read_station')
            device_data = lnetatmo.WeatherStationData(auth)

            metadata = device_data.stations

        self.metadata: Dict[str, Any] = metadata

        self.stations: List[NetatmoStation] = [
            NetatmoStation(**station) for station in metadata.values()
        ]

    def get_station_by_name(self, *, name: str) -> Union[NetatmoStation, None]:
        """Get a NetatmoDevice object by referencing the name of the device."""
        return next((stat for stat in self.stations if stat.name == name), None)

    def get_measurement(self, *, station_name: str, module_name: str, data_type: Union[str, NetatmoMeasurementType]
                        ) -> NetatmoMeasurement:
        """Given a device (station), a module and a data type, return the corresponding measurement from the domain:"""
        if isinstance(data_type, str):
            data_type = types.get_measurement(data_type)

        dev = self.get_station_by_name(name=station_name)
        module = dev.get_module_by_name(name=module_name)
        return module.get_measurement_by_name(name=data_type.name)
