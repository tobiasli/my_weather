from typing import Dict, List, Sequence
import lnetatmo
from shyft.api import StringVector, UtcPeriod, TimeAxis, TimeSeries, POINT_INSTANT_VALUE, TsVector, TsInfoVector, TsInfo
from shyft.repository.interfaces import TsRepository


def netatmo_read_url(station_name: str, module_name: str, data_type: str) -> str:
    return f'{NetatmoTsRepository.schema}://{station_name}/{module_name}/{data_type}'


StationMetadata = Dict[str, object]


class NetatmoTsRepository(TsRepository):
    schema = 'netatmo'

    def __init__(self, username: str, password: str, client_id: str, client_secret: str) -> None:
        self.auth = lnetatmo.ClientAuth(clientId=client_id, clientSecret=client_secret, username=username, password=password,
                                        scope='read_station')

        self.station_data = lnetatmo.WeatherStationData(self.auth)

        self.stations: List[StationMetadata] = list(self.station_data.stations.values())
        self.stations_by_id: Dict[str, StationMetadata] = self.station_data.stations
        self.stations_by_name: Dict[str, StationMetadata] = {station['station_name']: station for station in self.stations}

    def get_measurements_from_station(self,
                               station_name: str,
                               measurement_type: str,
                               utc_period: UtcPeriod = None
                               ) -> TsVector:
        """Get data for a specific station and set of measurements. utc_period is the timespan for which we ask for
        data, but it is optional, as utc_period=None asks for the longest possible timespan of data.

        NB: Calls are limited to 1024 values. Must split to get all data in period (50 req pr sec, 500 req pr hour).

        Args:
            station_name: The name of the station.
            measurement_type: Comma seperated string of measurement names:
                Temperature (°C)
                CO2 (ppm)
                Humidity (%)
                Pressure (mbar)
                Noise (db)
                Rain (mm)
                WindStrength (km/h)
                WindAngle (angles)
                Guststrength (km/h)
                GustAngle (angles)
            utc_period: Inclusive start/end. The period we want data for (if none, the longest possible period
                        (up to 1024 values).

        Returns:
            A Tsvector with timeseries containing data for each measurement type, in the order of the input.
        """

        date_start = utc_period.start if utc_period else None
        date_end = utc_period.end if utc_period else None
        data = self.station_data.getMeasure(device_id=self.stations_by_name[station_name]['_id'],
                                            scale='max',
                                            mtype=measurement_type,
                                            date_begin=date_start,
                                            date_end=date_end)

        time = [float(timestamp) for timestamp in data['body'].keys()]
        dt_list = [t2 - t1 for t1, t2 in zip(time[0:-2], time[1:-1])]
        dt_mode = max(set(dt_list), key=dt_list.count)

        ta = TimeAxis(time + [dt_mode])  # Add another value at end of timeseries using the most common dt found.

        values_pr_time = [value for value in data['body'].values()]
        values = list(map(list, zip(*values_pr_time)))

        tsvec = TsVector([TimeSeries(ta, vector, POINT_INSTANT_VALUE) for vector in values])

        return tsvec

    def read(self, list_of_ts_id: Sequence[str], period: UtcPeriod) -> TsVector:
        """Take a sequence of strings identifying specific timeseries and get data from these series according to
        the timespan defined in period.

        Args:
            list_of_ts_id: A sequence of strings identifying specific timeseries available from the netatmo login.
            period: A period defined by a utc timestamp for the start and end of the analysis period.

        Returns:
            A TsVector containing the resulting timeseries containing data enough to cover the query period.
        """

        abs(1)

    def read_callback(self, ts_ids: StringVector, read_period: UtcPeriod) -> TsVector:
        """This callback is passed as the default read_callback for a shyft.api.DtsServer.

        Args:
            ts_ids: A sequence of strings identifying specific timeseries available from the netatmo login.
            read_period: A period defined by a utc timestamp for the start and end of the analysis period.

        Returns:
            A TsVector containing the resulting timeseries containing data enough to cover the query period.
        """
        return self.read(ts_ids, read_period)

    def find(self, query: str) -> TsInfoVector:
        tsi = TsInfo(query)
        tsiv = TsInfoVector()
        tsiv.append(tsi)
        return tsiv

    def read_forecast(self, list_of_fc_id: List[str], period: UtcPeriod):
        """
        read and return the newest forecast that have the biggest overlap with specified period
        note that we should check that the semantic of this is reasonable
        """
        raise NotImplementedError("read_forecast")

    def store(self, timeseries_dict: Dict[str, TimeSeries]):
        """ Store the supplied time-series to the underlying db-system.
            Parameters
            ----------
            timeseries_dict: dict string:timeseries
                the keys are the wanted ts(-path) names
                and the values are shyft api.time-series.
                If the named time-series does not exist, create it.
        """
        raise NotImplementedError("read_forecast")


# !/usr/bin/python
# coding=utf-8

# 2013-01 : philippelt@users.sourceforge.net

# This is an example of graphing Temperature and Humidity from a module on the last 3 days
# The Matplotlib library is used and should be installed before running this sample program

# import datetime, time
#
# from matplotlib import pyplot as plt
# from matplotlib import dates
# from matplotlib.ticker import FormatStrFormatter
#
# # Access to the sensors
# auth = lnetatmo.ClientAuth()
# dev = lnetatmo.DeviceList(auth)
#
# # Time of information collection : 3*24hours windows to now
# now = time.time()
# start = now - 3 * 24 * 3600
#
# # Get Temperature and Humidity with GETMEASURE web service (1 sample every 30min)
# resp = dev.getMeasure(device_id='xxxx',  # Replace with your values
#                       module_id='xxxx',  # "      "    "    "
#                       scale="30min",
#                       mtype="Temperature,Humidity",
#                       date_begin=start,
#                       date_end=now)
#
# # Extract the timestamp, temperature and humidity from the more complex response structure
# result = [(int(k), v[0], v[1]) for k, v in resp['body'].items()]
# # Sort samples by timestamps (Warning, they are NOT sorted by default)
# result.sort()
# # Split in 3 lists for use with Matplotlib (timestamp on x, temperature and humidity on two y axis)
# xval, ytemp, yhum = zip(*result)
#
# # Convert the x axis values from Netatmo timestamp to matplotlib timestamp...
# xval = [dates.date2num(datetime.datetime.fromtimestamp(x)) for x in xval]
#
# # Build the two curves graph (check Matplotlib documentation for details)
# fig = plt.figure()
# plt.xticks(rotation='vertical')
#
# graph1 = fig.add_subplot(111)
#
# graph1.plot(xval, ytemp, color='r', linewidth=3)
# graph1.set_ylabel(u'Température', color='r')
# graph1.set_ylim(0, 25)
# graph1.yaxis.grid(color='gray', linestyle='dashed')
# for t in graph1.get_yticklabels(): t.set_color('r')
# graph1.yaxis.set_major_formatter(FormatStrFormatter(u'%2.0f °C'))
#
# graph2 = graph1.twinx()
#
# graph2.plot(xval, yhum, color='b', linewidth=3)
# graph2.set_ylabel(u'Humidité', color='b')
# graph2.set_ylim(50, 100)
# for t in graph2.get_yticklabels(): t.set_color('b')
# graph2.yaxis.set_major_formatter(FormatStrFormatter(u'%2i %%'))
#
# graph1.xaxis.set_major_locator(dates.HourLocator(interval=6))
# graph1.xaxis.set_minor_locator(dates.HourLocator())
# graph1.xaxis.set_major_formatter(dates.DateFormatter("%d-%Hh"))
# graph1.xaxis.grid(color='gray')
# graph1.set_xlabel(u'Jour et heure de la journée')
#
# # X display the resulting graph (you could generate a PDF/PNG/... in place of display).
# # The display provides a minimal interface that notably allows you to save your graph
# plt.show()
