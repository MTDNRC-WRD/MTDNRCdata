"""
Module to download data from MT DNRC StAGE API.

https://gis.dnrc.mt.gov/arcgis/rest/services/WRD/WMB_StAGE/MapServer

To do:
    * Add error statements and tracking for successful or un-successful queries and requests
    * Add geometry search functionality (bbox, shapefile, or geojson)
    * Add direct download to shapefile functionality (for list of sites)
    * Add multiple-site and parameter functionality
    * Add plotting functionality
"""

import requests
import pandas as pd
from tzlocal import get_localzone_name
import pytz

from MTDNRCdata import utilities

LOCATIONS_URL = 'https://gis.dnrc.mt.gov/arcgis/rest/services/WRD/WMB_StAGE/MapServer/4/query'
TIMESERIES_URL = 'https://gis.dnrc.mt.gov/arcgis/rest/services/WRD/WMB_StAGE/MapServer/2/query'
FORMAT = 'pjson'
LOCATION_FIELDS = [
    'LocationCode',
    'LocationID',
    'LocationName',
    'LocationType',
    'Longitude',
    'Latitude',
    'Elevation',
    'ElevationUnits',
    'Description',
    'SensorCode',
    'SensorID',
    'SensorLabel',
    'TimeSeriesType',
    'DatasetUtcOffset',
    'Parameter',
    'ParameterLabel',
    'UnitOfMeasure',
    'ComputationMethod',
    'ComputationPeriod',
    'CountyName',
    'BasinName',
    'HUC8Code',
    'StatusDesc'
]

TIMESERIES_FIELDS = [
    'Timestamp',
    'RecordedValue',
    'GradeCode',
    'GradeName',
    'Method',
    'ApprovalLevel',
    'ApprovalName'
]

AVAILABLE_DATASETS = ['QR', 'HG', 'TW', 'Wat_LVL_BLSD', 'Lake_Elev_NGVD', 'LS']

class GetSites(object):
    """
    A class that holds site/location information and specified datasets given a single site ID, or list of site
    IDs for DNRC stations, along with data query arguments.

    Attributes
    -----------
    site_id : str
        a string representing the station ID(s) of interest (only 1 site functional as of this version)
    timestep : str
        specify either 'instant' for instantaneous data or 'daily' for average daily values; default is 'instant'
    """
    def __init__(self, site_id, timestep='instant', dataset=None, start=None, end=None, notime_return='recent',
                 inst_only_method='end_day'):
        self._site = site_id
        self._data_timestep = timestep
        self._dset = dataset
        self._querystart = start
        self._queryend = end
        self._nt_return = notime_return
        self._instonly_method = inst_only_method
        self._location_info = self._get_location_info()

        self.site_info = self._format_site_info()
        self.data = self._get_timeseries()
        self.multiindex_dataframe = self.data.pivot(columns=['SiteID', 'DatasetLabel'])

    def _get_location_info(self):
        payload = {
            'where': "LocationCode='{0}'".format(self._site),
            'outFields': ','.join(LOCATION_FIELDS),
            'f': FORMAT
        }
        response = requests.get(LOCATIONS_URL, params=payload)
        rjson = response.json()
        return rjson['features']

    def _format_site_info(self):
        loc_dict = self._location_info[0]['attributes']
        avail_params = []
        for i in self._location_info:
            for k, v in i['attributes'].items():
                if k == 'Parameter':
                    avail_params.append(v)
                else:
                    continue
        keep_keys = ['LocationCode', 'LocationName', 'LocationType', 'Longitude', 'Latitude', 'Elevation',
                     'ElevationUnits', 'Description', 'AvailableDatasets', 'CountyName', 'BasinName', 'HUC8Code']
        Dfram = pd.DataFrame(loc_dict, index=[0])
        Dfram['AvailableDatasets'] = ','.join(avail_params)
        FDF = Dfram[keep_keys]
        return FDF

    def _get_timeseries(self):
        INST_ONLY = ['Wat_LVL_BLSD', 'Lake_Elev_NGVD', 'LS']
        sites = []
        paramCodes = []
        data_labels = []
        sensor_lst = []
        for i in self._location_info:
            if self._dset is None:
                if self._data_timestep == 'instant' and i['attributes']['ComputationPeriod'] == 'Unknown':
                    sensor_lst.append(i['attributes']['SensorID'])
                    sites.append(i['attributes']['LocationCode'])
                    paramCodes.append(i['attributes']['Parameter'])
                    data_labels.append("{0}({1})_{2}".format(i['attributes']['ParameterLabel'],
                                                             i['attributes']['Parameter'],
                                                             i['attributes']['UnitOfMeasure']))
                elif self._data_timestep == 'daily':
                    if i['attributes']['ComputationPeriod'] == 'Daily':
                        sensor_lst.append(i['attributes']['SensorID'])
                        sites.append(i['attributes']['LocationCode'])
                        paramCodes.append(i['attributes']['Parameter'])
                        data_labels.append("{0}_{1}_{2}({3})_{4}".format(i['attributes']['ComputationMethod'],
                                                                         i['attributes']['ComputationPeriod'],
                                                                         i['attributes']['ParameterLabel'],
                                                                         i['attributes']['Parameter'],
                                                                         i['attributes']['UnitOfMeasure']))
                    elif i['attributes']['Parameter'] in INST_ONLY:
                        sensor_lst.append(i['attributes']['SensorID'])
                        sites.append(i['attributes']['LocationCode'])
                        paramCodes.append(i['attributes']['Parameter'])
                        data_labels.append("{0}({1})_{2}".format(i['attributes']['ParameterLabel'],
                                                                 i['attributes']['Parameter'],
                                                                 i['attributes']['UnitOfMeasure']))
            if self._dset is not None:
                if isinstance(self._dset, list):
                    # TODO - Check to see if self._dset list has all valid entries
                    if self._data_timestep == 'instant':
                        if i['attributes']['Parameter'] in self._dset and i['attributes']['ComputationPeriod'] == 'Unknown':
                            sensor_lst.append(i['attributes']['SensorID'])
                            sites.append(i['attributes']['LocationCode'])
                            paramCodes.append(i['attributes']['Parameter'])
                            data_labels.append("{0}({1})_{2}".format(i['attributes']['ParameterLabel'],
                                                                     i['attributes']['Parameter'],
                                                                     i['attributes']['UnitOfMeasure']))

                    if self._data_timestep == 'daily':
                        if i['attributes']['Parameter'] in self._dset and i['attributes']['ComputationPeriod'] == 'Daily':
                            sensor_lst.append(i['attributes']['SensorID'])
                            sites.append(i['attributes']['LocationCode'])
                            paramCodes.append(i['attributes']['Parameter'])
                            data_labels.append("{0}_{1}_{2}({3})_{4}".format(i['attributes']['ComputationMethod'],
                                                                             i['attributes']['ComputationPeriod'],
                                                                             i['attributes']['ParameterLabel'],
                                                                             i['attributes']['Parameter'],
                                                                             i['attributes']['UnitOfMeasure']))

                        elif i['attributes']['Parameter'] in self._dset and i['attributes']['Parameter'] in INST_ONLY:
                            sensor_lst.append(i['attributes']['SensorID'])
                            sites.append(i['attributes']['LocationCode'])
                            paramCodes.append(i['attributes']['Parameter'])
                            data_labels.append("{0}({1})_{2}".format(i['attributes']['ParameterLabel'],
                                                                     i['attributes']['Parameter'],
                                                                     i['attributes']['UnitOfMeasure']))
                elif isinstance(self._dset, str):
                    if self._data_timestep == 'instant':
                        if i['attributes']['Parameter'] == self._dset and i['attributes']['ComputationPeriod'] == 'Unknown':
                            sensor_lst.append(i['attributes']['SensorID'])
                            sites.append(i['attributes']['LocationCode'])
                            paramCodes.append(i['attributes']['Parameter'])
                            data_labels.append("{0}({1})_{2}".format(i['attributes']['ParameterLabel'],
                                                                     i['attributes']['Parameter'],
                                                                     i['attributes']['UnitOfMeasure']))
                    elif self._data_timestep == 'daily':
                        if i['attributes']['Parameter'] == self._dset and i['attributes']['ComputationPeriod'] == 'Daily':
                            sensor_lst.append(i['attributes']['SensorID'])
                            sites.append(i['attributes']['LocationCode'])
                            paramCodes.append(i['attributes']['Parameter'])
                            data_labels.append("{0}_{1}_{2}({3})_{4}".format(i['attributes']['ComputationMethod'],
                                                                             i['attributes']['ComputationPeriod'],
                                                                             i['attributes']['ParameterLabel'],
                                                                             i['attributes']['Parameter'],
                                                                             i['attributes']['UnitOfMeasure']))

                        elif i['attributes']['Parameter'] == self._dset and i['attributes']['Parameter'] in INST_ONLY:
                            sensor_lst.append(i['attributes']['SensorID'])
                            sites.append(i['attributes']['LocationCode'])
                            paramCodes.append(i['attributes']['Parameter'])
                            data_labels.append("{0}({1})_{2}".format(i['attributes']['ParameterLabel'],
                                                                     i['attributes']['Parameter'],
                                                                     i['attributes']['UnitOfMeasure']))
                else:
                    print("Dataset argument is neither list nor string.")

        TSdata_lst = []
        for i, snsr in enumerate(sensor_lst):
            # Need to add logic for dealing with dates for each get request
            # Also need to separate instant only datasets and calculate end of day values
            utc_param = self._location_info[i]['attributes']['DatasetUtcOffset']
            utc_offset = utilities.utc_offset_from_str(utc_param)
            time_qry = self._format_time_inputs(utc_offset=utc_offset, units='H')
            payload = {'where': "SensorID='{0}'".format(snsr),
                            'outFields': ','.join(TIMESERIES_FIELDS),
                            'f': FORMAT
                       }
            if time_qry is None:
                pass
            else:
                payload.update(time_qry)

            response = requests.get(TIMESERIES_URL, params=payload)
            rjson = response.json()
            new_feat = [d['attributes'] for d in rjson['features']]
            DF = pd.DataFrame(new_feat)
            DF['SiteID'] = sites[i]
            DF['DatasetCode'] = paramCodes[i]
            DF['DatasetLabel'] = data_labels[i]

            if self._data_timestep == 'instant':
                inv_utc = -utc_offset
                TSunxdts = (DF['Timestamp'] / 1000) + inv_utc
                TSdts = pd.to_datetime(TSunxdts, unit='s', utc=True)
                dtind = pd.DatetimeIndex(TSdts)
                fn_dts = dtind.tz_convert(get_localzone_name())
                #fn_dts.rename('Datetime', inplace=True)
                #DF.set_index(fn_dts, inplace=True)
                DF['Datetime'] = fn_dts
                DF.drop('Timestamp', axis=1, inplace=True)
            elif self._data_timestep == 'daily' and paramCodes[i] in INST_ONLY:
                inv_utc = -utc_offset
                TSunxdts = (DF['Timestamp'] / 1000) + inv_utc
                TSdts = pd.to_datetime(TSunxdts, unit='s', utc=True)
                dtind = pd.DatetimeIndex(TSdts)
                fn_dts = dtind.tz_convert(get_localzone_name())
                fn_dts.rename('Datetime', inplace=True)
                DF.set_index(fn_dts, inplace=True)
                DF = DF.resample('1D').last()
                DF['Date'] = DF.index.strftime('%Y-%m-%d')
                DF.reset_index(inplace=True)
                DF.drop('Timestamp', axis=1, inplace=True)
                DF.drop('Datetime', axis=1, inplace=True)
            elif self._data_timestep == 'daily' and paramCodes[i] not in INST_ONLY:
                TSdts = pd.to_datetime(DF['Timestamp'], unit='ms')
                dtind = pd.DatetimeIndex(TSdts)
                fn_dts = dtind.strftime('%Y-%m-%d')
                #fn_dts.rename('Date', inplace=True)
                #TSdata.set_index(fn_dts, inplace=True)
                DF['Date'] = fn_dts
                DF.drop('Timestamp', axis=1, inplace=True)
            else:
                print("Timestamps could not be re-formatted.")
                pass
            TSdata_lst.append(DF)

        TSdata = pd.concat(TSdata_lst)
        if time_qry is None and self._nt_return == 'recent':
            TSdata = TSdata.iloc[-1, :]
        elif time_qry is None and self._nt_return == 'por':
            pass
        else:
            pass

        return TSdata

    # Something in here isn't working as expected...reading user input date as UTC instead of local zone
    def _format_time_inputs(self, **kwargs):
        if self._data_timestep == 'instant':
            if self._querystart is None and self._queryend is None:
                time_qry = None
            elif self._querystart is None and self._queryend is not None:
                strt = 'null'
                end = int((utilities.offset_unix(utilities.datetime_to_unix(self._queryend), **kwargs))*1000)
                time_qry = {'time': '{0}, {1}'.format(strt, end)}
            elif self._querystart is not None and self._queryend is None:
                strt = int((utilities.offset_unix(utilities.datetime_to_unix(self._querystart), **kwargs))*1000)
                end = 'null'
                time_qry = {'time': '{0}, {1}'.format(strt, end)}
            else:
                strt = int((utilities.offset_unix(utilities.datetime_to_unix(self._querystart), **kwargs))*1000)
                end = int((utilities.offset_unix(utilities.datetime_to_unix(self._queryend), **kwargs))*1000)
                time_qry = {'time': '{0}, {1}'.format(strt, end)}
        elif self._data_timestep == 'daily':
            if self._querystart is None and self._queryend is None:
                time_qry = None
            elif self._querystart is None and self._queryend is not None:
                strt = 'null'
                end = int(utilities.date_to_unix_naive(self._queryend)*1000)
                time_qry = {'time': '{0}, {1}'.format(strt, end)}
            elif self._querystart is not None and self._queryend is None:
                strt = int(utilities.date_to_unix_naive(self._querystart)*1000)
                end = 'null'
                time_qry = {'time': '{0}, {1}'.format(strt, end)}
            else:
                strt = int(utilities.date_to_unix_naive(self._querystart)*1000)
                end = int(utilities.date_to_unix_naive(self._queryend)*1000)
                time_qry = {'time': '{0}, {1}'.format(strt, end)}

        return time_qry



Test = GetSites('43D 01900', timestep='instant', dataset='QR', start='2023-06-01', end='2023-07-31')