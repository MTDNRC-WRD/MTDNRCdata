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
from tzlocal import get_localzone
import pytz

from MTDNRCdata import utilities

#TODO move all hard-coded url's and references to config file

# Layer Endpoints
LOCS_SPATIAL_URL = 'https://gis.dnrc.mt.gov/arcgis/rest/services/WRD/WMB_StAGE/MapServer/1/query'
# Table Endpoints
LOCATIONS_URL = 'https://gis.dnrc.mt.gov/arcgis/rest/services/WRD/WMB_StAGE/MapServer/1/query'
LOCATIONDATA_URL = 'https://gis.dnrc.mt.gov/arcgis/rest/services/WRD/WMB_StAGE/MapServer/4/query'
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


def site_list():
    status_type = ['Real-Time', 'Seasonal', 'FWP', 'Discontinued', 'Reservoir']
    siteoutfields = ['LocationCode', 'LocationName', 'StatusDesc']
    responses = []
    for i in status_type:
        payload = {
            'where': "StatusDesc='{0}'".format(i),
            'outFields': ','.join(siteoutfields),
            'f': FORMAT
        }
        response = requests.get(LOCATIONS_URL, params=payload)
        rjson = response.json()
        df_norm = pd.json_normalize(rjson['features'])
        responses.append(df_norm)

    sites_df = pd.concat(responses, ignore_index=True)

    return sites_df


def get_location_parameters(site_id):
    paramoutfields = ['Parameter', 'ParameterLabel', 'ComputationPeriod', 'UnitOfMeasure', 'SensorCode']

    payload = {
        'where': "LocationCode='{0}'".format(site_id),
        'outFields': ','.join(paramoutfields),
        'f': FORMAT
    }
    response = requests.get(LOCATIONDATA_URL, params=payload)
    rjson = response.json()
    df_norm = pd.json_normalize(rjson['features'])

    return df_norm


def get_sites_geojson(bbox=[-116.5, 42.5, -103, 49.5]):
    """
    Currently extracts all point data for gage locations based on bounding box.
    :param bbox: list, with bounding box coordinates of order [xmin, ymin, xmax, ymax]
    :return: requests object
    """
    if bbox is not None:
        req_url = "https://gis.dnrc.mt.gov/arcgis/rest/services/WRD/WMB_StAGE/MapServer/0/query?where=&text=&" \
                  "objectIds=&time=&timeRelation=esriTimeRelationOverlaps&geometry={0}%2C+{1}%2C+{2}%2C+{3}&" \
                  "geometryType=esriGeometryEnvelope&inSR=4326&spatialRel=esriSpatialRelIntersects&distance=&" \
                  "units=esriSRUnit_Foot&relationParam=&outFields=LocationCode%2C+ObjectID&returnGeometry=true&" \
                  "returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=4326&havingClause=&" \
                  "returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&" \
                  "outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&" \
                  "resultOffset=&resultRecordCount=&returnExtentOnly=false&sqlFormat=none&datumTransformation=&" \
                  "parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&" \
                  "f=geojson".format(bbox[0], bbox[1], bbox[2], bbox[3])
    else:
        print("bounding coordinates required")
    response = requests.get(req_url)
    return response


class GetSite(object):
    """
    A class that holds site/location information and specified datasets given a single site ID along with data query arguments.

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
        # Not sure if this is needed, maybe if multi-parameter query is implemented?
        #self.multiindex_dataframe = self.data.pivot(columns=['SiteID', 'DatasetLabel'])

    def _get_location_info(self):
        payload = {
            'where': "LocationCode='{0}'".format(self._site),
            'outFields': ','.join(LOCATION_FIELDS),
            'f': FORMAT
        }
        response = requests.get(LOCATIONDATA_URL, params=payload)
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
        loc_index = []
        for n, i in enumerate(self._location_info):
            if self._dset is None:
                if self._data_timestep == 'instant' and i['attributes']['ComputationPeriod'] == 'Unknown':
                    sensor_lst.append(i['attributes']['SensorID'])
                    sites.append(i['attributes']['LocationCode'])
                    paramCodes.append(i['attributes']['Parameter'])
                    data_labels.append("{0}({1})_{2}".format(i['attributes']['ParameterLabel'],
                                                             i['attributes']['Parameter'],
                                                             i['attributes']['UnitOfMeasure']))
                    loc_index.append(n)
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
                        loc_index.append(n)
                    elif i['attributes']['Parameter'] in INST_ONLY:
                        sensor_lst.append(i['attributes']['SensorID'])
                        sites.append(i['attributes']['LocationCode'])
                        paramCodes.append(i['attributes']['Parameter'])
                        data_labels.append("{0}({1})_{2}".format(i['attributes']['ParameterLabel'],
                                                                 i['attributes']['Parameter'],
                                                                 i['attributes']['UnitOfMeasure']))
                        loc_index.append(n)
            if self._dset is not None:
                if isinstance(self._dset, list):
                    # TODO - Check to see if self._dset list has all valid entries
                    # TODO - Some historic discontinued sites do not have correct ComputationPeriod Parameter, need
                    #   a work around to select based on Sensor Code? DON'T USE 'ComputationPeriod' instead use
                    #   if i['attributes']['SensorLabel'] == 'Daily Average' when self._data_timestep == 'daily'
                    if self._data_timestep == 'instant':
                        if i['attributes']['Parameter'] in self._dset and i['attributes']['ComputationPeriod'] == 'Unknown':
                            sensor_lst.append(i['attributes']['SensorID'])
                            sites.append(i['attributes']['LocationCode'])
                            paramCodes.append(i['attributes']['Parameter'])
                            data_labels.append("{0}({1})_{2}".format(i['attributes']['ParameterLabel'],
                                                                     i['attributes']['Parameter'],
                                                                     i['attributes']['UnitOfMeasure']))
                            loc_index.append(n)

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
                            loc_index.append(n)

                        elif i['attributes']['Parameter'] in self._dset and i['attributes']['Parameter'] in INST_ONLY:
                            sensor_lst.append(i['attributes']['SensorID'])
                            sites.append(i['attributes']['LocationCode'])
                            paramCodes.append(i['attributes']['Parameter'])
                            data_labels.append("{0}({1})_{2}".format(i['attributes']['ParameterLabel'],
                                                                     i['attributes']['Parameter'],
                                                                     i['attributes']['UnitOfMeasure']))
                            loc_index.append(n)
                elif isinstance(self._dset, str):
                    if self._data_timestep == 'instant':
                        if i['attributes']['Parameter'] == self._dset and i['attributes']['ComputationPeriod'] == 'Unknown':
                            sensor_lst.append(i['attributes']['SensorID'])
                            sites.append(i['attributes']['LocationCode'])
                            paramCodes.append(i['attributes']['Parameter'])
                            data_labels.append("{0}({1})_{2}".format(i['attributes']['ParameterLabel'],
                                                                     i['attributes']['Parameter'],
                                                                     i['attributes']['UnitOfMeasure']))
                            loc_index.append(n)
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
                            loc_index.append(n)

                        elif i['attributes']['Parameter'] == self._dset and i['attributes']['Parameter'] in INST_ONLY:
                            sensor_lst.append(i['attributes']['SensorID'])
                            sites.append(i['attributes']['LocationCode'])
                            paramCodes.append(i['attributes']['Parameter'])
                            data_labels.append("{0}({1})_{2}".format(i['attributes']['ParameterLabel'],
                                                                     i['attributes']['Parameter'],
                                                                     i['attributes']['UnitOfMeasure']))
                            loc_index.append(n)
                else:
                    print("Dataset argument is neither list nor string.")

        TSdata_lst = []
        for i, snsr in enumerate(sensor_lst):
            # Need to add logic for dealing with dates for each get request
            # Also need to separate instant only datasets and calculate end of day values
            time_qry = self._format_time_inputs()
            payload = {'where': "SensorID='{0}'".format(snsr),
                            'outFields': ','.join(TIMESERIES_FIELDS),
                            'f': FORMAT
                       }
            # Need to change/update, should never have time_qry = None
            # This conditional should check if type(time_qry) is dict or list
            #   if == list, that means there is more than 1 time_qry to send to requests
            #   have loop to get all requests for all time blocks, build DFs, then concat
            if time_qry is None:
                print("Time Query was not properly set.")
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
                TSunxdts = (DF['Timestamp'] / 1000)
                TSdts = pd.to_datetime(TSunxdts, unit='s')
                dtind = pd.DatetimeIndex(TSdts)
                dts_local = dtind.tz_localize('US/Mountain')
                fn_dts = dts_local.tz_convert(get_localzone())
                #fn_dts.rename('Datetime', inplace=True)
                #DF.set_index(fn_dts, inplace=True)
                DF['Datetime'] = fn_dts
                DF.drop('Timestamp', axis=1, inplace=True)
            elif self._data_timestep == 'daily' and paramCodes[i] in INST_ONLY:
                TSunxdts = (DF['Timestamp'] / 1000)
                TSdts = pd.to_datetime(TSunxdts, unit='s')
                dtind = pd.DatetimeIndex(TSdts)
                dts_local = dtind.tz_localize('US/Mountain')
                fn_dts = dts_local.tz_convert(get_localzone())
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
        #if self._nt_return == 'recent':
        #    TSdata = TSdata.iloc[[-1]]
        #else:
        #    pass

        return TSdata

    # no empty time queries, need to explicitly identify start and end times
    # need to provide subsetting date ranges for instant values in case user needs more than 10000 values paginated
    # (multiple time queries)
    # for instant values, need to return last time-stamp of the "end" query date
    # if no start/end provided, default behavior can be set when instantiating the class --
    # return latest value, previous 7-days, or previous 30-days
    def _format_time_inputs(self):
        # TODO - Check validity of start, end strings, currently only checks that it is not None
        if self._data_timestep == 'instant':
            if self._querystart is None and self._queryend is None:
                if self._nt_return == 'recent':
                    strt, end = utilities.get_previous_timerange()
                    strt = int(utilities.offset_unix(strt) * 1000)
                    end = int(utilities.offset_unix(end) * 1000)
                    time_qry = {'time': '{0}, {1}'.format(strt, end)}
                elif self._nt_return == '7D':
                    strt, end = utilities.get_previous_timerange(last=7, units='D')
                    strt = int(utilities.offset_unix(strt) * 1000)
                    end = int(utilities.offset_unix(end) * 1000)
                    time_qry = {'time': '{0}, {1}'.format(strt, end)}
                elif self._nt_return == '30D':
                    strt, end = utilities.get_previous_timerange(last=30, units='D')
                    strt = int(utilities.offset_unix(strt) * 1000)
                    end = int(utilities.offset_unix(end) * 1000)
                    time_qry = {'time': '{0}, {1}'.format(strt, end)}
                else:
                    print("No time query supplied and an invalid response was entered for notime_return behavior.")
                    print("Using most recent reading as default.")
                    strt, end = utilities.get_previous_timerange()
                    strt = int(utilities.offset_unix(strt) * 1000)
                    end = int(utilities.offset_unix(end) * 1000)
                    time_qry = {'time': '{0}, {1}'.format(strt, end)}
            elif self._querystart is None and self._queryend is not None:
                strt = 'null'
                end = int((utilities.offset_unix(utilities.datetime_to_unix(self._queryend)))*1000)
                time_qry = {'time': '{0}, {1}'.format(strt, end)}
            elif self._querystart is not None and self._queryend is None:
                strt = int((utilities.offset_unix(utilities.datetime_to_unix(self._querystart)))*1000)
                end = 'null'
                time_qry = {'time': '{0}, {1}'.format(strt, end)}
            else:
                strt = int((utilities.offset_unix(utilities.datetime_to_unix(self._querystart)))*1000)
                end = int((utilities.offset_unix(utilities.datetime_to_unix(self._queryend)))*1000)
                time_qry = {'time': '{0}, {1}'.format(strt, end)}
        elif self._data_timestep == 'daily':
            if self._querystart is None and self._queryend is None:
                if self._nt_return == 'recent':
                    strt, end = utilities.get_previous_timerange(last=2, units='D', unix=False)
                    strt = int(utilities.date_to_unix_naive(strt.strftime("%Y-%m-%d")) * 1000)
                    end = int(utilities.date_to_unix_naive(end.strftime("%Y-%m-%d")) * 1000)
                    time_qry = {'time': '{0}, {1}'.format(strt, end)}
                elif self._nt_return == '7D':
                    strt, end = utilities.get_previous_timerange(last=7, units='D', unix=False)
                    strt = int(utilities.date_to_unix_naive(strt.strftime("%Y-%m-%d")) * 1000)
                    end = int(utilities.date_to_unix_naive(end.strftime("%Y-%m-%d")) * 1000)
                    time_qry = {'time': '{0}, {1}'.format(strt, end)}
                elif self._nt_return == '30D':
                    strt, end = utilities.get_previous_timerange(last=30, units='D', unix=False)
                    strt = int(utilities.date_to_unix_naive(strt.strftime("%Y-%m-%d")) * 1000)
                    end = int(utilities.date_to_unix_naive(end.strftime("%Y-%m-%d")) * 1000)
                    time_qry = {'time': '{0}, {1}'.format(strt, end)}
                else:
                    print("No time query supplied and an invalid response was entered for notime_return behavior.")
                    print("Using most recent reading as default.")
                    strt, end = utilities.get_previous_timerange(last=2, units='D', unix=False)
                    strt = int(utilities.date_to_unix_naive(strt.strftime("%Y-%m-%d")) * 1000)
                    end = int(utilities.date_to_unix_naive(end.strftime("%Y-%m-%d")) * 1000)
                    time_qry = {'time': '{0}, {1}'.format(strt, end)}
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
