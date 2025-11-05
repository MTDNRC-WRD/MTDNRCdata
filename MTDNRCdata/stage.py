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
import geopandas as gpd
from tzlocal import get_localzone
import numpy as np
import pytz
from typing import Union
from pathlib import Path

from MTDNRCdata import utilities
from config import LOCATIONS_URL, LOCS_SPATIAL_URL
from config import LOCATIONDATA_URL
from config import TIMESERIES_URL
from config import AVAILABLE_DATASETS
from config import STATUS_TYPES
from config import INST_ONLY
from config import LOCATION_FIELDS
from config import TIMESERIES_FIELDS
from config import FORMAT

default_query_params={
    'where' : "1=1",
    'geometry': '-116.2, 44.3, -103.9, 49.1',
    'geometryType' : 'esriGeometryEnvelope',
    'spatialRel': 'esriSpatialRelIntersects',
    'inSR': '4326',
    'units': 'esriSRUnit_Foot',
    'outFields': '*',
    'returnGeometry': 'true',
    'outSR': '4326',
    'returnDistinctValues': 'false',
    'returnIdsOnly': 'false',
    'returnCountOnly': 'false',
    'returnExtentOnly': 'false',
    'returnZ': 'false',
    'returnM': 'false',
    'multipatchOption': 'xyFootprint',
    'returnTrueCurves': 'false',
    'returnExceededLimitFeatures': 'false',
    'returnCentroid': 'false',
    'timeReferenceUnknownClient': 'false',
    'sqlFormat': 'none',
    'featureEncoding': 'esriDefault',
    'f': 'geojson'

}



def site_list():
    siteoutfields = ['LocationCode', 'LocationName', 'StatusDesc']
    responses = []
    for i in STATUS_TYPES:
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


def get_location_parameters(site_ids: Union[str, list]) -> pd.DataFrame:
    """
    Function to return available parameters for a site or list of sites.

    Args:
        site_ids:
            A string or list of strings representing the site ID's

    Returns:
        A DataFrame that shows available parameters for the input sites. Structured as a multiindex
        of (SiteID, parameter_index).
    """

    paramoutfields = ['LocationCode', 'Parameter', 'ParameterLabel', 'ComputationPeriod', 'UnitOfMeasure', 'SensorCode']
    if isinstance(site_ids, str):
        payload = {
            'where': f"LocationCode='{site_ids}'",
            'outFields': ','.join(paramoutfields),
            'f': FORMAT
        }
    elif isinstance(site_ids, list):
        list_join = "','"
        payload = {
            'where': f"LocationCode IN ('{list_join.join(site_ids)}')",
            'outFields': ','.join(paramoutfields),
            'f': FORMAT
        }
    else:
        raise ValueError("The input site ids are not in a supported format.")

    response = requests.get(LOCATIONDATA_URL, params=payload)
    rjson = response.json()
    df_norm = pd.json_normalize(rjson['features'])
    newlabs = [x.split('.')[1] for x in list(df_norm.columns)]
    df_norm.columns = newlabs
    df_norm['Param_Index'] = df_norm[['Parameter', 'ComputationPeriod']].agg('_'.join, axis=1)
    df_norm = df_norm.set_index(['LocationCode', 'Param_Index']).sort_index()

    return df_norm


def get_site_locations(site_ids=None,
                       geometry=None,
                       site_type=None):
    """
    Function to get site locations by ID, input geometry, or site type.

    Args:
        site_ids(str | list):
            A string or list of strings of site IDs to get locations for.

        geometry(str | Path | gpd.GeoDataFrame):
            A string or path to a geometry file, or a geopandas GeoDataFrame polygon to extract sites within that
            area.

        site_type(str | list):
            A string or list of strings for site type labels:
                - 'Real-Time'
                - 'Seasonal'
                - 'FWP'
                - 'Discontinued'
                - 'Reservoir'

    Returns:
        gpd.GeoDataFrame:
            A GeoDataFrame of the resulting sites.
    """

    payload = default_query_params.copy()
    gdf_return = utilities.geojson_request_to_geodf(LOCS_SPATIAL_URL, payload)
    fin_gdf = gdf_return
    if geometry is not None:
        if isinstance(geometry, (str, Path)):
            in_geom = gpd.read_file(geometry)
        elif isinstance(geometry, gpd.GeoDataFrame):
            in_geom = geometry
        else:
            raise ValueError("The input geometry is neither a string path nor a geopandas GeoDataFrame.")

        in_geom = in_geom.to_crs(4326)
        fin_gdf = fin_gdf.loc[fin_gdf.intersects(in_geom.geometry[0]), :]

    if site_ids is not None:
        if isinstance(site_ids, str):
            fin_gdf = fin_gdf.loc[fin_gdf['LocationCode'] == site_ids,:]
        else:
            fin_gdf = fin_gdf.loc[fin_gdf['LocationCode'].isin(site_ids),:]

    if site_type is not None:
        if isinstance(site_type, str):
            fin_gdf = fin_gdf.loc[fin_gdf['StatusDesc'] == site_ids,:]
        else:
            fin_gdf = fin_gdf.loc[fin_gdf['StatusDesc'].isin(site_ids),:]

    return fin_gdf


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
                    #   to change selection to be based on Sensor Code? DON'T USE 'ComputationPeriod' instead use
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
        # TODO - change this so instead of looping through sensor list, query webservice with an 'IN' statement
        #   will require creating string list compatible with the webservice query ('1', '2', 'x'), then will have to sort out
        #   the response based on the SensorID
        TSdata_lst = []
        for i, snsr in enumerate(sensor_lst):
            # Need to add logic for dealing with dates for each get request
            # TODO - here is probably the best place to split instantaneous requests into chunks <= 10000, then loop
            #   through chunks and use ._format_time_inputs() for each chunk
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
                continue
            else:
                payload.update(time_qry)

            tot_records, step = utilities.count_records('https://gis.dnrc.mt.gov/arcgis/rest/services/WRD/WMB_StAGE/MapServer/2', payload)

            if tot_records > 0:
                response = requests.get(TIMESERIES_URL, params=payload)
                rjson = response.json()
                new_feat = [d['attributes'] for d in rjson['features']]
                DF = pd.DataFrame(new_feat)
                DF['SiteID'] = sites[i]
                DF['DatasetCode'] = paramCodes[i]
                DF['DatasetLabel'] = data_labels[i]

                # TODO - alter all conditionals to deal with duplicates and return Datetime as index
                if self._data_timestep == 'instant':
                    TSdts = pd.to_datetime(DF['Timestamp'], unit='ms')
                    dtind = pd.DatetimeIndex(TSdts)
                    #dts_local = dtind.tz_localize('US/Mountain', ambiguous='infer')
                    dts_local = dtind.tz_localize('etc/GMT+7', ambiguous='infer')
                    #fn_dts = dts_local.tz_convert(get_localzone())
                    #fn_dts.rename('Datetime', inplace=True)
                    #DF.set_index(fn_dts, inplace=True)
                    DF.index = dts_local
                    DF.drop('Timestamp', axis=1, inplace=True)
                    DF.sort_index(inplace=True)
                    DF = DF[~DF.index.duplicated(keep='last')]
                    off = DF.index.values - np.roll(DF.index.values, 1)
                    minoff = pd.to_timedelta(off[1:]).min()
                    DF = DF.reindex(pd.date_range(DF.index.min(), DF.index.max(), freq=minoff))
                    DF.index.name = 'Datetime'
                    DF['SiteID'] = DF['SiteID'].ffill()
                    DF['DatasetCode'] = DF['DatasetCode'].ffill()
                    DF['DatasetLabel'] = DF['DatasetLabel'].ffill()
                elif self._data_timestep == 'daily' and paramCodes[i] in INST_ONLY:
                    TSunxdts = (DF['Timestamp'] / 1000)
                    TSdts = pd.to_datetime(TSunxdts, unit='ms')
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
                    DF.sort_values(by='Date', inplace=True)
                    DF.reset_index(drop=True, inplace=True)
                elif self._data_timestep == 'daily' and paramCodes[i] not in INST_ONLY:
                    TSdts = pd.to_datetime(DF['Timestamp'], unit='ms')
                    dtind = pd.DatetimeIndex(TSdts)
                    DF.index = dtind.normalize()
                    DF.drop('Timestamp', axis=1, inplace=True)
                    DF.sort_index(inplace=True)
                    DF = DF[~DF.index.duplicated(keep='last')]
                    DF = DF.reindex(pd.date_range(DF.index.min(), DF.index.max(), freq='D'))
                    DF.index.name = 'Date'
                    DF['SiteID'] = DF['SiteID'].ffill()
                    DF['DatasetCode'] = DF['DatasetCode'].ffill()
                    DF['DatasetLabel'] = DF['DatasetLabel'].ffill()
                else:
                    print("Timestamps could not be re-formatted.")
                    pass
                TSdata_lst.append(DF)

            else:
                continue

        if len(TSdata_lst) > 0:
            TSdata = pd.concat(TSdata_lst)
        else:
            print("No Data returned for the selected time period.")
            TSdata = pd.DataFrame()

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
