"""
Module to download water rights data from MT DNRC Water Rights Query System (WRQS).

https://gis.dnrc.mt.gov/arcgis/rest/services/WRD/WRQS/FeatureServer

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
from tzlocal import get_localzone_name
import pytz

from MTDNRCdata import utilities

POD_URL = 'https://gis.dnrc.mt.gov/arcgis/rest/services/WRD/WRQS/FeatureServer/1/query'
POU_URL = 'https://gis.dnrc.mt.gov/arcgis/rest/services/WRD/WRQS/FeatureServer/2/query'
RESVR_URL = 'https://gis.dnrc.mt.gov/arcgis/rest/services/WRD/WRQS/FeatureServer/3/query'


class GetWaterRights(object):
    """
    A class that holds Water Right information for PODs, POUs, and Reservoirs for an area of interest.
    Currently, must be queried by a DNRC Administrative Basin Code or input geometry.

    Attributes
    -----------
    site_id : str
        a string representing the station ID(s) of interest (only 1 site functional as of this version)
    timestep : str
        specify either 'instant' for instantaneous data or 'daily' for average daily values; default is 'instant'
    """
    def __init__(self, basin_cd, geometry=None, out_format='spatial'):
        if out_format == 'spatial':
            self._format = 'geojson'
        elif out_format == 'table':
            self._format = 'pjson'
        else:
            self._format = 'pjson'

        self._basin = basin_cd
        self._IDs = self._getIDs()

        if geometry is None:
            self.in_geom = None
        else:
            self.in_geom = gpd.read_file(geometry).geometry

        self.pod =
        self.POU =
        self.resvr =

    def _getIDs(self):
        payload = {
            'where': "BOCA_CD='{0}'".format(self._basin),
            'returnIdsOnly': 'true',
            'f': 'pjson'
        }

        pod_response = requests.get(POD_URL, params=payload)
        pou_response = requests.get(POU_URL, params=payload)
        resvr_response = requests.get(RESVR_URL, params=payload)
        pod_rjson = pod_response.json()
        pou_rjson = pou_response.json()
        resvr_rjson = resvr_response.json()

        return {'POD_IDs': pod_rjson['objectIds'],
                'POU_IDs': pou_rjson['objectIds'],
                'RESVR_IDs': resvr_rjson['objectIds']}


    def _request_pods(self):
        if len(self._IDs['POD_IDs']) > 2000:
            blocks = int(len(self._IDs['POD_IDs']) / 2000) + 1
            incrmnt = int(len(self._IDs['POD_IDs']) / blocks)

            rjson = {}
            strt = 0
            for b in range(blocks):
                strt_blck = strt
                end_blck = strt + incrmnt
                payload = {
                    'where': """BOCA_CD='{0}'
                                AND (OBJECTID BETWEEN {1} AND {2})""".format(self._basin, strt_blck, end_blck),
                    'outFields': '*',
                    'f': self._format
                }
                response = requests.get(POD_URL, params=payload)
                rjson.update(response.json())
                ## if output = table join feature lists list1 += list2 in loop
                ## if output = spatial, join within GeoDataframes
                ## if want geojson or shapefile out...need to code method for that
        return rjson['features']