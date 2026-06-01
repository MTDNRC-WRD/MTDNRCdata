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

import pandas as pd
import geopandas as gpd
from pathlib import Path
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

from MTDNRCdata import utilities
from config import POD_URL, POU_URL, RESVR_URL

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

class GetWaterRights:
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
    def __init__(self, wr_number=None,
                 geometry=None,
                 basin_code=None,
                 county=None,
                 status=None,
                 purpose=None,
                 wrtype=None):

        self._where_query = utilities.build_wrqs_where_query(
            wr_number=wr_number,
            basin_code=basin_code,
            county=county,
            status=status,
            purpose=purpose,
            wrtype=wrtype,
        )

        if geometry is None:
            self.in_geom = None
        elif isinstance(geometry, (str, Path)):
            self.in_geom = gpd.read_file(geometry)
            if len(self.in_geom) > 1:
                warnings.warn("Input geometry has more than one feature, defaulting to the 1st geometry.",
                              UserWarning,
                              stacklevel=2)
                self.in_geom = self.in_geom.iloc[[0], :]
        elif isinstance(geometry, gpd.GeoDataFrame):
            self.in_geom = geometry
            if len(self.in_geom) > 1:
                warnings.warn("Input geometry has more than one feature, defaulting to the 1st geometry.",
                              UserWarning,
                              stacklevel=2)
                self.in_geom = self.in_geom.iloc[[0], :]
        else:
            raise ValueError("The input geometry is not a string path or geopandas GeoDataFrame.")

        self.pod = GetWaterRights.get_pods(self._where_query, geometry=self.in_geom)
        self.pou = GetWaterRights.get_pous(self._where_query, geometry=self.in_geom)
        self.reservoir = GetWaterRights.get_reservoirs(self._where_query, geometry=self.in_geom)


    @staticmethod
    def get_pods(where_str=None, geometry=None):
        query_url = f"{POD_URL}/query"
        # construct query
        payload = default_query_params.copy()
        in_geom = None

        if (where_str is None) and (geometry is None):
            pass

        elif (where_str is None) and geometry is not None:

            if isinstance(geometry, (str, Path)):
                in_geom = gpd.read_file(geometry)
            elif isinstance(geometry, gpd.GeoDataFrame):
                in_geom = geometry
            else:
                raise ValueError("The input geometry is neither a string path nor a geopandas GeoDataFrame.")

            if in_geom.crs.to_authority() != 4326:
                in_geom = in_geom.to_crs(4326)

            payload.update({'geometry': ','.join(list(in_geom.total_bounds.astype(str)))})

        elif (where_str is not None) and (geometry is None):
            payload.update({'where': where_str,
                            'geometry': None})

        elif (where_str is not None) and (geometry is not None):

            if isinstance(geometry, (str, Path)):
                in_geom = gpd.read_file(geometry)
            elif isinstance(geometry, gpd.GeoDataFrame):
                in_geom = geometry
            else:
                raise ValueError("The input geometry is neither a string path nor a geopandas GeoDataFrame.")

            if in_geom.crs.to_authority() != 4326:
                in_geom = in_geom.to_crs(4326)

            payload.update({'where': where_str,
                            'geometry': ','.join(list(in_geom.total_bounds.astype(str)))})

        else:
            raise ValueError("The input arguments were not valid, please check inputs and try again.")

        tot_records, step = utilities.count_records(POD_URL, payload)

        if tot_records <= step:
            out = utilities.geojson_request_to_geodf(query_url, payload)
            out = out.set_crs(4326)
        else:
            payload.update({
                'orderByFields': 'OBJECTID',
                'resultRecordCount': step
            })
            with ThreadPoolExecutor(max_workers=4) as ex:
                futures = []
                for offset in range(0, tot_records, step):
                    payload['resultOffset'] = offset
                    submit = ex.submit(utilities.geojson_request_to_geodf, query_url, payload)
                    futures.append(submit)

                gdfs = [f.result() for f in as_completed(futures)]

            out = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
            out = out.set_crs(4326)

        if in_geom is not None:
            out_clipped = out.loc[out.intersects(in_geom.geometry[0]),:]
            return out_clipped
        else:
            return out

    @staticmethod
    def get_pous(where_str=None, geometry=None):
        query_url = f"{POU_URL}/query"
        # construct query
        payload = default_query_params.copy()
        in_geom = None

        if (where_str is None) and (geometry is None):
            pass

        elif (where_str is None) and geometry is not None:

            if isinstance(geometry, (str, Path)):
                in_geom = gpd.read_file(geometry)
            elif isinstance(geometry, gpd.GeoDataFrame):
                in_geom = geometry
            else:
                raise ValueError("The input geometry is neither a string path nor a geopandas GeoDataFrame.")

            if in_geom.crs.to_authority() != 4326:
                in_geom = in_geom.to_crs(4326)

            payload.update({'geometry': ','.join(list(in_geom.total_bounds.astype(str)))})

        elif (where_str is not None) and (geometry is None):
            payload.update({'where': where_str,
                            'geometry': None})

        elif (where_str is not None) and (geometry is not None):

            if isinstance(geometry, (str, Path)):
                in_geom = gpd.read_file(geometry)
            elif isinstance(geometry, gpd.GeoDataFrame):
                in_geom = geometry
            else:
                raise ValueError("The input geometry is neither a string path nor a geopandas GeoDataFrame.")

            if in_geom.crs.to_authority() != 4326:
                in_geom = in_geom.to_crs(4326)

            payload.update({'where': where_str,
                            'geometry': ','.join(list(in_geom.total_bounds.astype(str)))})

        else:
            raise ValueError("The input arguments were not valid, please check inputs and try again.")

        tot_records, step = utilities.count_records(POU_URL, payload)

        if tot_records <= step:
            out = utilities.geojson_request_to_geodf(query_url, payload)
            out = out.set_crs(4326)
        else:
            payload.update({
                'orderByFields': 'OBJECTID',
                'resultRecordCount': step
            })
            with ThreadPoolExecutor(max_workers=4) as ex:
                futures = []
                for offset in range(0, tot_records, step):
                    payload['resultOffset'] = offset
                    submit = ex.submit(utilities.geojson_request_to_geodf, query_url, payload)
                    futures.append(submit)

                gdfs = [f.result() for f in as_completed(futures)]

            out = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
            out = out.set_crs(4326)

        if in_geom is not None:
            out_clipped = out.loc[out.intersects(in_geom.geometry[0]), :]
            return out_clipped
        else:
            return out

    @staticmethod
    def get_reservoirs(where_str=None, geometry=None):
        query_url = f"{RESVR_URL}/query"
        # construct query
        payload = default_query_params.copy()
        in_geom = None

        if (where_str is None) and (geometry is None):
            pass

        elif (where_str is None) and geometry is not None:

            if isinstance(geometry, (str, Path)):
                in_geom = gpd.read_file(geometry)
            elif isinstance(geometry, gpd.GeoDataFrame):
                in_geom = geometry
            else:
                raise ValueError("The input geometry is neither a string path nor a geopandas GeoDataFrame.")

            if in_geom.crs.to_authority() != 4326:
                in_geom = in_geom.to_crs(4326)

            payload.update({'geometry': ','.join(list(in_geom.total_bounds.astype(str)))})

        elif (where_str is not None) and (geometry is None):
            payload.update({'where': where_str,
                            'geometry': None})

        elif (where_str is not None) and (geometry is not None):

            if isinstance(geometry, (str, Path)):
                in_geom = gpd.read_file(geometry)
            elif isinstance(geometry, gpd.GeoDataFrame):
                in_geom = geometry
            else:
                raise ValueError("The input geometry is neither a string path nor a geopandas GeoDataFrame.")

            if in_geom.crs.to_authority() != 4326:
                in_geom = in_geom.to_crs(4326)

            payload.update({'where': where_str,
                            'geometry': ','.join(list(in_geom.total_bounds.astype(str)))})

        else:
            raise ValueError("The input arguments were not valid, please check inputs and try again.")

        tot_records, step = utilities.count_records(RESVR_URL, payload)

        if tot_records <= step:
            out = utilities.geojson_request_to_geodf(query_url, payload)
            out = out.set_crs(4326)
        else:
            payload.update({
                'orderByFields': 'OBJECTID',
                'resultRecordCount': step
            })
            with ThreadPoolExecutor(max_workers=4) as ex:
                futures = []
                for offset in range(0, tot_records, step):
                    payload['resultOffset'] = offset
                    submit = ex.submit(utilities.geojson_request_to_geodf, query_url, payload)
                    futures.append(submit)

                gdfs = [f.result() for f in as_completed(futures)]

            out = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
            out = out.set_crs(4326)

        if in_geom is not None:
            out_clipped = out.loc[out.intersects(in_geom.geometry[0]), :]
            return out_clipped
        else:
            return out