import requests
import geopandas as gpd
import urllib

from MTDNRCdata import utilities
from MTDNRCdata.wrqs import GetWaterRights
from config import POD_URL

payload={
    'where' : "BOCA_CD='43B' AND PURPOSES='IRRIGATION'",
    'geometry': '-111.2, 45, -109.8, 46.1',
    'geometryType' : 'esriGeometryEnvelope',
    'spatialRel': 'esriSpatialRelIntersects',
    'inSR': '4326',
    'units': 'esriSRUnit_Foot',
    'outFields': '*',
    'returnGeometry': 'true',
    'outSR': 'true',
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
dnrc_gages = 'C:/Users/CNB968/OneDrive - MT/GitHub/MTDNRCdata/Examples/StAGE_Nat_Flow_Sites.shp'
UY_basin = 'D:/ArcGIS_Projects/Yellowstone/Upper Yellowstone/Vector/UY_Boundary_abv_Shields.shp'
dnrc_nat = gpd.read_file(dnrc_gages)
uygd = gpd.read_file(UY_basin)
bbox = ', '.join(list(uygd.total_bounds.round(3).astype(str)))

# test utilities function for counting records
print(utilities.count_records(POD_URL, payload))

# test the wrqs where query builder
whereq = utilities.build_wrqs_where_query(
    status='ACTIVE',
    purpose='IRRIGATION'
)
print(whereq)


test = GetWaterRights.get_pods(geometry=UY_basin)

payload.update({'where': whereq,
                'returnCountOnly': 'false',
                'geometry': None,
                'outSR': '4326',
                'orderByFields': 'OBJECTID',
                'resultRecordCount': 10000
                })
r = requests.get(f"{POD_URL}/query", params=payload)

fs = r.json()['features']

geoms = []
for f in fs:
    if f['geometry']:
        geoms.append(True)
    else:
        geoms.append(False)

payload['resultOffset'] = 0
g = utilities.geojson_request_to_geodf(f"{POD_URL}/query", payload)

payload['resultOffset'] = 10000
g2 = utilities.geojson_request_to_geodf(f"{POD_URL}/query", payload)

import pandas as pd
cat = gpd.GeoDataFrame(pd.concat([g, g2], ignore_index=True))
cat = cat.set_crs(4326)

from concurrent.futures import ThreadPoolExecutor, as_completed

from MTDNRCdata.wrqs import default_query_params
payload = default_query_params.copy()
whereq = utilities.build_wrqs_where_query(
    status='ACTIVE',
    purpose='IRRIGATION'
)
payload.update({'where': whereq})
payload.update({'geometry': bbox})
tot_records, step = utilities.count_records(POD_URL, payload)

payload.update({
    'orderByFields': 'OBJECTID',
    'resultRecordCount': tot_records
})

f = utilities.geojson_request_to_geodf(f"{POD_URL}/query", payload)

with ThreadPoolExecutor(max_workers=4) as ex:
    futures = []
    for offset in range(0, tot_records, step):
        payload['resultOffset'] = offset
        submit = ex.submit(utilities.geojson_request_to_geodf, f"{POD_URL}/query", payload)
        futures.append(submit)

    gdfs = [f.result() for f in as_completed(futures)]

out = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
out = out.set_crs(4326)

out.loc[out.intersects(uygd.to_crs(4326).geometry),:]