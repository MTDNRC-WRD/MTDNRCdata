# Script that gets daily discharge for all StAGE stations
import pandas as pd
import geopandas as gpd
import json

from MTDNRCdata.stage import GetSite, site_list, get_location_parameters, get_sites_geojson

# Get gage id's from csv
gage_df = pd.read_csv('gage_list.csv')
gage_list = gage_df['Gage_id'].tolist()

start_date = '1900-01-01'
end_date = '2024-12-31'
timestep = 'daily'
dataset = 'QR'

# Query all sites that have Discharge Data
site_data = []
for i in gage_list:
    site = i
    # Use get_location_parameters() function to see what datasets are available for each site
    p = get_location_parameters(site)
    # Check if site has valid dataset parameters (some new sites will not)
    if not p.empty:
        # If Daily Discharge is available, extract data for that site ID, otherwise skip it
        if 'Discharge Daily' in list(p['attributes.ParameterLabel'] + ' ' + p['attributes.ComputationPeriod']):
            location = GetSite(i, timestep='daily', dataset='QR', start=start_date, end=end_date)
            data = location.data.sort_values(by='Date')
            data.drop_duplicates(subset='Date', inplace=True)
            export_df = data[['Date', 'RecordedValue', 'SiteID']]
            export_df.to_csv(f'timeseries_data/{i}.csv')
        # Try different query for improperly labeled parameters
        elif 'Discharge.Daily Average' in str(p['attributes.SensorCode']):
            location = GetSite(i, timestep='instant', dataset='QR', start=start_date, end=end_date)
            data = location.data
            data.rename(columns={'Datetime': 'Date'}, inplace=True)
            data = data.sort_values(by='Date')
            data.drop_duplicates(subset='Date', inplace=True)
            export_df = data['Date', 'RecordedValue', 'SiteID']
            export_df.to_csv(f'timeseries_data/{i}.csv')
        else:
            print("No Daily Discharge found for {0}".format(site))
    else:
        print("{0} has no listed parameters".format(site))

# Use the default behavior of get_sites_geojson() function to query the spatial locations of all sites
sgjson = get_sites_geojson()
# Write to file
with open('StAGE_Site_Locations.geojson', 'w') as f:
    f.write(sgjson.text)

sl_gdf = gpd.read_file('StAGE_Site_Locations.geojson')
site_coords_df = pd.DataFrame(columns=['SiteID', 'x', 'y'])

for geom in sl_gdf.geometry:
    site = sl_gdf.loc[sl_gdf['geometry'] == geom, 'LocationCode'].values[0]
    new_row = {'SiteID': site, 'x': geom.x, 'y': geom.y}
    site_coords_df = pd.concat([site_coords_df, pd.DataFrame([new_row])], ignore_index=True)

site_coords_df.to_csv('StAGE_site_locations.csv')

if __name__ == '__main__':
    pass
