# Example script to get all daily discharge data for all StAGE stations
import pandas as pd

from MTDNRCdata.stage import GetSite, site_list, get_location_parameters, get_sites_geojson

# Use site_list() function to query all available sites on the StAGE Web Interface
sites = site_list()

# Query all sites that have Discharge Data
site_data = []
for index, row in sites.iterrows():
    site = row['attributes.LocationCode']
    # Use get_location_parameters() function to see what datasets are available for each site
    p = get_location_parameters(site)
    # Check if site has valid dataset parameters (some new sites will not)
    if not p.empty:
        # If Daily Discharge is available, extract data for that site ID, otherwise skip it
        if 'Discharge Daily' in list(p['attributes.ParameterLabel'] + ' ' + p['attributes.ComputationPeriod']):
            location = GetSite(row['attributes.LocationCode'], timestep='daily', dataset='QR', start='1900-01-01', end='2024-05-08')
            data = location.data.drop_duplicates(subset='Date', inplace=True)
            site_data.append(data[['Date', 'RecordedValue', 'SiteID']])
        # Try different query for improperly labeled parameters
        elif 'Discharge.Daily Average' in str(p['attributes.SensorCode']):
            location = GetSite(row['attributes.LocationCode'], timestep='instant', dataset='QR', start='1900-01-01',
                               end='2024-05-08')
            data = location.data
            data.rename(columns={'Datetime': 'Date'}, inplace=True)
            data.drop_duplicates(subset='Date', inplace=True)
            site_data.append(data[['Date', 'RecordedValue', 'SiteID']])
        else:
            print("No Daily Discharge found for {0}".format(site))
    else:
        print("{0} has no listed parameters".format(site))

All_Data = pd.concat(site_data, ignore_index=True)
All_Data.to_csv('StAGE_All_Daily.csv')

# Use the default behavior of get_sites_geojson() function to query the spatial locations of all sites
sgjson = get_sites_geojson()
# Write to file
with open('StAGE_Site_Locations.geojson', 'w') as f:
    f.write(sgjson.text)

if __name__ == '__main__':
    pass
