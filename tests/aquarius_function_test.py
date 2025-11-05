from MTDNRCdata.aquarius import Aquarius

gage_ids = ['41H 08900', '41H 08990', '41H 08950', '41H 2000']

# Initiate the class and enter credentials
AQ = Aquarius('', '')

# Print location information in terminal
for gid in gage_ids:
    info = AQ.get_site_info(gid)
    for key, value in info.items():
        print(f"{key}: {value}")
    print('\n')

# give list of gage names to get all unique timeseries id's
df_id = AQ.get_timeseries_ids(gage_ids)

# only select timeseries parameter you want
filter_df = AQ.filter_timeseries(df_id, 'Discharge', daily_average=False)

# loop through your df of parameters to call timeseries
for idx in filter_df.index:
    unique_id = filter_df.loc[idx, 'UniqueId']
    location = filter_df.loc[idx, 'LocationIdentifier']
    ts_df = AQ.get_timeseries_corrected(unique_id, '2025-10-01', '2025-11-01')
    print(location)
    print(ts_df)


# get rating curve id's for each discharge timeseries
rc_list = []
for i in gage_ids:
    rc_id = AQ.get_rating_model_ids(i)
    rc_list.append(rc_id)

# Convert stage to discharge
stages = [1.5, 2.0, 2.5, 3.0]
q_df = AQ.get_discharge_from_rating(rc_list[0], stages)
print(q_df)

# Convert discharge to stage (inverse)
discharges = [100, 200, 300, 400]
st_df = AQ.get_stage_from_rating(rc_list[0], discharges)
print(st_df)

