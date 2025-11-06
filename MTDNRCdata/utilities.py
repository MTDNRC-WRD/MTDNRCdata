"""
Utility functions used by stage.py
"""

from datetime import datetime, timezone, timedelta
import pytz
import requests
import geopandas as gpd

stage_tz = 'US/Mountain'

def aq_datetime_now():
    t_now = datetime.now(timezone.utc)
    t_delta = t_now - timedelta(hours=12)
    t_now = t_now.strftime(f"%Y-%m-%dT%H:%M:%SZ")
    t_delta = t_delta.strftime(f"%Y-%m-%dT%H:%M:%SZ")
    return t_now, t_delta

def aq_datetime_formatter(date_str):
    """formats date string 'yyyy-mm-dd' to AQUARIUS friendly string"""
    try:
        ts = datetime.strptime(date_str, "%Y-%m-%d")
        ts_out = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
        return ts_out

    except Exception as e:
        print(f"Error occurred loading date info: {e}")


def datetime_to_unix(date_str):
    """
    Function that takes date string formatted "YYYY-mm-dd"; '%Y-%m-%d' in local time and returns UNIX Timestamp
    :param date_str: string of date formatted "YYYY-mm-dd"
    :return: UNIX Timestamp (seconds from Epoch)
    """
    dt_naive = datetime.strptime(date_str, "%Y-%m-%d")
    if dt_naive > datetime(1970, 1, 1):
        ux_ts = dt_naive.timestamp()
    else:
        ux_ts = (dt_naive - datetime(1970, 1, 1)).total_seconds()
    return int(ux_ts)


def date_to_unix_naive(date_str):
    epoch = datetime(1970, 1, 1)
    dt_naive = datetime.strptime(date_str, "%Y-%m-%d")
    ux_ts = (dt_naive - epoch).total_seconds()
    return int(ux_ts)


def offset_unix(timestamp):
    tz = pytz.timezone(stage_tz)
    if timestamp > 0:
        dt = datetime.utcfromtimestamp(timestamp)
    else:
        dt = datetime(1970, 1, 1) + timedelta(seconds=timestamp)
    dt = pytz.utc.localize(dt)
    offset = dt.astimezone(tz).utcoffset().total_seconds()
    ux_off = timestamp + offset
    return ux_off

## Depricated
# def offset_unix(timestamp, utc_offset, units='H'):
#     if units == 'H':
#         off_s = utc_offset * 3600
#     elif units == 'S':
#         off_s = utc_offset
#     else:
#         off_s = 0
#         print("Offset Units not recognized: Only hours ('H') and seconds ('S') are supported for offset units.")
#         print("Assuming UTC + 0.00")
#     ts_offset = timestamp + off_s
#     return ts_offset

## Depricated
# def utc_offset_from_str(tzstring):
#     utc_str = re.findall(r'\d+', tzstring)
#     if '-' in tzstring:
#         utc_off = -int(utc_str[0])
#     elif '-' not in tzstring and int(utc_str[0]) != 0:
#         utc_off = int(utc_str[0])
#     elif int(utc_str[0]) == 0:
#         utc_off = int(utc_str[0])
#     else:
#         utc_off = 0
#     return utc_off


def round_seconds(obj: datetime) -> datetime:
    if obj.microsecond >= 500_000:
        obj += timedelta(seconds=1)
    return obj.replace(microsecond=0)


def get_previous_timerange(last=2, units='H', unix=True):
    if units == 'D':
        tdel = timedelta(days=last)
    elif units == 'H':
        tdel = timedelta(hours=last)
    elif units == 'S':
        tdel = timedelta(seconds=last)
    else:
        print("Entered time units are invalid. Setting 'now' = True.")
        tdel = timedelta(hours=1)

    if unix is True:
        tnow = round_seconds(datetime.now(timezone.utc))
        epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)
        strt = round_seconds(tnow - tdel)
        strt_ux = (strt - epoch).total_seconds()
        end_ux = (tnow - epoch).total_seconds()
        return tuple([strt_ux, end_ux])
    else:
        tnow = round_seconds(datetime.now())
        strt = round_seconds(tnow - tdel)
        end = tnow
        return tuple([strt, end])


def subset_date_range(start, end, interval, max_size=10000):
    start = datetime.strptime(start, "%Y-%m-%d")
    end = datetime.strptime(end, "%Y-%m-%d")
    diff = (end - start) / interval
    for i in range(interval):
        yield (start + diff * i).strftime("%Y%m%d")
    yield end.strftime("%Y%m%d")

def count_records(service_url, query_payload):
    payload = query_payload.copy()
    payload.update({"returnCountOnly": 'true'})
    payload.update({"f": "json"})
    query_json = requests.get(f"{service_url}/query", params=payload).json()
    service_json = requests.get(service_url, params={'f': 'json'}).json()
    tot_records = query_json['count']
    step = service_json['maxRecordCount']

    return tot_records, step

def build_wrqs_where_query(
        wr_number=None,
        basin_code=None,
        county=None,
        status=None,
        purpose=None,
        wrtype=None
    ):

    list_join = "','"
    list_in_query = lambda key, item: f"({key} IN ('{list_join.join(item)}'))"
    str_query = lambda key, item: f"({key}='{item}')"
    like_list_join = "%') OR (PURPOSES LIKE '%"
    purpose_list_q = lambda key, item: f"({key} LIKE '%{like_list_join.join(item)}%')"
    purpose_str_q = lambda key, item: f"({key} LIKE '%{item}%')"

    query_args = {
        'WR_NUMBER': wr_number,
        'BOCA_CD': basin_code,
        'COUNTY': county,
        'WR_STATUS': status,
        'PURPOSES': purpose,
        'WR_TYPE': wrtype
    }

    qry_strs = []
    for k, i in query_args.items():
        if i is None:
            continue
        elif isinstance(i, str):
            if k == 'PURPOSES':
                qs = purpose_str_q(k, i)
            else:
                qs = str_query(k, i)
        elif isinstance(i, list):
            if k == 'PURPOSES':
                qs = purpose_list_q(k, i)
            else:
                qs = list_in_query(k, i)
        else:
            raise ValueError(f"The argument for query parameter {k} is neither string nor list.")

        qry_strs.append(qs)

    if len(qry_strs) == 0:
        result = None
    elif len(qry_strs) == 1:
        result = qry_strs[0]
    else:
        result = ' AND '.join(qry_strs)

    return result

def geojson_request_to_geodf(query_url, payload):
    payload = payload.copy()
    if payload['f'] != 'geojson':
        payload['f'] = 'geojson'

    if payload['returnGeometry'] != 'true':
        payload['returnGeometry'] = 'true'

    res_req = requests.get(query_url, params=payload)
    geodf = gpd.GeoDataFrame.from_features(res_req.json()['features'])
    #geodf = geodf.set_geometry('geometry')

    return geodf
