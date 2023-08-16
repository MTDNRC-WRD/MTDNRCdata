"""
Utility functions used by stage.py
"""

from datetime import datetime, timezone, timedelta
from tzlocal import get_localzone_name
import pytz
import re


def datetime_to_unix(date_str):
    """
    Function that takes date string formatted "YYYY-mm-dd"; '%Y-%m-%d' in local time and returns UNIX Timestamp
    :param date_str: string of date formatted "YYYY-mm-dd"
    :return: UNIX Timestamp (seconds from Epoch)
    """
    epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)
    loc_tz = pytz.timezone(get_localzone_name())
    dt_aware = loc_tz.localize(datetime.strptime(date_str, "%Y-%m-%d"))
    utc_dt = dt_aware.astimezone(pytz.utc)
    ux_ts = (utc_dt - epoch).total_seconds()
    return int(ux_ts)


def date_to_unix_naive(date_str):
    epoch = datetime(1970, 1, 1)
    dt_naive = datetime.strptime(date_str, "%Y-%m-%d")
    ux_ts = (dt_naive - epoch).total_seconds()
    return int(ux_ts)


def offset_unix(timestamp, utc_offset, units='H'):
    if units == 'H':
        off_s = utc_offset * 3600
    elif units == 'S':
        off_s = utc_offset
    else:
        off_s = 0
        print("Offset Units not recognized: Only hours ('H') and seconds ('S') are supported for offset units.")
        print("Assuming UTC + 0.00")
    ts_offset = timestamp + off_s
    return ts_offset


def utc_offset_from_str(tzstring):
    utc_str = re.findall(r'\d+', tzstring)
    if '-' in tzstring:
        utc_off = -int(utc_str[0])
    elif '-' not in tzstring and int(utc_str[0]) != 0:
        utc_off = int(utc_str[0])
    elif int(utc_str[0]) == 0:
        utc_off = int(utc_str[0])
    else:
        utc_off = 0
    return utc_off


def subset_date_range(start, end, interval, max_size=10000):
    start = datetime.strptime(start, "%Y-%m-%d")
    end = datetime.strptime(end, "%Y-%m-%d")
    diff = (end - start) / interval
    for i in range(interval):
        yield (start + diff * i).strftime("%Y%m%d")
    yield end.strftime("%Y%m%d")
