import MTDNRCdata.stage as stage
import pandas as pd
import requests

from MTDNRCdata.stage import default_query_params
from config import LOCATIONS_URL, FORMAT, STATUS_TYPES

site_ids = ['41O 03000', '41O 02000']

stage.get_location_parameters(site_ids)

stage.get_site_locations()
