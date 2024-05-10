#__init__.py#

from pathlib import Path
import tomli

path = Path(__file__).parent / 'StAGE_settings.toml'
with path.open(mode='rb') as f:
    settings = tomli.load(f)

LOCS_SPATIAL_URL = settings['URLs']['LOCS_SPATIAL_URL']
LOCATIONS_URL = settings['URLs']['LOCATIONS_URL']
LOCATIONDATA_URL = settings['URLs']['LOCATIONDATA_URL']
TIMESERIES_URL = settings['URLs']['TIMESERIES_URL']
AVAILABLE_DATASETS = settings['DATASET_INFO']['AVAILABLE_DATASETS']
STATUS_TYPES = settings['DATASET_INFO']['STATUS_TYPES']
INST_ONLY = settings['DATASET_INFO']['INSTANTANEOUS_ONLY']
LOCATION_FIELDS = settings['DATASET_INFO']['LOCATION_FIELDS']
TIMESERIES_FIELDS = settings['DATASET_INFO']['TIMESERIES_FIELDS']
FORMAT = settings['API_SETTINGS']['DEFAULT_RETURN_FORMAT']
