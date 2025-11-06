from datetime import datetime, timedelta

import json
# print(json.dumps(response, indent=4))
import pandas as pd
import requests

import MTDNRCdata.utilities as utils
# https://develop-1.dev.aquariusdev.net/AQUARIUS/Publish/v2/swagger-ui/#!/GetRatingCurveList/RatingCurveListServiceRequest_Get

class Aquarius:
    """A Python client for interacting with the AQUARIUS Publish API.

    This class provides methods to retrieve time series data, unique identifiers,
    and metadata from an AQUARIUS Time-Series server.

    Attributes:
        username: AQUARIUS server username for authentication.
        password: AQUARIUS server password for authentication.
        server_name: AQUARIUS server hostname.
    """

    def __init__(self, username, password):
        """Initializes the Aquarius API client.

        Args:
            username: AQUARIUS server username.
            password: AQUARIUS server password.
        """
        self.username = username
        self.password = password
        self.server_name = 'dnrhln6386.state.mt.ads'


    def call_aquarius(self, params, request):
        """Makes a generic request to the AQUARIUS Publish API.

        This is a low-level method used by other methods in the class to interact
        with the AQUARIUS API endpoints.

        Args:
            params: Dictionary of query parameters to pass to the API endpoint.
            request: The API endpoint name.

        Returns:
            A dictionary containing the JSON response from the API, or None if
            an HTTP error occurs.
        """
        site_data = []
        response = requests.get(
            url=f'http://{self.server_name}/AQUARIUS/Publish/v2/{request}',
            auth=(self.username, self.password),
            params=params)
        try:
            response.raise_for_status()
            series_data = response.json()
            return series_data
        except requests.exceptions.HTTPError as e:
            print(f"Error occurred with input parameters: {e}")


    def get_timeseries_corrected(self, timeseries_id, start_day, end_day):
        """Retrieves corrected time series data for a specific time series ID.

        Fetches data points from the AQUARIUS server and returns them as a pandas
        DataFrame with formatted timestamps and values. Handles edge cases like
        24:00:00 timestamps (midnight of next day).

        Args:
            timeseries_id: The unique identifier for the time series (GUID).
            start_day: Start date in 'YYYY-MM-DD' format.
            end_day: End date in 'YYYY-MM-DD' format.

        Returns:
            A pandas DataFrame with two columns:
                - 'timestamp': Formatted as 'YYYY-MM-DD HH:MM'.
                - Unit column: Column name is the unit from API (e.g., 'ft^3/s'),
                  values are the numeric measurements.
        """
        start_time = utils.aq_datetime_formatter(start_day)
        end_time = utils.aq_datetime_formatter(end_day)
        aq_params = {'TimeSeriesUniqueId': timeseries_id,
                     'QueryFrom': start_time,
                     'QueryTo': end_time}
        request = 'GetTimeSeriesCorrectedData'
        ts_json = self.call_aquarius(aq_params, request)
        # Extract points
        points = ts_json.get('Points', [])
        # Create lists for timestamps and values
        timestamps = []
        values = []

        for point in points:
            # Get the timestamp string
            timestamp_str = point['Timestamp']

            # Remove timezone and fractional seconds
            timestamp_clean = timestamp_str.split('.')[0]  # Gets "2025-09-30T24:00:00"

            # Handle the 24:00:00 case (which means midnight of next day)
            if 'T24:' in timestamp_clean:
                # Replace 24:00:00 with 00:00:00 and add one day
                timestamp_clean = timestamp_clean.replace('T24:', 'T00:')
                dt = datetime.strptime(timestamp_clean, '%Y-%m-%dT%H:%M:%S')
                dt = dt + timedelta(days=1)
            else:
                # Normal parsing
                dt = datetime.strptime(timestamp_clean, '%Y-%m-%dT%H:%M:%S')

            # Format to 'yyyy-mm-dd hh:mm'
            formatted_timestamp = dt.strftime('%Y-%m-%d %H:%M')
            # Extract the numeric value
            value = point['Value']['Numeric']

            timestamps.append(formatted_timestamp)
            values.append(value)

        # Create DataFrame
        df = pd.DataFrame({
            'timestamp': timestamps,
            ts_json['Unit']: values
        })

        return df


    def get_timeseries_ids(self, gage_list):
        """Retrieves all time series unique IDs and metadata for gage locations.

        For each gage location, queries the AQUARIUS API to get all available time
        series (different parameters, labels, etc.) and compiles them into a single
        DataFrame with metadata.

        Args:
            gage_list: List of location identifiers.

        Returns:
            A pandas DataFrame with columns:
                - 'UniqueId': Time series unique identifier (GUID).
                - 'LocationIdentifier': Gage location identifier.
                - 'Parameter': Parameter name (e.g., 'Discharge', 'Stage').
                - 'Unit': Unit of measurement (e.g., 'ft^3/s', 'ft').
                - 'Label': Time series label (e.g., 'Daily Average', 'discharge').

        Note:
            This method queries recent data to retrieve active time series metadata.
            It may take time for large gage lists.
        """
        df_columns = ['UniqueId', 'LocationIdentifier', 'Parameter', 'Unit', 'Label']
        id_df = pd.DataFrame(columns=df_columns)
        tnow, tdelta = utils.aq_datetime_now()

        # get all ts_id's for each gage location
        for g_id in gage_list:
            params = {'LocationIdentifier': g_id}
            request = 'GetTimeSeriesUniqueIdList'
            tsid_json = self.call_aquarius(params, request)
            tsids = tsid_json["TimeSeriesUniqueIds"]
            ts_ids = [key["UniqueId"] for key in tsids]

            site_data = []
            for i in ts_ids:
                params = {'TimeSeriesUniqueId': i,
                          "QueryFrom": tdelta,
                          "QueryTo": tnow
                          }
                request = 'GetTimeSeriesCorrectedData'
                series_data = self.call_aquarius(params, request)

                point_dict = {
                    "UniqueId": series_data["UniqueId"],
                    "LocationIdentifier": series_data["LocationIdentifier"],
                    "Parameter": series_data["Parameter"],
                    "Unit": series_data["Unit"],
                    "Label": series_data['Label']
                }

                site_data.append(point_dict)

            gage_df = pd.DataFrame.from_records(site_data)
            id_df = pd.concat([id_df, gage_df], ignore_index=True)

        return id_df


    def filter_timeseries(self, tsid_df, param, daily_average=False):
        """Filters time series DataFrame to select specific parameters and data types.

        Filters a DataFrame of time series IDs to return only the desired parameter
        type and optionally select between instantaneous or daily average data.

        Args:
            tsid_df: DataFrame containing time series metadata.
            param: Parameter to filter for. Supported values:
                -'Discharge'
                -'Stage'
                -'Voltage'
                -'Water Temp'
                -'Total Storage'
            daily_average: If True, selects daily average data. If False, selects
                instantaneous data (lowercase label). Only applies to 'Discharge',
                'Stage', and 'Water Temp'. Defaults to False.

        Returns:
            A pandas DataFrame containing only matching time series.
        """
        filtered_df = tsid_df[tsid_df['Parameter'] == param]
        if param in ('Discharge', 'Stage', 'Water Temp'):
            if not daily_average:
                filtered_df = filtered_df[filtered_df['Label'] == param.lower()]
            else:
                filtered_df = filtered_df[filtered_df['Label'] == 'Daily Average']

        return filtered_df

    def get_rating_model_ids(self, location_identifier, return_latest=True):
        """Retrieves rating model identifiers for a location.

        Args:
            location_identifier: The gage location identifier.
            return_latest: If True, returns only the Identifier of the most recent
                rating model as a string. If False, returns a DataFrame of all
                rating models. Defaults to False.

        Returns:
            If return_latest is True: A string containing the RatingModelIdentifier
                of the most recently edited rating model.
            If return_latest is False: A pandas DataFrame containing rating model
                metadata including RatingModelIdentifier, Parameter, Label, and
                other details.
        """
        params = {'LocationIdentifier': location_identifier}
        request = 'GetRatingModelDescriptionList'
        response = self.call_aquarius(params, request)

        rating_models = response.get('RatingModelDescriptions', [])

        if return_latest:
            if rating_models:
                return rating_models[0]['Identifier']
            else:
                return None
        else:
            return pd.DataFrame(rating_models)


    def get_discharge_from_rating(self, rating_model_id, stage_array):
        """Calculates discharge values from stage values using a rating curve.

        Args:
            rating_model_id: The rating model identifier.
            stage_array: List or array of stage values to convert to discharge.

        Returns:
            A pandas DataFrame with 'Stage' and 'Discharge' columns.
        """
        params = {
            'RatingModelIdentifier': rating_model_id,
            'InputValues': stage_array
        }
        request = 'GetRatingModelOutputValues'
        response = self.call_aquarius(params, request)

        discharge_values = response.get('OutputValues', [])

        df = pd.DataFrame({
            'Stage': stage_array,
            'Discharge': discharge_values
        })

        return df


    def get_stage_from_rating(self, rating_model_id, discharge_array):
        """Calculates stage values from discharge values using a rating curve.

        Args:
            rating_model_id: The rating model identifier.
            discharge_array: List or array of discharge values to convert to stage.

        Returns:
            A pandas DataFrame with 'Stage' and 'Discharge' columns.
        """
        params = {
            'RatingModelIdentifier': rating_model_id,
            'OutputValues': discharge_array
        }
        request = 'GetRatingModelInputValues'
        response = self.call_aquarius(params, request)

        stage_values = response.get('InputValues', [])

        df = pd.DataFrame({
            'Stage': stage_values,
            'Discharge': discharge_array
        })

        return df



    def get_site_info(self, location_identifier):
        """Retrieves location metadata and information.

        Args:
            location_identifier: The location identifier (e.g., '41H 08900').

        Returns:
            A dictionary containing location metadata including:
                - LocationIdentifier: The location ID
                - LocationName: Name of the location
                - Latitude: Latitude coordinate
                - Longitude: Longitude coordinate
                - Elevation: Elevation value and units
                - UtcOffset: UTC time offset
                - Description: Location description
                - LocationType: Type of location
                - And other location-specific metadata
        """
        params = {'LocationIdentifier': location_identifier}
        request = 'GetLocationData'
        response = self.call_aquarius(params, request)
        point_dict = {
            "Identifier": response["Identifier"],
            "LocationName": response["LocationName"],
            "Description": response["Description"],
            "LocationType": response["LocationType"],
            "Latitude": response["Latitude"],
            "Longitude": response["Longitude"],
            "Srid": response["Srid"],
            "ElevationUnits": response["ElevationUnits"],
            "Elevation": response["Elevation"],
            "UtcOffset": response["UtcOffset"],
        }
        return point_dict

