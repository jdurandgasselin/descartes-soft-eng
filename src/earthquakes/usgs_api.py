"""
This module contains functions and resources for calls to USGS public API
"""
import datetime
import pandas as pd
import urllib
USGS_ROOT_URL = r'https://earthquake.usgs.gov/fdsnws/event/1/'

METHODS = [
    'query',
    'count'
]

DEFAULT_METHOD = METHODS[0]

PARAMETERS_NORMALIZERS = {
'latitude': lambda x: str(x),
'longitude': lambda x: str(x),
'maxradiuskm': lambda x: str(x),
'minmagnitude': lambda x: str(x),
'startime': lambda x: x.isoformat(),
'endtime': lambda x: x.isoformat(),
}


def get_earthquake_data(
        latitude: float,
        longitude: float,
        radius: float,
        minimum_magnitude: float,
        start_date: datetime.datetime=None,
        end_date: datetime.datetime=None
) -> pd.DataFrame:
    """top level caller for requests to USGS public API"""
    PN = PARAMETERS_NORMALIZERS

    # agregates user & default parameters into normalized parameters dict
    params = {
        'format': 'csv',
        'limit': 200,
        'latitude': PN['latitude'](latitude),
        'longitude': PN['longitude'](longitude),
        'maxradiuskm': PN['maxradiuskm'](radius),
        'minmagnitude': PN['minmagnitude'](minimum_magnitude),
    }

    if start_date is not None:
        params['starttime'] = PN['startime'](start_date)

    if end_date is not None:
        params['endtime'] = PN['endtime'](end_date)

    # build fully qualified request url
    print(params)
    full_url = build_api_url('query', params)
    print(full_url)
    # make request
    data = _handle_request(full_url)

    try:
        # build output df from request data in CSV format
        df = pd.read_csv(data)
    except Exception as err:
        raise RuntimeError(f'Failed to build data array from USGS data. raw data:\n{data}\nerror:\n{str(err)}')

    return df


def _handle_request(full_url: str) -> str:
    try:
        response = urllib.request.urlopen(full_url)
    # todo: differentiate business errors from technical errors
    except Exception as err:
        raise RuntimeError(f'Request to USGS API failed with error: {str(err)}')

    else:
        if response.code != 200:
            raise RuntimeError(f'Request to USGS API returned error: {response.code}, {response.reason}')
        try:
            data = response.read().decode()

        except Exception as err:
            raise RuntimeError(f'Failed to parse response for USGS API: {str(err)}')

    return data

def build_api_url(method: str=DEFAULT_METHOD, parameters: dict=None) -> str:
    """
    Builds fully qualified request url (str) to USGS API using USGS_ROOT_URL and specified method & associated
    parameters
    """
    assert method in METHODS
    data = urllib.parse.urlencode(parameters) if parameters is not None else ''
    return USGS_ROOT_URL + method + '?' + data
# parse result and builds output df.

