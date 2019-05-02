"""Methods used for building and parsing urls used for read and find callbacks."""
from typing import List, Dict, Tuple
import urllib
import tregex

REPO_IDENTIFIER = 'netatmo'
PARAMETERS = {'device_name', 'module_name', 'data_type'}

query_pattern = tregex.TregexCompiled(r'(\w+)=(.*?)(?:&|$)')


class NetatmoUrlParseError(Exception):
    """Errors raised by the Netatmo url parsers."""
    pass


def create_ts_id(*, device_name: str, data_type: str, module_name: str = '') -> str:
    """Create a valid ts url from a netatmo device_name, module_name and data_type to identify a timeseries. If
    measurement resides in a NetatmoDevice, module can be left blank."""
    return f'{REPO_IDENTIFIER}://?device_name={device_name}&module_name={module_name}&data_type={data_type}'


def parse_ts_id(*, ts_id: str) -> Dict[str, str]:
    """Create a valid ts url from a netatmo device_name, module_name and data_type to identify a timeseries."""
    parse = urllib.parse.urlparse(ts_id)
    if parse.scheme != REPO_IDENTIFIER:
        raise NetatmoUrlParseError(f'ts_id scheme does not match repository name: '
                                   f'ts_id={parse.scheme}, repo={REPO_IDENTIFIER}')

    match: List[Tuple[str, str]] = query_pattern.to_tuple(parse.query)
    if not all(m[0] in PARAMETERS for m in match):
        raise NetatmoUrlParseError(f'ts_id url does not contain the correct parameters: {PARAMETERS}')
    return dict(match)


def create_ts_query(*, device_name: str, data_type: str, module_name: str = '') -> str:
    """Create a valid query url from a netatmo device_name, module_name and data_type to identify a timeseries.
    Uses the same format as NetatmoRepository.create_ts_id(). If
    measurement resides in a NetatmoDevice, module can be left blank."""
    return create_ts_id(device_name=device_name, module_name=module_name, data_type=data_type)


def parse_ts_query(*, ts_query) -> Dict[str, str]:
    """Create a valid ts url from a netatmo device_name, module_name and data_type to identify a timeseries."""
    return parse_ts_id(ts_id=ts_query)
