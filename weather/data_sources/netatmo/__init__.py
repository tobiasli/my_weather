"""Simplified imports of netatmo components."""
import os
import sys
# Get credentials:
if 'CONFIG_DIRECTORY' not in os.environ:
    raise EnvironmentError('Cannot find path netatmo configs in env var CONFIG_DIRECTORY.')

sys.path.append(os.environ['CONFIG_DIRECTORY'])

from netatmo_config import login
from weather.data_sources.netatmo.netatmo_domain import NetatmoDomain


def get_netatmo_domain():
    """Create an instance of the netatmo domain model."""
    return NetatmoDomain(**login)
