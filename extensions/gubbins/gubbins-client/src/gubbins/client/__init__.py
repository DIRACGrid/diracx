"""
This init file exposes what we want exported, as well as initializate the client extension magic
"""

# This must be here in order to initialize the MetaPathFinder
import diracx.client  # noqa

from .generated import *  # pylint: disable=unused-wildcard-import # noqa
from .patches import GubbinsClient  # noqa
