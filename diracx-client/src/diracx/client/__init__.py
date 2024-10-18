from .extensions import initialize_client

initialize_client()


from .generated import *  # pylint: disable=unused-wildcard-import
from .patches import DiracClient
