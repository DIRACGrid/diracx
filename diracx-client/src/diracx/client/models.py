from .generated.models import *  # pylint: disable=unused-wildcard-import

# TODO: replace with postprocess
from .patches.utils import DeviceFlowErrorResponse

__all__ = ("DeviceFlowErrorResponse",)
