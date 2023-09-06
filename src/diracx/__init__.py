import logging
from importlib.metadata import PackageNotFoundError, version

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s | %(name)s | %(levelname)s | %(message)s")

try:
    __version__ = version("diracx")
except PackageNotFoundError:
    # package is not installed
    pass
