"""pytest configuration for diracx-api tests.

This conftest.py runs before any test module is collected. It saves
references to real cwl_utils and ruamel.yaml module objects so that
the integration test (test_job_wrapper_integration.py) can restore them
during test execution, even if other test files mock these modules at
collection time.
"""

from __future__ import annotations

import sys

# Pre-load and save real module objects BEFORE any test file can mock them.
# test_job_wrapper.py and test_job_wrapper_sandbox.py mock cwl_utils.parser
# and related modules at collection time (module-level code). By saving
# references here (conftest runs first), the integration test can use
# these real objects regardless of what sys.modules contains later.
import cwl_utils.parser as _cwl_parser
import cwl_utils.parser.cwl_v1_2 as _cwl_v1_2
import ruamel.yaml as _ruamel_yaml

# Store real class/function references as pytest fixtures accessible globally
_REAL_CWL_PARSER = _cwl_parser
_REAL_CWL_V1_2 = _cwl_v1_2
_REAL_RUAMEL_YAML = _ruamel_yaml

# Store the original module objects keyed by their sys.modules names so
# the integration test can temporarily restore them
_REAL_MODULES: dict[str, object] = {
    "cwl_utils.parser": _cwl_parser,
    "cwl_utils.parser.cwl_v1_2": _cwl_v1_2,
    "ruamel.yaml": _ruamel_yaml,
    # Also store attribute references
    "cwl_utils.parser.save": _cwl_parser.save,
    "cwl_utils.parser.cwl_v1_2.CommandLineTool": _cwl_v1_2.CommandLineTool,
    "cwl_utils.parser.cwl_v1_2.File": _cwl_v1_2.File,
    "cwl_utils.parser.cwl_v1_2.Workflow": _cwl_v1_2.Workflow,
    "cwl_utils.parser.cwl_v1_2.ExpressionTool": _cwl_v1_2.ExpressionTool,
    "cwl_utils.parser.cwl_v1_2.Saveable": _cwl_v1_2.Saveable,
    "ruamel.yaml.YAML": _ruamel_yaml.YAML,
}

# Make these accessible to tests via the conftest module itself
sys.modules[__name__] = sys.modules.get(__name__, type(sys)(__name__))
