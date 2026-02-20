"""
**File:** ``__init__.py``
**Region:** ``ds_protocol_sftp_py_lib/utils/sftp``

SFTP utility subpackage.

This file exists to ensure tools (like Sphinx AutoAPI) treat this directory as a
proper Python package, so intra-package relative imports resolve correctly.
"""

from .config import SftpConfig
from .provider import Sftp

__all__ = [
    "Sftp",
    "SftpConfig",
]
