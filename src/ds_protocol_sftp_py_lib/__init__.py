"""
**File:** ``__init__.py``
**Region:** ``ds-protocol-sftp-py-lib``

Description
-----------
A Python package from the ds-protocol-sftp-py-lib library.

Example
-------
.. code-block:: python

    from ds_protocol_sftp_py_lib import __version__

    print(f"Package version: {__version__}")
"""

from importlib.metadata import version

from .dataset import SftpDataset, SftpDatasetSettings
from .linked_service import SftpLinkedService, SftpLinkedServiceSettings

__version__ = version("ds-protocol-sftp-py-lib")

__all__ = ["SftpDataset", "SftpDatasetSettings", "SftpLinkedService", "SftpLinkedServiceSettings", "__version__"]
