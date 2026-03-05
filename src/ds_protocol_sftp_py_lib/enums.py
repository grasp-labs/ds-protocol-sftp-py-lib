"""
**File:** ``enums.py``
**Region:** ``ds_protocol_sftp_py_lib/enums``

Constants for SFTP protocol.

Example:
    >>> ResourceType.LINKED_SERVICE
    'ds.resource.linked-service.sftp'
    >>> ResourceType.DATASET
    'ds.resource.dataset.sftp'
"""

from enum import StrEnum


class ResourceType(StrEnum):
    """
    Sftp ResourceTypes.
    """

    LINKED_SERVICE = "ds.resource.linked-service.sftp"
    DATASET = "ds.resource.dataset.sftp"
