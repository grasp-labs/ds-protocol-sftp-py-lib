"""
**File:** ``enums.py``
**Region:** ``ds_protocol_sftp_py_lib/enums``

Constants for SFTP protocol.

Example:
    >>> ResourceType.LINKED_SERVICE
    'DS.RESOURCE.LINKED_SERVICE.SFTP'
    >>> ResourceType.DATASET
    'DS.RESOURCE.DATASET.SFTP'
"""

from enum import StrEnum


class ResourceType(StrEnum):
    """
    Sftp ResourceTypes.
    """

    LINKED_SERVICE = "DS.RESOURCE.LINKED_SERVICE.SFTP"
    DATASET = "DS.RESOURCE.DATASET.SFTP"
