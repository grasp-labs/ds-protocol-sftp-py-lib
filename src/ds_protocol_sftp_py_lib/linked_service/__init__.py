"""
**File:** ``__init__.py``
**Region:** ``ds_protocol_sftp_py_lib/linked_service``

SFTP Linked Service

This module implements a linked service for SFTP connections.

Example:
    >>> from ds_protocol_sftp_py_lib.enums import AuthType
    >>> linked_service = SftpLinkedService(
    ...     id=uuid.uuid4(),
    ...     name="example::linked_service",
    ...     version="1.0.0",
    ...     settings=SftpLinkedServiceSettings(
    ...         host="sftp.example.com",
    ...         username="user",
    ...         password="password123",
    ...         encrypted_credential="encrypted_cred",
    ...         private_key=None,
    ...         passphrase=None,
    ...         timeout=30.0,
    ...         host_key_fingerprint=None,
    ...         host_key_validation=True,
    ...         port=22,
    ...     ),
    ... )
    >>> linked_service.connect()
"""

from .sftp import (
    SftpLinkedService,
    SftpLinkedServiceSettings,
)

__all__ = [
    "SftpLinkedService",
    "SftpLinkedServiceSettings",
]
