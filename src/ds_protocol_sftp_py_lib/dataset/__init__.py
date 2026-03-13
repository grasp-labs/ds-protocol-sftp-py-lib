"""
**File**: `__init__.py`
**Region**: `ds_protocol_sftp_py_lib/dataset`

SFTP Dataset

This module implements the SFTP Dataset, which is a dataset that can be used to
read and write data from an SFTP server.

Example:
    >>> dataset = SftpDataset(
    ...     id=uuid.uuid4(),
    ...     name="My SFTP Dataset",
    ...     version="1.0",
    ...     deserializer=PandasDeserializer(),
    ...     serializer=PandasSerializer(),
    ...     settings=SftpDatasetSettings(
    ...         folder_path="/path/to/dataset.csv",
    ...         file_name="dataset.csv",
    ...     ),
    ...     linked_service=SftpLinkedService(
    ...         id=uuid.uuid4(),
    ...         name="My SFTP Linked Service",
    ...         version="1.0.0",
    ...         settings=SftpLinkedServiceSettings(
    ...             host="sftp.example.com",
    ...             port=22,
    ...             username="username",
    ...             password="password",
    ...         ),
    ...     )
    ... )
    ... dataset.read()
    ... data = dataset.output
"""

from .sftp import SftpDataset, SftpDatasetSettings

__all__ = [
    "SftpDataset",
    "SftpDatasetSettings",
]
