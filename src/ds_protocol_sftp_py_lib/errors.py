"""
**File:** ``errors.py``
**Region:** ``ds_protocol_sftp_py_lib/dataset``

Description
-----------
SFTP dataset-specific exceptions.
"""

from __future__ import annotations

from typing import Any

from ds_resource_plugin_py_lib.common.resource.dataset.errors import DatasetException


class FileExistsError(DatasetException):
    """Raised when the target file already exists."""

    def __init__(
        self,
        message: str = "Target file already exists on SFTP server.",
        code: str = "DS_DATASET_SFTP_FILE_EXISTS_ERROR",
        status_code: int = 409,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code, status_code, details)
