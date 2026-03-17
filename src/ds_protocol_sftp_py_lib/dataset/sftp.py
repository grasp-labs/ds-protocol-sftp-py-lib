"""
**File:** ``sftp.py``
**Region:** ``ds_protocol_sftp_py_lib/dataset/sftp``

SFTP Dataset

This module implements a dataset for SFTP connections.

Example:
    >>> from ds_protocol_sftp_py_lib.dataset import SftpDataset, SftpDatasetSettings
    >>> from ds_protocol_sftp_py_lib.linked_service import SftpLinkedService, SftpLinkedServiceSettings
    >>> from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
    >>> from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer
    >>> from ds_resource_plugin_py_lib.common.resource.dataset import DatasetStorageFormatType
    >>> dataset = SftpDataset(
    ...     deserializer=PandasDeserializer(format=DatasetStorageFormatType.JSON),
    ...     serializer=PandasSerializer(format=DatasetStorageFormatType.JSON),
    ...     settings=SftpDatasetSettings(
    ...         folder_path="/path/to/dataset",
    ...         file_name="dataset_*.json",
    ...     ),
    ...     linked_service=SftpLinkedService(
    ...         settings=SftpLinkedServiceSettings(
    ...             host="sftp.example.com",
    ...             username="user",
    ...             password="password123",
    ...             encrypted_credential="encrypted_cred",
    ...             private_key=None,
    ...             passphrase=None,
    ...             timeout=30.0,
    ...             host_key_fingerprint=None,
    ...             host_key_validation=True,
    ...             port=22,
    ...         ),
    ...     ),
    ... )
    >>> dataset.read()
    >>> data = dataset.output
"""

import builtins
import errno
import fnmatch
import mimetypes
import posixpath
from dataclasses import dataclass, field
from pathlib import PureWindowsPath
from typing import Any, Generic, TypeVar

import pandas as pd
from ds_common_logger_py_lib import Logger
from ds_common_serde_py_lib import Serializable
from ds_resource_plugin_py_lib.common.resource.dataset import (
    DatasetSettings,
    DatasetStorageFormatType,
    TabularDataset,
)
from ds_resource_plugin_py_lib.common.resource.dataset.errors import (
    CreateError,
    ListError,
    PurgeError,
    ReadError,
    UpsertError,
)
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError
from ds_resource_plugin_py_lib.common.serde.deserialize import PandasDeserializer
from ds_resource_plugin_py_lib.common.serde.serialize import PandasSerializer
from paramiko import SFTPAttributes

from ..enums import ResourceType
from ..errors import FileExistsError
from ..linked_service.sftp import SftpLinkedService

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class ListSettings(Serializable):
    """Settings for listing the SFTP dataset."""

    download: bool = False
    """Whether to download (supply the dataframe with content) the files when listing the SFTP dataset."""


@dataclass(kw_only=True)
class SftpDatasetSettings(DatasetSettings):
    """Settings for the SFTP dataset."""

    folder_path: str
    """Path to the folder containing the file(s) to read/write on the SFTP server."""

    file_name: str
    """Name of the file to read/write on the SFTP server."""

    list: ListSettings = field(default_factory=ListSettings)
    """Settings for listing the SFTP dataset."""


SftpDatasetSettingsType = TypeVar(
    "SftpDatasetSettingsType",
    bound=SftpDatasetSettings,
)
SftpLinkedServiceType = TypeVar(
    "SftpLinkedServiceType",
    bound=SftpLinkedService[Any],
)


@dataclass(kw_only=True)
class SftpDataset(
    TabularDataset[
        SftpLinkedServiceType,
        SftpDatasetSettingsType,
        PandasSerializer,
        PandasDeserializer,
    ],
    Generic[SftpLinkedServiceType, SftpDatasetSettingsType],
):
    linked_service: SftpLinkedServiceType
    settings: SftpDatasetSettingsType

    serializer: PandasSerializer = field(default_factory=lambda: PandasSerializer(format=DatasetStorageFormatType.JSON))
    deserializer: PandasDeserializer = field(default_factory=lambda: PandasDeserializer(format=DatasetStorageFormatType.JSON))

    @property
    def type(self) -> ResourceType:
        return ResourceType.DATASET

    def read(self) -> None:
        """
        Read files from the SFTP server.

        Returns:
            None: The output is stored in the `output` attribute as a DataFrame containing
            the contents of the matched files.

        Raises:
            ReadError: If there is an error reading from the SFTP dataset.
        """
        try:
            files = self._get_files_by_pattern(
                path=self.settings.folder_path,
                fnmatch_pattern=self.settings.file_name,
            )

            if not files:
                logger.warning(
                    f"No files found matching pattern: {self.settings.file_name} in folder: {self.settings.folder_path}"
                )
                self.output = pd.DataFrame()
                return
            else:
                logger.info("Reading files from SFTP.")
                self.output = self._read_files_as_dataframe(files)

        except FileNotFoundError as exc:
            logger.error(
                f"Folder: {self.settings.folder_path} not found on SFTP server while looking for file: {self.settings.file_name}."
            )
            raise ReadError(
                message=f"Folder: {self.settings.folder_path} not found on SFTP server while looking for "
                f"file: {self.settings.file_name}.",
                status_code=404,
                details={
                    "folder_path": self.settings.folder_path,
                    "file_name": self.settings.file_name,
                    "settings": self.settings.serialize(),
                },
            ) from exc
        except Exception as exc:
            logger.error(f"Error reading from SFTP dataset: {exc}")
            raise ReadError(
                message=f"Error reading from SFTP dataset: {exc}",
                details={
                    "folder_path": self.settings.folder_path,
                    "file_name": self.settings.file_name,
                    "settings": self.settings.serialize(),
                },
            ) from exc

    def create(self) -> None:
        """
        Create data on the SFTP server.

        Note:
            This method is **not idempotent**. If called multiple times with the same parameters,
            it will raise a CreateError if the file already exists. If a network or server error
            occurs after the file is created but before the method returns, retrying may result
            in a CreateError due to the file's existence. Orchestration and retry policies should
            account for this non-idempotent behavior.

        Returns:
            None

        Raises:
            CreateError: If there is an error creating the dataset on the SFTP server, or if the file already exists.
        """
        remote_path = self._get_folder_and_file_path()
        try:
            if self.input is None or self.input.empty:
                logger.info("No input data provided.")
                return

            self._ensure_sftp_directory(remote_directory=self.settings.folder_path)
            self._ensure_file_does_not_exist(remote_path=remote_path)

            with self.linked_service.connection.client.open(filename=remote_path, mode="wb") as remote_file:
                logger.info(f"Creating file on SFTP server: {remote_path}")
                remote_file.write(self.serializer(self.input))
            self.output = self.input.copy()

        except OSError as exc:
            if exc.errno == errno.EEXIST:
                logger.error(f"File already exists at path: {remote_path} on SFTP server.")
                raise CreateError(
                    message=f"File already exists at path: {remote_path} on SFTP server.",
                    status_code=409,
                    details={
                        "folder_path": self.settings.folder_path,
                        "file_name": self.settings.file_name,
                        "settings": self.settings.serialize(),
                    },
                ) from exc
            if exc.errno in (errno.EACCES, errno.EPERM):
                logger.error(f"Permission denied while creating file on SFTP server: {exc}")
                raise CreateError(
                    message=f"Unauthorized to create file at path: {remote_path} on SFTP server.",
                    status_code=403,
                    details={
                        "folder_path": self.settings.folder_path,
                        "file_name": self.settings.file_name,
                        "settings": self.settings.serialize(),
                    },
                ) from exc

            logger.error(f"OS error occurred while creating file on SFTP server: {exc}")
            raise CreateError(
                message=f"OS error occurred while creating file on SFTP server: {exc}",
                details={
                    "folder_path": self.settings.folder_path,
                    "file_name": self.settings.file_name,
                    "settings": self.settings.serialize(),
                },
            ) from exc

        except Exception as exc:
            logger.error(f"Error creating file on SFTP server: {exc}")
            raise CreateError(
                message=f"Error creating file on SFTP server: {exc}",
                status_code=getattr(exc, "status_code", 500),
                details={
                    "folder_path": self.settings.folder_path,
                    "file_name": self.settings.file_name,
                    "settings": self.settings.serialize(),
                },
            ) from exc

    def update(self) -> None:
        """
        Update operation is not supported for in this provider.

        Returns:
            None

        Raises:
            NotSupportedError: Always raised since update is not supported for SftpDataset.
        """
        logger.error("Update operation is not supported by SftpDataset.")
        raise NotSupportedError(
            message="Method 'update' is not supported by SftpDataset.",
            details={"method": "update", "provider": self.type.value},
        )

    def upsert(self) -> None:
        """
        Upsert a file on the SFTP server. If the file already exists, it will be overwritten.

        Returns:
            None

        Raises:
            UpsertError: If there is an error upserting the dataset on the SFTP server.
        """
        try:
            if self.input is None or self.input.empty:
                logger.info("No input data provided.")
                return

            remote_path = self._get_folder_and_file_path()

            self._ensure_sftp_directory(remote_directory=self.settings.folder_path)

            with self.linked_service.connection.client.open(filename=remote_path, mode="wb") as remote_file:
                logger.info(f"Upserting file to SFTP server: {remote_path}")
                remote_file.write(self.serializer(self.input))
            self.output = self.input.copy()

        except Exception as exc:
            logger.error(f"Error upserting file to SFTP server: {exc}")
            raise UpsertError(
                message=f"Error upserting file to SFTP server: {exc}",
                details={
                    "folder_path": self.settings.folder_path,
                    "file_name": self.settings.file_name,
                    "settings": self.settings.serialize(),
                },
            ) from exc

    def delete(self) -> None:
        """
        Delete operation is not supported for in this provider.

        Returns:
            None

        Raises:
            NotSupportedError: Always raised since delete is not supported for SftpDataset.
        """
        logger.error("Delete operation is not supported by SftpDataset.")
        raise NotSupportedError(
            message="Method 'delete' is not supported by SftpDataset.",
            details={"method": "delete", "provider": self.type.value},
        )

    def purge(self) -> None:
        """
        Purge the dataset, deleting all files matching the pattern from the SFTP server.

        Returns:
            None

        Raises:
            PurgeError: If there is an error purging files from the SFTP server
        """
        try:
            files = self._get_files_by_pattern(
                path=self.settings.folder_path,
                fnmatch_pattern=self.settings.file_name,
            )
            if not files:
                logger.warning(
                    f"No files found for purging matching pattern: {self.settings.file_name} "
                    f"in folder: {self.settings.folder_path}"
                )
                return
            for file in files:
                file_path = f"{self.settings.folder_path}/{file.filename}"
                logger.info(f"Purging file from SFTP server: {file_path}")
                try:
                    self.linked_service.connection.client.remove(file_path)
                except FileNotFoundError:
                    logger.warning(f"File not found for purging: {file_path}. It may have already been deleted.")
                except Exception as exc:
                    logger.error(f"Error purging file from SFTP server: {exc}")
                    raise PurgeError(
                        message=f"Error purging file from SFTP server: {exc}",
                        details={
                            "folder_path": self.settings.folder_path,
                            "file_name": self.settings.file_name,
                            "settings": self.settings.serialize(),
                        },
                    ) from exc
        except Exception as exc:
            logger.error(f"Error purging files from SFTP server: {exc}")
            raise PurgeError(
                message=f"Error purging files from SFTP server: {exc}",
                details={
                    "folder_path": self.settings.folder_path,
                    "file_name": self.settings.file_name,
                    "settings": self.settings.serialize(),
                },
            ) from exc

    def list(self) -> None:
        """
        List the files in the directory on the SFTP server based on the specified
        pattern and settings.

        Returns:
            None: The output is stored in the `output` attribute as a DataFrame containing the file information.

        Raises:
            ListError: If there is an error listing the files in the SFTP dataset.
        """
        try:
            files = self._get_files_by_pattern(
                path=self.settings.folder_path,
                fnmatch_pattern=self.settings.file_name,
            )
            self.output = self._list_directory_files(files)

            if self.output.empty or "file_name" not in self.output.columns:
                return

            if self.settings.list.download:
                logger.info("Downloading file content as part of listing operation.")
                self.output["content"] = self.output["file_name"].apply(
                    lambda file_name: self.linked_service.connection.client.open(
                        f"{self.settings.folder_path}/{file_name}", mode="rb"
                    ).read()
                )
        except FileNotFoundError as exc:
            logger.error(f"Directory: {self.settings.folder_path} not found on SFTP server.")
            raise ListError(
                message=f"Directory: {self.settings.folder_path} not found on SFTP server.",
                status_code=404,
                details={
                    "folder_path": self.settings.folder_path,
                    "file_name": self.settings.file_name,
                    "settings": self.settings.list.serialize(),
                },
            ) from exc
        except Exception as exc:
            logger.error(f"Error listing files in SFTP dataset: {exc}")
            raise ListError(
                message=f"Error listing files in SFTP dataset: {exc}",
                details={
                    "folder_path": self.settings.folder_path,
                    "file_name": self.settings.file_name,
                    "settings": self.settings.list.serialize(),
                },
            ) from exc

    def rename(self) -> None:
        """
        Rename operation is not supported for in this provider.

        Returns:
            None

        Raises:
            NotSupportedError: Always raised since rename is not supported for SftpDataset.
        """
        logger.error("Rename operation is not supported by SftpDataset.")
        raise NotSupportedError(
            message="Method 'rename' is not supported by SftpDataset.",
            details={"method": "rename", "provider": self.type.value},
        )

    def close(self) -> None:
        """
        Close any open connections or resources.

        Returns:
            None
        """
        self.linked_service.close()

    def _get_folder_and_file_path(self) -> str:
        """
        Get combined path of folder_path and file_name, using forward slashes.
        This ensures consistent path formatting across Windows, Linux, and macOS.
        It also replaces any Windows-style backslashes with forward slashes.

        Returns:
            str: The full file path as a POSIX-style string.
        """
        folder_posix = PureWindowsPath(self.settings.folder_path).as_posix()
        return posixpath.join(folder_posix, self.settings.file_name)

    def _ensure_file_does_not_exist(self, remote_path: str) -> None:
        """
        Ensure the target file does not already exist on the SFTP server.

        Args:
            remote_path (str): Full target file path on the SFTP server.

        Raises:
            FileExistsError: If the target file already exists.
        """
        try:
            self.linked_service.connection.client.stat(remote_path)
            raise FileExistsError(
                message=f"File already exists at path: {remote_path} on SFTP server.",
                details={
                    "folder_path": self.settings.folder_path,
                    "file_name": self.settings.file_name,
                    "settings": self.settings.serialize(),
                },
            )
        except FileNotFoundError:
            return

    def _list_directory(self, path: str) -> builtins.list[SFTPAttributes]:
        """
        List the files in the specified directory on the SFTP server.

        Args:
            path (str): The directory path to list files from.
        Returns:
            list[SFTPAttributes]: A list of SFTPAttributes for the files in the directory.
        """
        logger.info(f"Listing files in SFTP directory: {path}")
        return self.linked_service.connection.client.listdir_attr(path)

    def _get_files_by_pattern(self, path: str, fnmatch_pattern: str) -> builtins.list[SFTPAttributes]:
        """
        Get files from the SFTP server that match the specified pattern.

        Args:
            path (str): The directory path to search for files.
            fnmatch_pattern (str): The pattern to match file names against.

        Returns:
            list[SFTPAttributes]: A list of SFTPAttributes for the matching files.
        """
        logger.info(f"Listing files in SFTP directory: {path} with pattern: {fnmatch_pattern}")
        matched_files = []
        for file in self._list_directory(path):
            if fnmatch.fnmatch(file.filename, fnmatch_pattern):
                matched_files.append(file)
        return matched_files

    def _ensure_sftp_directory(self, remote_directory: str, max_depth: int = 20) -> None:
        """
        Ensure that the specified directory exists on the SFTP server.
        If it does not exist, create it.

        Args:
            remote_directory (str): The directory path to ensure on the SFTP server.
            max_depth (int): The maximum directory depth to traverse when ensuring the directory exists. Default is 20.

        Returns:
            None

        Raises:
            CreateError: If the maximum directory depth is exceeded while ensuring the SFTP directory.
        """
        remote_directory = posixpath.normpath(remote_directory)
        if not remote_directory or remote_directory == "/":
            return

        directories: list[str] = []
        depth = 0
        current_directory = remote_directory
        while current_directory not in ("", "/") and depth < max_depth:
            directories.insert(0, current_directory)
            current_directory = posixpath.dirname(current_directory)
            depth += 1

        if depth >= max_depth:
            raise CreateError(
                message=f"Maximum directory depth of {max_depth} exceeded while ensuring SFTP directory: {remote_directory}",
                details={
                    "remote_directory": remote_directory,
                    "max_depth": max_depth,
                    "folder_path": self.settings.folder_path,
                    "file_name": self.settings.file_name,
                },
            )

        for directory in directories:
            try:
                self.linked_service.connection.client.stat(directory)
            except FileNotFoundError:
                self.linked_service.connection.client.mkdir(directory)

    def _read_files_as_dataframe(self, files: builtins.list[SFTPAttributes]) -> pd.DataFrame:
        """
        Read the dataset from the SFTP server as a dataframe.

        Args:
            files (list[SFTPAttributes]): List of SFTPAttributes for the files to read.

        Returns:
            pd.DataFrame: The combined data from the files as a single DataFrame.
        """
        dfs = []
        for file in files:
            file_path = f"{self.settings.folder_path}/{file.filename}"
            logger.info(f"Reading file from SFTP server: {file_path}")
            with self.linked_service.connection.client.open(file_path, "rb") as remote_file:
                df = self.deserializer(remote_file.read())
                dfs.append(df)

        if not dfs:
            logger.warning("No valid dataframes to concatenate. Returning empty dataframe.")
            return pd.DataFrame()
        else:
            logger.info(f"Successfully read {len(dfs)} files as dataframes. Concatenating into a single dataframe.")
            return pd.concat(dfs, ignore_index=True)

    def _list_directory_files(self, files: builtins.list[SFTPAttributes]) -> pd.DataFrame:
        """
        List the files in the directory as a dataframe.

        Args:
            files (list[SFTPAttributes]): List of SFTPAttributes for the files to list.

        Returns:
            pd.DataFrame: A dataframe containing the file information.
        """
        dfs = []
        for file in files:
            file_path = f"{self.settings.folder_path}/{file.filename}"
            logger.info(f"Listing file in SFTP directory: {file_path}")
            file_location = (
                f"sftp://{self.linked_service.settings.username}@"
                f"{self.linked_service.settings.host}:"
                f"{self.linked_service.settings.port}/"
                f"{file_path}"
            )
            content_type, _ = mimetypes.guess_type(file.filename)
            df = pd.DataFrame(
                {
                    "file_name": [file.filename],
                    "file_path": [f"{self.settings.folder_path}/{file.filename}"],
                    "file_uri": [file_location],
                    "content_type": [content_type or "application/octet-stream"],
                    "file_size": [file.st_size],
                    "user_id": [file.st_uid],
                    "group_id": [file.st_gid],
                    "file_permissions": [file.st_mode],
                    "last_accessed_time": [file.st_atime],
                    "last_modified_time": [file.st_mtime],
                }
            )
            dfs.append(df)
        if not dfs:
            return pd.DataFrame()
        return pd.concat(dfs, ignore_index=True)
