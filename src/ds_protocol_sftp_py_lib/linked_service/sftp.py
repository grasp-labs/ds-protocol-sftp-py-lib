"""
**File**: `sftp.py`
**Region**: `src/ds_protocol_sftp_py_lib/linked_service/sftp`

SFTP Linked Service implementation.

This module defines the `SftpLinkedService` class, which implements a linked service for SFTP connections,
including connection management, error handling, and integration with the SFTP client.

Example:
    >>> import uuid
    >>> from ds_protocol_sftp_py_lib.linked_service import SftpLinkedService, SftpLinkedServiceSettings
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

from dataclasses import dataclass, field
from typing import Generic, TypeVar

from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.linked_service import LinkedService, LinkedServiceSettings
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    ConnectionError,
)
from paramiko import SFTPClient

from ..enums import ResourceType
from ..utils.sftp.config import SftpConfig
from ..utils.sftp.provider import Sftp

logger = Logger.get_logger(__name__, package=True)


@dataclass(kw_only=True)
class SftpLinkedServiceSettings(LinkedServiceSettings):
    """Settings for SFTP Linked Service connections.

    Attributes:
        host (str): SFTP server hostname.
        username (str): Username for authentication.
        password (str | None): Password for authentication.
        encrypted_credential (str): Encrypted credential.
        private_key (str | None): Private key for authentication.
        passphrase (str | None): Passphrase for private key.
        timeout (float | None): Connection timeout in seconds.
        host_key_fingerprint (str | None): Expected host key fingerprint.
        host_key_validation (bool): Whether to validate host key.
        port (int): SFTP server port.
    """

    host: str
    """Hostname or IP address of the SFTP server."""

    username: str
    """Username for authentication."""

    password: str | None = None
    """Password for authentication."""

    encrypted_credential: str
    """Encrypted credential."""

    private_key: str | None = None
    """Private key for authentication."""

    passphrase: str | None = None
    """Passphrase for private key."""

    timeout: float | None = None
    """Connection timeout in seconds."""

    host_key_fingerprint: str | None = None
    """Expected host key fingerprint."""

    host_key_validation: bool = True
    """Whether to validate host key."""

    port: int = 22
    """SFTP server port."""


SftpLinkedServiceSettingsType = TypeVar("SftpLinkedServiceSettingsType", bound=SftpLinkedServiceSettings)


@dataclass(kw_only=True)
class SftpLinkedService(
    LinkedService[SftpLinkedServiceSettingsType],
    Generic[SftpLinkedServiceSettingsType],
):
    """SFTP Linked Service implementation.

    Attributes:
        settings (SftpLinkedServiceSettingsType): Linked service settings.
        _connection (SFTPClient | None): Underlying SFTP client connection.
        _sftp (Sftp | None): Sftp provider instance.
    """

    settings: SftpLinkedServiceSettingsType

    _connection: SFTPClient | None = field(default=None, init=False, repr=False, metadata={"serialize": False})
    _sftp: Sftp | None = field(default=None, init=False, repr=False, metadata={"serialize": False})

    @property
    def type(self) -> ResourceType:
        """Get the type of linked service.

        Returns:
            ResourceType: The type of the linked service.
        """
        return ResourceType.LINKED_SERVICE

    @property
    def connection(self) -> SFTPClient:
        """Get the SFTP client connection.

        Returns:
            SFTPClient: The active SFTP client connection.

        Raises:
            ConnectionError: If the connection is not initialized.
        """
        if self._connection is None:
            raise ConnectionError(
                message="Connection is not initialized",
                details={
                    "host": self.settings.host,
                    "username": self.settings.username,
                    "port": self.settings.port,
                    "type": self.type.value,
                },
            )
        return self._connection

    def _init_sftp(self) -> Sftp:
        """Initialize the Sftp client instance with SftpConfig.

        Returns:
            Sftp: An initialized Sftp provider instance.
        """
        config = SftpConfig(
            pkey=self.settings.private_key,
            host_key_validation=self.settings.host_key_validation,
            timeout=self.settings.timeout,
        )
        return Sftp(config=config)

    def connect(self) -> None:
        """Initialize the Sftp client instance if not already initialized.

        Raises:
            ConnectionError: If connection fails.
            AuthenticationError: If authentication fails.
        """
        if self._connection is not None:
            logger.info(f"Already connected to {self.settings.host}. Establishing a new connection.")
            self.close()

        if self._sftp is None:
            self._sftp = self._init_sftp()

        self._connection = self._sftp.connect(
            host=self.settings.host,
            port=self.settings.port,
            username=self.settings.username,
            password=self.settings.password,
            passphrase=self.settings.passphrase,
            host_key_fingerprint=self.settings.host_key_fingerprint,
        )

    def test_connection(self) -> tuple[bool, str]:
        """Perform a lightweight health check against the SFTP backend.

        Uses getcwd to verify connectivity without modifying data.

        Returns:
            tuple[bool, str]:
                - (True, message) if successful.
                - (False, error message) otherwise.
        """
        try:
            if self._sftp is None:
                self.connect()
            # Lightweight health check: get current working directory
            cwd = self.connection.getcwd()
            if cwd is not None:
                return True, "Connection successfully tested"
            else:
                return False, "Could not get current working directory"
        except Exception as exc:
            return False, str(exc)

    def close(self) -> None:
        """Close the linked service.

        Always set _sftp and _connection to None, even if exceptions are raised.

        Raises:
            ConnectionError: If closing the SFTP connection fails.
        """
        sftp_exc = None
        conn_exc = None
        if self._sftp:
            try:
                self._sftp.close()
            except Exception as exc:
                sftp_exc = exc
            finally:
                self._sftp = None

        if self._connection:
            try:
                self._connection.close()
            except Exception as exc:
                conn_exc = exc
            finally:
                self._connection = None

        if sftp_exc or conn_exc:
            raise ConnectionError(
                message="Failed to close SFTP connection",
                details={
                    "host": self.settings.host,
                    "username": self.settings.username,
                    "port": self.settings.port,
                    "type": self.type.value,
                },
            ) from sftp_exc or conn_exc
