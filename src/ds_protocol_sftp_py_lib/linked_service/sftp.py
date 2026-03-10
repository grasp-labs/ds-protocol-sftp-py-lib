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
    ...         private_key=None,
    ...         passphrase=None,
    ...         timeout=30.0,
    ...         host_key_fingerprint="AbCdEfGhIjKlMnOpQrStUvWxYz0123456789abcdEf==",
    ...         host_key_validation=True,
    ...         port=22,
    ...     ),
    ... )
    >>> linked_service.connect()
"""

from dataclasses import dataclass, field
from typing import Generic, TypeVar

from ds_resource_plugin_py_lib.common.resource.linked_service import LinkedService, LinkedServiceSettings
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    ConnectionError,
)
from paramiko import MissingHostKeyPolicy

from ..enums import ResourceType
from ..utils.sftp.provider import Sftp


@dataclass(kw_only=True)
class SftpLinkedServiceSettings(LinkedServiceSettings):
    """Settings for SFTP Linked Service connections.

    Attributes:
        host (str): SFTP server hostname.
        username (str): Username for authentication.
        password (str | None): Password for authentication.
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

    password: str | None = field(default=None, metadata={"mask": True})
    """Password for authentication."""

    private_key: str | None = field(default=None, metadata={"mask": True})
    """Private key for authentication."""

    passphrase: str | None = field(default=None, metadata={"mask": True})
    """Passphrase for private key."""

    timeout: float | None = None
    """Connection timeout in seconds."""

    host_key_fingerprint: str | None = None
    """Expected host key fingerprint (base64-encoded MD5, as produced by
    Paramiko's get_fingerprint(); e.g., 'AbCdEfGhIjKlMnOpQrStUvWxYz0123456789abcdEf==')."""

    host_key_validation: bool = True
    """Whether to validate host key."""

    port: int = 22
    """SFTP server port."""

    policy: MissingHostKeyPolicy | None = None
    """Host key policy to use if host key validation is disabled."""


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

    _sftp: Sftp | None = field(default=None, init=False, repr=False, metadata={"serialize": False})

    @property
    def type(self) -> ResourceType:
        """Get the type of linked service.

        Returns:
            ResourceType: The type of the linked service.
        """
        return ResourceType.LINKED_SERVICE

    @property
    def connection(self) -> Sftp:
        """Get the SFTP client connection.

        Returns:
            Sftp: The active SFTP client connection.

        Raises:
            ConnectionError: If the connection is not initialized.
        """
        if self._sftp is None:
            raise ConnectionError(
                message="Connection is not initialized",
                details={
                    "host": self.settings.host,
                    "username": self.settings.username,
                    "port": self.settings.port,
                    "type": self.type.value,
                },
            )
        return self._sftp

    def _init_sftp(self) -> Sftp:
        """Initialize the Sftp client.

        Returns:
            Sftp: An initialized Sftp provider instance.
        """
        return Sftp()

    def connect(self) -> None:
        """Initialize the Sftp client instance if not already initialized.

        Raises:
            ConnectionError: If connection fails.
            AuthenticationError: If authentication fails.
        """
        if self._sftp is None:
            self._sftp = self._init_sftp()

        self._sftp.connect(
            host=self.settings.host,
            port=self.settings.port,
            username=self.settings.username,
            password=self.settings.password,
            passphrase=self.settings.passphrase,
            host_key_fingerprint=self.settings.host_key_fingerprint,
            pkey=self.settings.private_key,
            host_key_validation=self.settings.host_key_validation,
            timeout=self.settings.timeout,
            policy=self.settings.policy,
        )

    def test_connection(self) -> tuple[bool, str]:
        """Perform a lightweight health check against the SFTP backend.

        Uses the SFTP client's listdir method to check connectivity and authentication.

        Returns:
            tuple[bool, str]:
                - (True, message) if successful.
                - (False, error message) otherwise.
        """
        try:
            self.connect()
            if self._sftp is None:
                return False, "SFTP connection is not initialized after connect()"
            directory = self._sftp.client.listdir(".")
            if directory is not None:
                return True, "Connection successfully tested"
        except Exception as exc:
            return False, f"Failed to connect to SFTP server, error: {exc}"

    def close(self) -> None:
        """Close the linked service.

        Sets the _sftp attribute to None to indicate the connection is closed.

        Raises:
            ConnectionError: If closing the SFTP connection fails.
        """
        if self._sftp:
            self._sftp.close()
            self._sftp = None
