from dataclasses import dataclass, field
from typing import Generic, TypeVar

from ds_resource_plugin_py_lib.common.resource.linked_service import LinkedService, LinkedServiceSettings
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    ConnectionError,
)
from paramiko import SFTPClient

from ..enums import ResourceType
from ..utils.sftp.config import SftpConfig
from ..utils.sftp.provider import Sftp


@dataclass(kw_only=True)
class SftpLinkedServiceSettings(LinkedServiceSettings):
    """
    Settings for SFTP Linked Service connections.
    """

    host: str
    username: str
    password: str | None = None
    encrypted_credential: str
    private_key: str | None = None
    passphrase: str | None = None
    timeout: float | None = None
    host_key_fingerprint: str | None = None
    host_key_validation: bool = True
    port: int = 22


SftpLinkedServiceSettingsType = TypeVar("SftpLinkedServiceSettingsType", bound=SftpLinkedServiceSettings)


@dataclass(kw_only=True)
class SftpLinkedService(
    LinkedService[SftpLinkedServiceSettingsType],
    Generic[SftpLinkedServiceSettingsType],
):
    """
    Docstring for SftpLinkedService
    """

    settings: SftpLinkedServiceSettingsType

    _connection: SFTPClient | None = field(default=None, init=False, repr=False, metadata={"serialize": False})
    _sftp: Sftp | None = field(default=None, init=False, repr=False, metadata={"serialize": False})

    @property
    def type(self) -> ResourceType:
        """
        Function for getting type of linked service.

        :param self: Description
        :return: Description
        :rtype: ResourceType
        """
        return ResourceType.LINKED_SERVICE

    @property
    def connection(self) -> SFTPClient:
        """
        Get the connection.
        Returns:
            SFTPClient: The connection.
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
        """
        Initialise the Sftp client instance with SftpConfig.

        Creates an Sftp instance with:
        - SftpConfig using settings from SftpLinkedServiceSettings.

        :param self: Description
        :return: Description
        :rtype: Sftp
        """
        config = SftpConfig(
            pkey=self.settings.private_key,
            host_key_validation=self.settings.host_key_validation,
            timeout=self.settings.timeout,
        )
        return Sftp(config=config)

    def connect(self) -> None:
        """
        Initializes the Sftp client instance if not already initialized.
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
        )
        self._connection = self._sftp._client

    def test_connection(self) -> tuple[bool, str]:
        """
        Perform a lightweight health check against the SFTP backend.
        Uses getcwd to verify connectivity without modifying data.
        Returns:
            tuple[bool, str]: (True, message) if successful, (False, error message) otherwise.
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
        """
        Close the linked service.
        Always set _sftp and _connection to None, even if exceptions are raised.
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
