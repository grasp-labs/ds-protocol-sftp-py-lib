from dataclasses import dataclass, field
from typing import Generic, TypeVar

from ds_resource_plugin_py_lib.common.resource.linked_service import LinkedService, LinkedServiceSettings
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    ConnectionError,
)

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

    _session: Sftp | None = field(default=None, init=False, repr=False, metadata={"serialize": False})
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
    def session(self) -> Sftp:
        """
        Get the session.
        Returns:
            Sftp: The session.
        """
        if self._session is None:
            raise ConnectionError(
                message="Session is not initialized",
                details={"type": self.type.value},
            )
        return self._session

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
        self._session = self._sftp

    def test_connection(self) -> tuple[bool, str]:
        """
        Test the connection to the SFTP Server.

        Returns:
            tuple[bool, str]: A tuple containing a boolean indicating success and a string message.
        """
        try:
            if self._sftp is None:
                self.connect()
            self.session.list_directory(".")
            return True, "Connection successfully tested"
        except Exception as exc:
            return False, str(exc)

    def close(self) -> None:
        """
        Close the linked service.
        """
        if self._sftp:
            self._sftp.close()
