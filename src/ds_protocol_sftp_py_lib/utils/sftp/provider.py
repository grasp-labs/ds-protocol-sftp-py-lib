import base64
import fnmatch
import io
import posixpath
from typing import Any

import paramiko
from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    AuthenticationError,
    ConnectionError,
)
from paramiko import SFTPAttributes, ssh_exception

from .config import SftpConfig

logger = Logger.get_logger(__name__, package=True)


class Sftp:
    """
    High-level wrapper around :class:`paramiko.SFTPClient` for interacting with an SFTP
    server using SSH.

    The class manages the underlying :class:`paramiko.SSHClient` and SFTP session, and
    provides convenience methods for connecting, listing directories, moving files and
    accessing the raw SSH/SFTP clients when needed.

    A :class:`SftpConfig` instance can be supplied to customize connection behavior
    (for example, host key policies). An existing :class:`paramiko.SFTPClient` can be
    injected for cases where the SSH/SFTP session is created externally.

    This class is also a context manager and can be used with ``with`` statements to
    automatically close the underlying SSH/SFTP connections.

    Basic usage::

        from ds_protocol_sftp_py_lib.utils.sftp.config import SftpConfig
        from ds_protocol_sftp_py_lib.utils.sftp.provider import Sftp

        config = SftpConfig()

        # Using explicit connect/close
        sftp = Sftp(config=config)
        client = sftp.connect(
            host="sftp.example.com",
            port=22,
            username="user",
            password="secret",
            passphrase=None,
            host_key_fingerprint=None,
        )
        files = sftp.list_directory("/remote/path")
        sftp.close()

        # Using as a context manager
        with Sftp(config=config) as sftp:
            client = sftp.connect(
                host="sftp.example.com",
                port=22,
                username="user",
                password="secret",
                passphrase=None,
                host_key_fingerprint=None,
            )
            files = sftp.list_directory("/remote/path")
    """

    def __init__(self, config: SftpConfig | None = None, client: paramiko.SFTPClient | None = None):
        self._config = config or SftpConfig()
        self._client: paramiko.SFTPClient | None = client
        self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(self._config.policy)

    def connect(
        self, host: str, port: int, username: str, password: str | None, passphrase: str | None, host_key_fingerprint: str | None
    ) -> paramiko.SFTPClient:
        """
        Establish and return an active SFTP client connection to the remote server.

        The connection may use password authentication, private key authentication (with optional passphrase),
        or a combination, depending on the configuration. If host key validation is enabled, the remote server's
        host key fingerprint is validated against the provided fingerprint.

        Args:
            host (str): Hostname or IP address of the SFTP server.
            port (int): Port number to connect to (typically 22).
            username (str): Username for authentication.
            password (str | None): Password for authentication, or None if using only key-based auth.
            passphrase (str | None): Passphrase for the private key, if required.
            host_key_fingerprint (str | None): Expected base64-encoded host key fingerprint for validation.
                Required if host key validation is enabled.

        Returns:
            paramiko.SFTPClient: An active SFTP client connection.

        Raises:
            AuthenticationError: If authentication fails or SSH transport is unavailable.
            ConnectionError: For network errors, host key validation failures, or other connection issues.
        """
        pkey = None
        if self._config.pkey:
            pkey = self._load_private_key(private_key=self._config.pkey, passphrase=passphrase)

        try:
            logger.info(f"Connecting to {host}")
            self._ssh.connect(
                hostname=host, port=port, username=username, password=password, pkey=pkey, timeout=self._config.timeout
            )
        except ssh_exception.AuthenticationException as exc:
            logger.error(f"Failed to authenticate to host: {host}: {exc}")
            raise AuthenticationError(
                message=f"Failed to authenticate towards {host}",
                details={
                    "host": host,
                    "username": username,
                    "port": port,
                },
            ) from exc
        except Exception as exc:
            # network errors, DNS, etc.
            raise ConnectionError(
                message=f"Failed to connect to {host}: {exc}",
                details={"host": host, "username": username, "port": port},
            ) from exc

        # Get servers host key.
        transport = self._ssh.get_transport()
        if not transport:
            self.close()
            raise AuthenticationError(
                message="SSH transport not available.",
                details={
                    "host": host,
                    "username": username,
                    "port": port,
                },
            )

        if self._config.host_key_validation:
            if host_key_fingerprint is None:
                self.close()
                raise ConnectionError(
                    message="Host key validation is enabled but no fingerprint was provided.",
                    details={
                        "host": host,
                        "username": username,
                        "port": port,
                    },
                )
            key = transport.get_remote_server_key()
            encoded_fingerprint = base64.b64encode(key.get_fingerprint()).decode("utf-8")

            # Verify the server's fingerprint.
            if encoded_fingerprint != host_key_fingerprint:
                self.close()
                raise ConnectionError(
                    message="Host key fingerprint validation failed.",
                    details={
                        "host": host,
                        "username": username,
                        "port": port,
                    },
                )

        self._client = self._ssh.open_sftp()
        return self._client

    # ------ Helper Functions ------

    def _load_private_key(self, private_key: str, passphrase: str | None) -> paramiko.PKey:
        """
        Load and return an RSA private key for SFTP authentication.

        Args:
            private_key (str): The private key in PEM format as a string.
            passphrase (str | None): Passphrase for the private key, if required.

        Returns:
            paramiko.PKey: The loaded RSA private key object.

        Raises:
            AuthenticationError: If the private key cannot be loaded (invalid format, wrong passphrase, etc).
        """
        key_file = io.StringIO(private_key)

        try:
            pkey = paramiko.RSAKey.from_private_key(file_obj=key_file, password=passphrase)
            logger.info("Successfully loaded RSA private key.")
            return pkey
        except Exception as exc:
            raise AuthenticationError(
                message=f"Unable to load RSA private key: {exc}. "
                "Please ensure you're providing a valid RSA private key "
                "in PEM format.",
            ) from exc

    def list_directory(self, path: str) -> list[SFTPAttributes]:
        """
        List files in a directory on the remote system.
        """
        logger.info(f"Listing directory on path {path}.")
        if self._client is None:
            raise ConnectionError(message="Not Connected to SFTP.", details={"path": path})
        files = sorted(self._client.listdir_attr(path), key=lambda x: x.filename)
        return files

    def get_files_by_pattern(self, path: str, fnmatch_pattern: str) -> list[SFTPAttributes]:
        """
        Get files from SFTP server matching a pattern.
        """
        logger.info(f"Getting files with pattern: {fnmatch_pattern} on path: {path}.")
        matched_files = []
        for file in self.list_directory(path):
            if fnmatch.fnmatch(file.filename, fnmatch_pattern):
                matched_files.append(file)

        return matched_files

    def move(self, old_path: str, new_path: str) -> None:
        """
        Rename/move a file on the remote SFTP server.
        Uses POSIX path handling for remote paths.
        """
        logger.info(f"Moving file from {old_path} to {new_path}.")
        if self._client is None:
            raise ConnectionError(message="Not Connected to SFTP.", details={"old_path": old_path, "new_path": new_path})
        directory, pattern = posixpath.split(old_path)
        matching_files = self.get_files_by_pattern(directory, pattern)
        for file in matching_files:
            source_path = posixpath.join(directory, file.filename)
            destination_path = posixpath.join(new_path, file.filename)
            self._client.rename(source_path, destination_path)
            logger.info(f"Moved {source_path} to {destination_path}")

    # ------ Properties ------

    @property
    def ssh(self) -> paramiko.SSHClient:
        """
        Get the underlying SSHClient for direct use.
        """
        return self._ssh

    @property
    def client(self) -> paramiko.SFTPClient:
        """
        Get the underlying SFTPClient for direct use.
        """
        if self._client is None:
            raise ConnectionError(
                message="Not Connected to SFTP.",
            )
        return self._client

    def __enter__(self) -> "Sftp":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
        if self._ssh is not None:
            self._ssh.close()
            self._ssh = paramiko.SSHClient()
