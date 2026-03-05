"""
**File**: provider.py
**Region**: ds_protocol_sftp_py_lib/utils/sftp/provider

SFTP Provider

This module implements the Sftp class, which is a high-level wrapper around
paramiko's SFTPClient for managing SFTP connections.

Example:
    >> with Sftp() as sftp:
    ...     client = sftp.connect(
    ...         host="sftp.example.com",
    ...         port=22,
    ...         username="user",
    ...         password="secret",
    ...         passphrase=None,
    ...         host_key_fingerprint=None,
    ...     )
"""

import base64
import io
from typing import Any

import paramiko
from ds_common_logger_py_lib import Logger
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import (
    AuthenticationError,
    ConnectionError,
)
from paramiko import AutoAddPolicy, MissingHostKeyPolicy, ssh_exception

logger = Logger.get_logger(__name__, package=True)


class Sftp:
    """
    High-level wrapper around :class:`paramiko.SFTPClient` for interacting with an SFTP
    server using SSH.

    The class manages the underlying :class:`paramiko.SSHClient` and SFTP session, and
    provides convenience methods for connecting, listing directories, moving files and
    accessing the raw SSH/SFTP clients when needed.

    An existing :class:`paramiko.SFTPClient` can be injected for cases where the
    SSH/SFTP session is created externally.

    This class is also a context manager and can be used with ``with`` statements to
    automatically close the underlying SSH/SFTP connections.

    Basic usage::

        from ds_protocol_sftp_py_lib.utils.sftp.provider import Sftp

        # Using explicit connect/close
        sftp = Sftp()
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
        with Sftp() as sftp:
            client = sftp.connect(
                host="sftp.example.com",
                port=22,
                username="user",
                password="secret",
                passphrase=None,
                host_key_fingerprint=None,
                pkey=None,
                host_key_validation=True,
                timeout=None,
                policy=None,
            )
            files = sftp.list_directory("/remote/path")
    """

    def __init__(self, client: paramiko.SFTPClient | None = None):
        self._client: paramiko.SFTPClient | None = client
        self._ssh = paramiko.SSHClient()

    def connect(
        self,
        host: str,
        port: int,
        username: str,
        password: str | None,
        passphrase: str | None,
        host_key_fingerprint: str | None,
        pkey: str | None = None,
        host_key_validation: bool = True,
        timeout: float | None = None,
        policy: MissingHostKeyPolicy | None = None,
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
            pkey (str | None): Private key in PEM format as a string, or None if not using key-based auth.
            host_key_validation (bool): Whether to perform host key validation against the provided fingerprint.
            timeout (float | None): Optional connection timeout in seconds.
            policy (MissingHostKeyPolicy | None): Optional Paramiko host key policy to use if host key validation is disabled.

        Returns:
            paramiko.SFTPClient: An active SFTP client connection.

        Raises:
            AuthenticationError: If authentication fails, host key validation fails, or SSH transport is unavailable.
            ConnectionError: For network errors or other connection issues.
        """
        pkey_obj = None
        if pkey:
            pkey_obj = self._load_private_key(private_key=pkey, passphrase=passphrase)

        if not host_key_validation:
            if policy is None:
                policy = AutoAddPolicy()
            self._ssh.set_missing_host_key_policy(policy)

        # Pre-load expected host key if host_key_validation is enabled
        if host_key_validation and host_key_fingerprint is None:
            self.close()
            raise ConnectionError(
                message="Host key validation is enabled but no fingerprint was provided.",
                details={
                    "host": host,
                    "username": username,
                    "port": port,
                },
            )

        try:
            logger.info(f"Connecting to {host}")
            self._ssh.connect(hostname=host, port=port, username=username, password=password, pkey=pkey_obj, timeout=timeout)
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

        # Get server's host key and validate fingerprint if required
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

        if host_key_validation:
            server_key = transport.get_remote_server_key()
            actual_fingerprint = base64.b64encode(server_key.get_fingerprint()).decode()
            if actual_fingerprint != host_key_fingerprint:
                self.close()
                raise AuthenticationError(
                    message="Host key fingerprint does not match.",
                    details={
                        "host": host,
                        "username": username,
                        "port": port,
                        "expected_fingerprint": host_key_fingerprint,
                        "actual_fingerprint": actual_fingerprint,
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
