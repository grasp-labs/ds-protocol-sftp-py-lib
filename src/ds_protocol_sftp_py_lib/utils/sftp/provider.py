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
import socket
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
    provides convenience methods for connecting and accessing the raw SSH/SFTP clients when needed.

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
        files = sftp.client.listdir("/remote/path")
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
            files = sftp.client.listdir("/remote/path")
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
        host key fingerprint is validated against the provided fingerprint before authentication.

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
        if self._client is not None:
            logger.warning(f"Already connected to the SFTP server with host: {host}.")
            return self._client

        pkey_obj = None
        if pkey:
            pkey_obj = self._load_private_key(private_key=pkey, passphrase=passphrase)

        if host_key_validation:
            if host_key_fingerprint is None:
                raise ConnectionError(
                    message="Host key validation is enabled but no fingerprint was provided.",
                    details={
                        "host": host,
                        "username": username,
                        "port": port,
                    },
                )
            # Secure flow: validate host key before authentication
            try:
                logger.info(f"Connecting to {host}")
                connected_client = self._connect_with_socket(
                    host=host,
                    port=port,
                    timeout=timeout,
                    username=username,
                    password=password,
                    pkey_obj=pkey_obj,
                    host_key_fingerprint=host_key_fingerprint,
                )
                if connected_client is None:
                    raise ConnectionError(
                        message="Failed to establish SFTP connection after host key validation.",
                        details={
                            "host": host,
                            "username": username,
                            "port": port,
                        },
                    )
                self._client = connected_client
                return self._client
            except Exception as exc:
                raise ConnectionError(
                    message=f"Failed to connect to {host} with host key validation: {exc}",
                    details={
                        "host": host,
                        "username": username,
                        "port": port,
                    },
                ) from exc
        else:
            # Legacy/less secure flow: use SSHClient and policy
            if policy is None:
                policy = AutoAddPolicy()
            self._ssh.set_missing_host_key_policy(policy)
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
                raise ConnectionError(
                    message=f"Failed to connect to {host}: {exc}",
                    details={"host": host, "username": username, "port": port},
                ) from exc
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

    def _connect_with_socket(
        self,
        host: str,
        port: int,
        timeout: float | None,
        username: str,
        password: str | None,
        pkey_obj: paramiko.PKey | None,
        host_key_fingerprint: str,
    ) -> paramiko.SFTPClient | None:
        """
        Establish a socket connection to the SFTP server and return a Paramiko SFTP client.

        This method is used for the secure flow where host key validation is performed before authentication.

        Args:
            host (str): Hostname or IP address of the SFTP server.
            port (int): Port number to connect to.
            timeout (float | None): Optional connection timeout in seconds.
            username (str): Username for authentication.
            password (str | None): Password for authentication, if applicable.
            pkey_obj (paramiko.PKey | None): Private key object for authentication, if applicable.
            host_key_fingerprint (str): Expected host key fingerprint for validation.

        Returns:
            paramiko.SFTPClient: The SFTP client stored in the instance variable self._client.

        Raises:
            ConnectionError: If the socket connection cannot be established.
        """
        sock = None
        transport = None
        try:
            sock = socket.create_connection((host, port), timeout=timeout)
            transport = paramiko.Transport(sock)
            transport.start_client(timeout=timeout)
            server_key = transport.get_remote_server_key()
            actual_fingerprint = base64.b64encode(server_key.get_fingerprint()).decode()
            if actual_fingerprint != host_key_fingerprint:
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
            # Authenticate after fingerprint validation
            try:
                if pkey_obj:
                    transport.auth_publickey(username, pkey_obj)
                elif password:
                    transport.auth_password(username, password)
                else:
                    raise AuthenticationError(
                        message="No authentication method provided. Please provide either a password or a private key.",
                        details={
                            "host": host,
                            "username": username,
                            "port": port,
                        },
                    )
            except ssh_exception.AuthenticationException as exc:
                raise AuthenticationError(
                    message=f"Failed to authenticate towards {host}",
                    details={
                        "host": host,
                        "username": username,
                        "port": port,
                    },
                ) from exc
            return paramiko.SFTPClient.from_transport(transport)
        except Exception:
            if transport is not None:
                transport.close()
            if sock is not None:
                sock.close()
            raise

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
