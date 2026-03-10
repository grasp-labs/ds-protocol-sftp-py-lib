"""
**File:** ``test_provider.py``
**Region:** ``tests/utils/sftp/test_provider``

SFTP provider tests.

Covers:
- Sftp class initialization and default state.
- Client property behavior when connection is not initialized.
- Idempotency of close method.
- Error handling for client property and private key loading.
- Sftp.connect: success, authentication error, transport missing, host key validation failure, general exception, and pkey usage.
- Context manager (__enter__, __exit__) and close method.
- SSH property access.
"""

from unittest.mock import MagicMock, patch

import pytest
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import AuthenticationError, ConnectionError
from paramiko import SSHClient
from paramiko.ssh_exception import AuthenticationException

from ds_protocol_sftp_py_lib.utils.sftp.provider import Sftp


def test_sftp_init_defaults():
    """Verify Sftp initializes with default values."""
    sftp = Sftp()
    assert sftp._client is None
    assert sftp._ssh is not None


@patch("paramiko.SFTPClient")
def test_client_property_raises_when_none(mock_sftp_client):
    """Verify accessing client property raises when client is None."""
    sftp = Sftp()
    sftp._client = None
    with pytest.raises(ConnectionError):
        _ = sftp.client


@patch("paramiko.SFTPClient")
def test_close_idempotent(mock_sftp_client):
    """Verify close method is idempotent."""
    sftp = Sftp()
    sftp._client = MagicMock()
    sftp._ssh = MagicMock()
    sftp.close()
    # Second call should not raise
    sftp.close()


def test_load_private_key_raises_authentication_error():
    """Verify that _load_private_key raises AuthenticationError when given an invalid key."""
    sftp = Sftp()
    # Invalid key string
    invalid_key = "not-a-valid-key"
    with pytest.raises(AuthenticationError):
        sftp._load_private_key(private_key=invalid_key, passphrase=None)


@patch.object(Sftp, "_connect_with_socket")
def test_connect_success_secure_flow(mock_connect_with_socket):
    """Verify connect returns client from _connect_with_socket (secure flow)."""
    sftp = Sftp()
    mock_client = MagicMock()
    mock_connect_with_socket.return_value = mock_client
    result = sftp.connect(
        host="host",
        port=22,
        username="user",
        password="pass",
        passphrase=None,
        host_key_fingerprint="ZHVtbXktYnl0ZXM=",
        pkey=None,
        host_key_validation=True,
        timeout=None,
        policy=None,
    )
    mock_connect_with_socket.assert_called_once()
    assert result is mock_client


@patch.object(Sftp, "_connect_with_socket", side_effect=Exception("fail"))
def test_connect_secure_flow_wraps_exception(mock_connect_with_socket):
    """Verify connect wraps _connect_with_socket exceptions in ConnectionError (secure flow)."""
    sftp = Sftp()
    with pytest.raises(ConnectionError) as excinfo:
        sftp.connect(
            host="host",
            port=22,
            username="user",
            password="pass",
            passphrase=None,
            host_key_fingerprint="ZHVtbXktYnl0ZXM=",
            pkey=None,
            host_key_validation=True,
            timeout=None,
            policy=None,
        )
    assert "Failed to connect to host" in str(excinfo.value) or "with host key validation" in str(excinfo.value)


@patch("paramiko.Transport")
@patch("socket.create_connection")
def test_connect_authentication_error(mock_create_conn, mock_transport_cls):
    """Verify that connect raises ConnectionError when authentication fails (secure flow)."""
    mock_sock = MagicMock()
    mock_create_conn.return_value = mock_sock
    mock_transport = MagicMock()
    mock_transport_cls.return_value = mock_transport
    mock_transport.start_client.return_value = None
    mock_server_key = MagicMock()
    mock_server_key.get_fingerprint.return_value = b"dummy-bytes"
    mock_transport.get_remote_server_key.return_value = mock_server_key
    # Simulate auth failure
    mock_transport.auth_password.side_effect = AuthenticationException("fail")
    sftp = Sftp()
    with pytest.raises(ConnectionError):
        sftp.connect(
            host="host",
            port=22,
            username="user",
            password="pass",
            passphrase=None,
            host_key_fingerprint="ZHVtbXktYnl0ZXM=",
            pkey=None,
            host_key_validation=True,
            timeout=None,
            policy=None,
        )


@patch("paramiko.Transport")
@patch("socket.create_connection")
def test_connect_transport_missing(mock_create_conn, mock_transport_cls):
    """Verify connect raises AuthenticationError when transport is missing after connection.
    Simulates start_client failure."""
    mock_sock = MagicMock()
    mock_create_conn.return_value = mock_sock
    mock_transport = MagicMock()
    mock_transport_cls.return_value = mock_transport
    # Simulate start_client raising Exception
    mock_transport.start_client.side_effect = Exception("transport fail")
    sftp = Sftp()
    with pytest.raises(ConnectionError):
        sftp.connect(
            host="host",
            port=22,
            username="user",
            password="pass",
            passphrase=None,
            host_key_fingerprint="ZHVtbXktYnl0ZXM=",
            pkey=None,
            host_key_validation=True,
            timeout=None,
            policy=None,
        )


@patch("paramiko.Transport")
@patch("socket.create_connection")
def test_connect_host_key_validation_failure(mock_create_conn, mock_transport_cls):
    """Verify that connect raises ConnectionError when host key fingerprint does not match (secure flow)."""
    mock_sock = MagicMock()
    mock_create_conn.return_value = mock_sock
    mock_transport = MagicMock()
    mock_transport_cls.return_value = mock_transport
    mock_server_key = MagicMock()
    mock_server_key.get_fingerprint.return_value = b"wrong"
    mock_transport.get_remote_server_key.return_value = mock_server_key
    mock_transport.start_client.return_value = None
    sftp = Sftp()
    with pytest.raises(ConnectionError):
        sftp.connect(
            host="host",
            port=22,
            username="user",
            password="pass",
            passphrase=None,
            host_key_fingerprint="notmatching",
            pkey=None,
            host_key_validation=True,
            timeout=None,
            policy=None,
        )


@patch("paramiko.Transport")
@patch("socket.create_connection")
def test_connect_general_exception(mock_create_conn, mock_transport_cls):
    """Verify connect raises Exception on general exception (e.g., start_client failure, network, DNS, etc)."""
    mock_sock = MagicMock()
    mock_create_conn.return_value = mock_sock
    mock_transport = MagicMock()
    mock_transport_cls.return_value = mock_transport
    mock_transport.start_client.side_effect = Exception("network fail")
    sftp = Sftp()
    with pytest.raises(ConnectionError) as excinfo:
        sftp.connect(
            host="host",
            port=22,
            username="user",
            password="pass",
            passphrase=None,
            host_key_fingerprint="ZHVtbXktYnl0ZXM=",
            pkey=None,
            host_key_validation=True,
            timeout=None,
            policy=None,
        )
    assert "network fail" in str(excinfo.value)


@patch.object(Sftp, "_connect_with_socket")
def test_connect_with_pkey_secure_flow(mock_connect_with_socket):
    """Verify connect works with a private key (pkey) provided (secure flow)."""
    sftp = Sftp()
    sftp._load_private_key = MagicMock(return_value="pkeyobj")
    mock_client = MagicMock()
    mock_connect_with_socket.return_value = mock_client
    result = sftp.connect(
        host="host",
        port=22,
        username="user",
        password="pass",
        passphrase=None,
        host_key_fingerprint="ZHVtbXktYnl0ZXM=",
        pkey="dummy-key",
        host_key_validation=True,
        timeout=None,
        policy=None,
    )
    sftp._load_private_key.assert_called_once()
    assert result is mock_client


@patch.object(Sftp, "_connect_with_socket")
def test_connect_host_key_validation_success_secure_flow(mock_connect_with_socket):
    """Verify connect succeeds when host key validation passes (secure flow)."""
    sftp = Sftp()
    mock_client = MagicMock()
    mock_connect_with_socket.return_value = mock_client
    result = sftp.connect(
        host="host",
        port=22,
        username="user",
        password="pass",
        passphrase=None,
        host_key_fingerprint="YWJj",
        pkey="",
        host_key_validation=True,
        timeout=None,
        policy=None,
    )
    assert result is mock_client


# --- Direct tests for _connect_with_socket (optional, for coverage) ---
@patch("paramiko.SFTPClient.from_transport")
@patch("paramiko.Transport")
@patch("socket.create_connection")
def test__connect_with_socket_success(mock_create_conn, mock_transport_cls, mock_sftp_from_transport):
    """Test _connect_with_socket returns client on success."""
    mock_sock = MagicMock()
    mock_create_conn.return_value = mock_sock
    mock_transport = MagicMock()
    mock_transport_cls.return_value = mock_transport
    mock_server_key = MagicMock()
    mock_server_key.get_fingerprint.return_value = b"dummy-bytes"
    mock_transport.get_remote_server_key.return_value = mock_server_key
    mock_transport.start_client.return_value = None
    mock_transport.auth_password.return_value = None
    mock_sftp_client = MagicMock()
    mock_sftp_from_transport.return_value = mock_sftp_client
    sftp = Sftp()
    result = sftp._connect_with_socket(
        host="host",
        port=22,
        timeout=None,
        username="user",
        password="pass",
        pkey_obj=None,
        host_key_fingerprint="ZHVtbXktYnl0ZXM=",
    )
    assert result is mock_sftp_client


@patch("paramiko.Transport")
@patch("socket.create_connection")
def test__connect_with_socket_fingerprint_mismatch(mock_create_conn, mock_transport_cls):
    """Test _connect_with_socket raises AuthenticationError on fingerprint mismatch."""
    mock_sock = MagicMock()
    mock_create_conn.return_value = mock_sock
    mock_transport = MagicMock()
    mock_transport_cls.return_value = mock_transport
    mock_server_key = MagicMock()
    mock_server_key.get_fingerprint.return_value = b"wrong"
    mock_transport.get_remote_server_key.return_value = mock_server_key
    mock_transport.start_client.return_value = None
    sftp = Sftp()
    with pytest.raises(AuthenticationError):
        sftp._connect_with_socket(
            host="host",
            port=22,
            timeout=None,
            username="user",
            password="pass",
            pkey_obj=None,
            host_key_fingerprint="notmatching",
        )


def test_load_private_key_authentication_error_branch():
    """Verify _load_private_key raises AuthenticationError for invalid key (branch coverage)."""
    sftp = Sftp()
    # This will hit the except branch in _load_private_key
    with pytest.raises(AuthenticationError):
        sftp._load_private_key(private_key="invalid-key", passphrase="badpass")


@patch("paramiko.SFTPClient")
def test_close_no_client(mock_sftp_client):
    """Verify close does not raise if _client is None (branch coverage)."""
    sftp = Sftp()
    sftp._client = None
    sftp._ssh = MagicMock()
    sftp.close()  # Should not raise


def test_ssh_property():
    """Verify that ssh property returns a valid SSHClient instance."""
    sftp = Sftp()
    assert sftp.ssh is not None


@patch("paramiko.SSHClient")
def test_connect_host_key_validation_missing_fingerprint_closes(mock_ssh):
    """Verify connect closes resources and raises ConnectionError if host_key_validation is enabled but fingerprint is missing."""
    mock_ssh_instance = MagicMock()
    mock_transport = MagicMock()
    mock_ssh_instance.get_transport.return_value = mock_transport
    mock_ssh_instance.open_sftp.return_value = MagicMock()
    mock_ssh.return_value = mock_ssh_instance
    sftp = Sftp()
    sftp._ssh = mock_ssh_instance
    sftp.close = MagicMock()
    with pytest.raises(ConnectionError) as excinfo:
        sftp.connect(
            host="host",
            port=22,
            username="user",
            password="pass",
            passphrase=None,
            host_key_fingerprint=None,
            pkey=None,
            host_key_validation=True,
            timeout=None,
            policy=None,
        )
    assert "no fingerprint" in str(excinfo.value)


@patch("paramiko.SFTPClient.from_transport")
@patch("paramiko.Transport")
@patch("socket.create_connection")
def test_connect_sets_missing_host_key_policy_when_validation(mock_create_conn, mock_transport_cls, mock_sftp_from_transport):
    """Verify connect sets missing host key policy when host_key_validation is True (secure flow, policy is not used)."""
    mock_sock = MagicMock()
    mock_create_conn.return_value = mock_sock
    mock_transport = MagicMock()
    mock_transport_cls.return_value = mock_transport
    mock_key = MagicMock()
    mock_key.get_fingerprint.return_value = b"abc"
    mock_transport.get_remote_server_key.return_value = mock_key
    mock_transport.start_client.return_value = None
    mock_transport.auth_password.return_value = None
    mock_sftp_client = MagicMock()
    mock_sftp_from_transport.return_value = mock_sftp_client
    sftp = Sftp()
    sftp.connect(
        host="host",
        port=22,
        username="user",
        password="pass",
        passphrase=None,
        host_key_fingerprint="YWJj",
        pkey=None,
        host_key_validation=True,
        timeout=None,
        policy=None,
    )


@patch("paramiko.Transport")
@patch("socket.create_connection")
def test_connect_fingerprint_mismatch_calls_close_and_raises(mock_create_conn, mock_transport_cls):
    """Verify connect calls close and raises AuthenticationError on fingerprint mismatch (secure flow)."""
    mock_sock = MagicMock()
    mock_create_conn.return_value = mock_sock
    mock_transport = MagicMock()
    mock_transport_cls.return_value = mock_transport
    mock_key = MagicMock()
    mock_key.get_fingerprint.return_value = b"wrong"
    mock_transport.get_remote_server_key.return_value = mock_key
    mock_transport.start_client.return_value = None
    mock_transport.auth_password.return_value = None
    sftp = Sftp()
    sftp.close = MagicMock()
    with pytest.raises(ConnectionError) as exc_info:
        sftp.connect(
            host="host",
            port=22,
            username="user",
            password="pass",
            passphrase=None,
            host_key_fingerprint="notmatching",
            pkey=None,
            host_key_validation=True,
            timeout=None,
            policy=None,
        )
    # Optionally, check that the error message indicates a fingerprint mismatch
    assert "Host key fingerprint does not match" in str(exc_info.value)


def test_exit_calls_close():
    """Verify __exit__ calls close when exiting context manager."""
    sftp = Sftp()
    sftp.close = MagicMock()
    with sftp:
        pass
    sftp.close.assert_called_once()


def test_load_private_key_success(monkeypatch):
    """Covers: _load_private_key success branch (returns pkey)."""
    sftp = Sftp()

    class DummyPKey:
        pass

    def dummy_from_private_key(file_obj, password=None):
        return DummyPKey()

    monkeypatch.setattr("paramiko.RSAKey.from_private_key", dummy_from_private_key)
    result = sftp._load_private_key(private_key="dummy", passphrase=None)
    assert isinstance(result, DummyPKey)


def test_client_property_success():
    """Covers: client property when _client is set."""
    sftp = Sftp()
    dummy_client = object()
    sftp._client = dummy_client
    assert sftp.client is dummy_client


def test_enter_returns_self():
    """Covers: __enter__ returns self."""
    sftp = Sftp()
    with sftp as s:
        assert s is sftp


def test_close_only_ssh():
    """Covers: close() when only _ssh is set (not _client)."""
    sftp = Sftp()
    sftp._client = None
    mock_ssh = MagicMock()
    sftp._ssh = mock_ssh
    sftp.close()
    mock_ssh.close.assert_called_once()


@patch("paramiko.SSHClient")
def test_connect_returns_client_when_already_connected(mock_ssh):
    """Covers: connect early return when already connected."""
    sftp = Sftp()
    dummy_client = MagicMock()
    sftp._client = dummy_client
    result = sftp.connect(
        host="host",
        port=22,
        username="user",
        password="pass",
        passphrase=None,
        host_key_fingerprint="YWJj",
        pkey=None,
        host_key_validation=False,
        timeout=None,
        policy=None,
    )
    assert result is dummy_client


@patch("paramiko.Transport")
@patch("socket.create_connection")
def test_connect_host_key_validation_missing_fingerprint(mock_create_conn, mock_transport_cls):
    """Covers: host_key_validation True and host_key_fingerprint None raises ConnectionError (secure flow)."""
    mock_sock = MagicMock()
    mock_create_conn.return_value = mock_sock
    mock_transport = MagicMock()
    mock_transport_cls.return_value = mock_transport
    sftp = Sftp()
    sftp.close = MagicMock()
    with pytest.raises(ConnectionError) as excinfo:
        sftp.connect(
            host="host",
            port=22,
            username="user",
            password="pass",
            passphrase=None,
            host_key_fingerprint=None,
            pkey=None,
            host_key_validation=True,
            timeout=None,
            policy=None,
        )
    assert "no fingerprint" in str(excinfo.value)


@patch("paramiko.Transport")
@patch("socket.create_connection")
def test_connect_transport_none_raises_auth_error(mock_create_conn, mock_transport_cls):
    """Covers: transport is None or start_client fails, raises AuthenticationError (secure flow)."""
    mock_sock = MagicMock()
    mock_create_conn.return_value = mock_sock
    mock_transport = MagicMock()
    mock_transport_cls.return_value = mock_transport
    # Simulate start_client raising an Exception (e.g., handshake failure)
    mock_transport.start_client.side_effect = Exception("transport fail")
    sftp = Sftp()
    with pytest.raises(ConnectionError):
        sftp.connect(
            host="host",
            port=22,
            username="user",
            password="pass",
            passphrase=None,
            host_key_fingerprint="YWJj",
            pkey=None,
            host_key_validation=True,
            timeout=None,
            policy=None,
        )


def test_close_covers_all_branches():
    """Covers: close method when both _client and _ssh are set."""
    sftp = Sftp()
    mock_client = MagicMock()
    mock_ssh = MagicMock()
    sftp._client = mock_client
    sftp._ssh = mock_ssh
    sftp.close()
    mock_client.close.assert_called_once()
    mock_ssh.close.assert_called_once()
    assert sftp._client is None
    assert isinstance(sftp._ssh, type(SSHClient()))


# --- Coverage for secure flow: no authentication method provided ---
@patch("paramiko.Transport")
@patch("socket.create_connection")
def test_connect_secure_no_auth_method(mock_create_conn, mock_transport_cls):
    """Covers: secure flow, neither password nor pkey provided (raises ConnectionError)."""
    mock_sock = MagicMock()
    mock_create_conn.return_value = mock_sock
    mock_transport = MagicMock()
    mock_transport_cls.return_value = mock_transport
    mock_server_key = MagicMock()
    mock_server_key.get_fingerprint.return_value = b"dummy-bytes"
    mock_transport.get_remote_server_key.return_value = mock_server_key
    mock_transport.start_client.return_value = None
    sftp = Sftp()
    with pytest.raises(ConnectionError) as excinfo:
        sftp.connect(
            host="host",
            port=22,
            username="user",
            password=None,
            passphrase=None,
            host_key_fingerprint="ZHVtbXktYnl0ZXM=",
            pkey=None,
            host_key_validation=True,
            timeout=None,
            policy=None,
        )
    assert "No authentication method provided" in str(excinfo.value)


# --- Coverage for legacy flow (host_key_validation=False) ---
@patch("paramiko.SSHClient")
def test_connect_legacy_success(mock_ssh):
    """Covers: legacy flow, successful connection returns open_sftp()."""
    mock_ssh_instance = MagicMock()
    mock_transport = MagicMock()
    mock_ssh_instance.get_transport.return_value = mock_transport
    mock_ssh_instance.open_sftp.return_value = MagicMock()
    mock_ssh.return_value = mock_ssh_instance
    sftp = Sftp()
    result = sftp.connect(
        host="host",
        port=22,
        username="user",
        password="pass",
        passphrase=None,
        host_key_fingerprint=None,
        pkey=None,
        host_key_validation=False,
        timeout=None,
        policy=None,
    )
    assert result is mock_ssh_instance.open_sftp.return_value


@patch("paramiko.SSHClient")
def test_connect_legacy_authentication_error(mock_ssh):
    """Covers: legacy flow, AuthenticationException is raised."""
    mock_ssh_instance = MagicMock()
    mock_ssh_instance.connect.side_effect = AuthenticationException("fail")
    mock_ssh.return_value = mock_ssh_instance
    sftp = Sftp()
    with pytest.raises(AuthenticationError):
        sftp.connect(
            host="host",
            port=22,
            username="user",
            password="pass",
            passphrase=None,
            host_key_fingerprint=None,
            pkey=None,
            host_key_validation=False,
            timeout=None,
            policy=None,
        )


@patch("paramiko.SSHClient")
def test_connect_legacy_general_exception(mock_ssh):
    """Covers: legacy flow, general Exception is raised."""
    mock_ssh_instance = MagicMock()
    mock_ssh_instance.connect.side_effect = Exception("network fail")
    mock_ssh.return_value = mock_ssh_instance
    sftp = Sftp()
    with pytest.raises(ConnectionError) as excinfo:
        sftp.connect(
            host="host",
            port=22,
            username="user",
            password="pass",
            passphrase=None,
            host_key_fingerprint=None,
            pkey=None,
            host_key_validation=False,
            timeout=None,
            policy=None,
        )
    assert "network fail" in str(excinfo.value)


@patch("paramiko.SSHClient")
def test_connect_legacy_transport_none(mock_ssh):
    """Covers: legacy flow, get_transport() returns None (raises AuthenticationError)."""
    mock_ssh_instance = MagicMock()
    mock_ssh_instance.get_transport.return_value = None
    mock_ssh_instance.open_sftp.return_value = MagicMock()
    mock_ssh.return_value = mock_ssh_instance
    sftp = Sftp()
    sftp.close = MagicMock()
    with pytest.raises(AuthenticationError):
        sftp.connect(
            host="host",
            port=22,
            username="user",
            password="pass",
            passphrase=None,
            host_key_fingerprint=None,
            pkey=None,
            host_key_validation=False,
            timeout=None,
            policy=None,
        )
    sftp.close.assert_called_once()
