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


@patch("paramiko.SSHClient")
def test_connect_success(mock_ssh):
    """Verify that connect successfully establishes a connection and sets the client."""
    mock_ssh_instance = MagicMock()
    mock_transport = MagicMock()
    mock_server_key = MagicMock()
    mock_server_key.get_fingerprint.return_value = b"dummy-bytes"
    mock_transport.get_remote_server_key.return_value = mock_server_key
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
        host_key_fingerprint="ZHVtbXktYnl0ZXM=",
        pkey=None,
        host_key_validation=True,
        timeout=None,
        policy=None,
    )
    assert result is not None


@patch("paramiko.SSHClient")
def test_connect_authentication_error(mock_ssh):
    """Verify that connect raises AuthenticationError when SSH connection fails."""
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
            host_key_fingerprint="ZHVtbXktYnl0ZXM=",
            pkey=None,
            host_key_validation=True,
            timeout=None,
            policy=None,
        )


@patch("paramiko.SSHClient")
def test_connect_transport_missing(mock_ssh):
    """Verify that connect raises AuthenticationError when transport is missing after connection."""
    mock_ssh_instance = MagicMock()
    mock_ssh_instance.get_transport.return_value = None
    mock_ssh_instance.open_sftp.return_value = MagicMock()
    mock_ssh.return_value = mock_ssh_instance
    sftp = Sftp()
    with pytest.raises(AuthenticationError):
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


@patch("paramiko.SSHClient")
def test_connect_host_key_validation_failure(mock_ssh):
    """Verify that connect raises AuthenticationError when host key fingerprint does not match."""
    mock_ssh_instance = MagicMock()
    mock_transport = MagicMock()
    mock_key = MagicMock()
    mock_key.get_fingerprint.return_value = b"wrong"
    mock_transport.get_remote_server_key.return_value = mock_key
    mock_ssh_instance.get_transport.return_value = mock_transport
    mock_ssh_instance.open_sftp.return_value = MagicMock()
    mock_ssh.return_value = mock_ssh_instance
    sftp = Sftp()
    # Patch paramiko.RSAKey to avoid real key parsing
    with pytest.raises(AuthenticationError):
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


@patch("paramiko.SSHClient")
def test_connect_general_exception(mock_ssh):
    """Verify connect raises ConnectionError on general exception (network, DNS, etc)."""
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
            host_key_fingerprint="ZHVtbXktYnl0ZXM=",
            pkey=None,
            host_key_validation=True,
            timeout=None,
            policy=None,
        )
    assert "network fail" in str(excinfo.value)


@patch("paramiko.SSHClient")
def test_connect_with_pkey(mock_ssh):
    """Verify connect works with a private key (pkey) provided."""
    mock_ssh_instance = MagicMock()
    mock_transport = MagicMock()
    mock_server_key = MagicMock()
    mock_server_key.get_fingerprint.return_value = b"dummy-bytes"
    mock_transport.get_remote_server_key.return_value = mock_server_key
    mock_ssh_instance.get_transport.return_value = mock_transport
    mock_ssh_instance.open_sftp.return_value = MagicMock()
    mock_ssh.return_value = mock_ssh_instance
    sftp = Sftp()
    # Patch _load_private_key to avoid real key parsing
    sftp._load_private_key = MagicMock(return_value="pkeyobj")
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
    assert result is not None


@patch("paramiko.SSHClient")
def test_connect_host_key_validation_success(mock_ssh):
    """Verify connect succeeds when host key validation passes."""
    mock_ssh_instance = MagicMock()
    mock_transport = MagicMock()
    mock_key = MagicMock()
    mock_key.get_fingerprint.return_value = b"abc"
    mock_transport.get_remote_server_key.return_value = mock_key
    mock_ssh_instance.get_transport.return_value = mock_transport
    mock_ssh_instance.open_sftp.return_value = MagicMock()
    mock_ssh.return_value = mock_ssh_instance
    sftp = Sftp()
    with patch("paramiko.RSAKey", MagicMock()) as mock_rsakey:
        mock_rsakey.return_value.get_name.return_value = "ssh-rsa"
        result = sftp.connect(
            host="host",
            port=22,
            username="user",
            password="pass",
            passphrase=None,
            host_key_fingerprint="YWJj",  # base64.b64encode(b"abc").decode()
            pkey="",
            host_key_validation=True,
            timeout=None,
            policy=None,
        )
        assert result is not None


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
    sftp.close.assert_called_once()
    assert "no fingerprint" in str(excinfo.value)


@patch("paramiko.SSHClient")
def test_connect_sets_missing_host_key_policy_when_validation(mock_ssh):
    """Verify connect sets missing host key policy when host_key_validation is True."""
    mock_ssh_instance = MagicMock()
    mock_key = MagicMock()
    mock_ssh.return_value = mock_ssh_instance
    mock_key.get_fingerprint.return_value = b"abc"
    # Mock the transport and remote_server_key chain
    mock_transport = MagicMock()
    mock_ssh_instance.get_transport.return_value = mock_transport
    mock_transport.get_remote_server_key.return_value = mock_key
    sftp = Sftp()
    sftp._ssh = mock_ssh_instance
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
    mock_ssh_instance.set_missing_host_key_policy.assert_called()


@patch("paramiko.SSHClient")
def test_connect_fingerprint_mismatch_calls_close_and_raises(mock_ssh):
    """Verify connect calls close and raises AuthenticationError on fingerprint mismatch."""
    mock_ssh_instance = MagicMock()
    mock_transport = MagicMock()
    mock_key = MagicMock()
    mock_key.get_fingerprint.return_value = b"wrong"
    mock_transport.get_remote_server_key.return_value = mock_key
    mock_ssh_instance.get_transport.return_value = mock_transport
    mock_ssh_instance.open_sftp.return_value = MagicMock()
    mock_ssh.return_value = mock_ssh_instance
    sftp = Sftp()
    sftp._ssh = mock_ssh_instance
    sftp.close = MagicMock()
    with pytest.raises(AuthenticationError):
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
    sftp.close.assert_called_once()


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
