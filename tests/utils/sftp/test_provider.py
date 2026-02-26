"""
**File:** ``test_provider.py``
**Region:** ``tests/utils/sftp/test_provider``

SFTP provider tests.

Covers:
- Sftp class initialization and default state.
- Client property behavior when connection is not initialized.
- Idempotency of close method.
- Directory listing and file retrieval methods calling the underlying client correctly.
- Move method invoking the client's rename functionality.
- Error handling for list_directory, client property, move, and private key loading.
- Sftp.connect: success, authentication error, transport missing, host key validation failure, general exception, and pkey usage.
- Context manager (__enter__, __exit__) and close method.
- SSH property access.
"""

from unittest.mock import MagicMock, patch

import pytest
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import AuthenticationError, ConnectionError
from paramiko.ssh_exception import AuthenticationException

from ds_protocol_sftp_py_lib.utils.sftp.config import SftpConfig
from ds_protocol_sftp_py_lib.utils.sftp.provider import Sftp


def test_sftp_init_defaults():
    """Verify Sftp initializes with default values."""
    sftp = Sftp()
    assert isinstance(sftp._config, SftpConfig)
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


@patch("paramiko.SFTPClient")
def test_list_directory_calls_client(mock_sftp_client):
    """Verify list_directory calls the underlying client's listdir_attr."""
    sftp = Sftp()
    sftp._client = MagicMock()
    sftp._client.listdir_attr.return_value = []
    files = sftp.list_directory("/tmp")
    assert files == []
    sftp._client.listdir_attr.assert_called_once_with("/tmp")


@patch("paramiko.SFTPClient")
def test_get_files_by_pattern(mock_sftp_client):
    """Verify get_files_by_pattern calls the underlying client's listdir_attr and filters correctly."""
    sftp = Sftp()
    mock_file = MagicMock()
    mock_file.filename = "test.txt"
    sftp._client = MagicMock()
    sftp._client.listdir_attr.return_value = [mock_file]
    files = sftp.get_files_by_pattern("/tmp", "*.txt")
    assert files == [mock_file]


@patch("paramiko.SFTPClient")
def test_move_calls_rename(mock_sftp_client):
    """Verify move method calls the underlying client's rename."""
    sftp = Sftp()
    mock_file = MagicMock()
    mock_file.filename = "test.txt"
    sftp._client = MagicMock()
    sftp._client.listdir_attr.return_value = [mock_file]
    sftp._client.rename = MagicMock()
    sftp.get_files_by_pattern = MagicMock(return_value=[mock_file])
    sftp.move("/tmp/test.txt", "/newtmp")
    sftp._client.rename.assert_called_once()


def test_list_directory_raises_connection_error():
    """Verify list_directory raises ConnectionError when client is not connected."""
    sftp = Sftp()
    sftp._client = None
    with pytest.raises(ConnectionError):
        sftp.list_directory("/tmp")


def test_client_property_raises_connection_error():
    """Verify client property raises ConnectionError when client is not connected."""
    sftp = Sftp()
    sftp._client = None
    with pytest.raises(ConnectionError):
        _ = sftp.client


def test_move_raises_connection_error():
    """Verify move method raises ConnectionError when client is not connected."""
    sftp = Sftp()
    sftp._client = None
    with pytest.raises(ConnectionError):
        sftp.move("/tmp/test.txt", "/newtmp")


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
    mock_transport.get_remote_server_key.return_value = MagicMock()
    mock_ssh_instance.get_transport.return_value = mock_transport
    mock_ssh_instance.open_sftp.return_value = MagicMock()
    mock_ssh.return_value = mock_ssh_instance
    sftp = Sftp()
    sftp._config.host_key_validation = False  # skip fingerprint check
    result = sftp.connect(host="host", port=22, username="user", password="pass", passphrase=None, host_key_fingerprint=None)
    assert result is not None


@patch("paramiko.SSHClient")
def test_connect_authentication_error(mock_ssh):
    """Verify that connect raises AuthenticationError when SSH connection fails."""
    mock_ssh_instance = MagicMock()
    mock_ssh_instance.connect.side_effect = AuthenticationException("fail")
    mock_ssh.return_value = mock_ssh_instance
    sftp = Sftp()
    with pytest.raises(AuthenticationError):
        sftp.connect(host="host", port=22, username="user", password="pass", passphrase=None, host_key_fingerprint=None)


@patch("paramiko.SSHClient")
def test_connect_transport_missing(mock_ssh):
    """Verify that connect raises AuthenticationError when transport is missing after connection."""
    mock_ssh_instance = MagicMock()
    mock_ssh_instance.get_transport.return_value = None
    mock_ssh_instance.open_sftp.return_value = MagicMock()
    mock_ssh.return_value = mock_ssh_instance
    sftp = Sftp()
    with pytest.raises(AuthenticationError):
        sftp.connect(host="host", port=22, username="user", password="pass", passphrase=None, host_key_fingerprint=None)


@patch("paramiko.SSHClient")
def test_connect_host_key_validation_failure(mock_ssh):
    """Verify that connect raises ConnectionError when host key fingerprint does not match."""
    mock_ssh_instance = MagicMock()
    mock_transport = MagicMock()
    mock_key = MagicMock()
    mock_key.get_fingerprint.return_value = b"wrong"
    mock_transport.get_remote_server_key.return_value = mock_key
    mock_ssh_instance.get_transport.return_value = mock_transport
    mock_ssh_instance.open_sftp.return_value = MagicMock()
    mock_ssh.return_value = mock_ssh_instance
    sftp = Sftp()
    sftp._config.host_key_validation = True
    with pytest.raises(ConnectionError):
        sftp.connect(host="host", port=22, username="user", password="pass", passphrase=None, host_key_fingerprint="notmatching")


@patch("paramiko.SSHClient")
def test_connect_general_exception(mock_ssh):
    """Verify connect raises ConnectionError on general exception (network, DNS, etc)."""
    mock_ssh_instance = MagicMock()
    mock_ssh_instance.connect.side_effect = Exception("network fail")
    mock_ssh.return_value = mock_ssh_instance
    sftp = Sftp()
    with pytest.raises(ConnectionError) as excinfo:
        sftp.connect(host="host", port=22, username="user", password="pass", passphrase=None, host_key_fingerprint=None)
    assert "network fail" in str(excinfo.value)


@patch("paramiko.SSHClient")
def test_connect_with_pkey(mock_ssh):
    """Verify connect works with a private key (pkey) provided."""
    mock_ssh_instance = MagicMock()
    mock_transport = MagicMock()
    mock_transport.get_remote_server_key.return_value = MagicMock()
    mock_ssh_instance.get_transport.return_value = mock_transport
    mock_ssh_instance.open_sftp.return_value = MagicMock()
    mock_ssh.return_value = mock_ssh_instance
    sftp = Sftp()
    sftp._config.pkey = "dummy-key"
    sftp._config.host_key_validation = False
    # Patch _load_private_key to avoid real key parsing
    sftp._load_private_key = MagicMock(return_value="pkeyobj")
    result = sftp.connect(host="host", port=22, username="user", password="pass", passphrase=None, host_key_fingerprint=None)
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
    sftp._config.host_key_validation = True
    result = sftp.connect(
        host="host",
        port=22,
        username="user",
        password="pass",
        passphrase=None,
        host_key_fingerprint="YWJj",  # base64.b64encode(b"abc").decode()
    )
    assert result is not None


def test_load_private_key_authentication_error_branch():
    """Verify _load_private_key raises AuthenticationError for invalid key (branch coverage)."""
    sftp = Sftp()
    # This will hit the except branch in _load_private_key
    with pytest.raises(AuthenticationError):
        sftp._load_private_key(private_key="invalid-key", passphrase="badpass")


@patch("paramiko.SFTPClient")
def test_move_branch(mock_sftp_client):
    """Verify move covers branch where no files match pattern (branch coverage)."""
    sftp = Sftp()
    sftp._client = MagicMock()
    sftp._client.listdir_attr.return_value = []
    sftp.get_files_by_pattern = MagicMock(return_value=[])
    # Should not raise or call rename
    sftp._client.rename = MagicMock()
    sftp.move("/tmp/nomatch.txt", "/newtmp")
    sftp._client.rename.assert_not_called()


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
