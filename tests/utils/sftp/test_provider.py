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
"""

from unittest.mock import MagicMock, patch

import pytest
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import AuthenticationError, ConnectionError

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
    sftp = Sftp()
    sftp._client = None
    with pytest.raises(ConnectionError):
        sftp.list_directory("/tmp")


def test_client_property_raises_connection_error():
    sftp = Sftp()
    sftp._client = None
    with pytest.raises(ConnectionError):
        _ = sftp.client


def test_move_raises_connection_error():
    sftp = Sftp()
    sftp._client = None
    with pytest.raises(ConnectionError):
        sftp.move("/tmp/test.txt", "/newtmp")


def test_load_private_key_raises_authentication_error():
    sftp = Sftp()
    # Invalid key string
    invalid_key = "not-a-valid-key"
    with pytest.raises(AuthenticationError):
        sftp._load_private_key(private_key=invalid_key, passphrase=None)
