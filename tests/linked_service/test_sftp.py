"""
**File:** ``test_sftp.py``
**Region:** ``tests/linked_service/test_sftp``

Linked service tests for SFTP protocol implementation.

Covers:
- Unit tests for the SftpLinkedService class, including connection management,
  test_connection behavior, and close method functionality.
- Tests for connection property behavior and error handling in various scenarios.
"""

from unittest.mock import MagicMock, patch

import pytest
from ds_resource_plugin_py_lib.common.resource.linked_service.errors import ConnectionError

from ds_protocol_sftp_py_lib.linked_service.sftp import SftpLinkedService, SftpLinkedServiceSettings


def make_settings():
    """Helper function to create default SftpLinkedServiceSettings for testing."""
    return SftpLinkedServiceSettings(
        host="localhost",
        username="user",
        password="pass",
        encrypted_credential="enc",
        private_key=None,
        passphrase=None,
        timeout=1.0,
        host_key_fingerprint=None,
        host_key_validation=True,
        port=22,
    )


@patch("ds_protocol_sftp_py_lib.linked_service.sftp.Sftp")
def test_connect_sets_connection(mock_sftp):
    """Verify that connect method sets the _connection attribute from the provider's connect return value."""
    mock_instance = MagicMock()
    mock_client = MagicMock()
    mock_instance.connect.return_value = mock_client
    mock_sftp.return_value = mock_instance
    svc = SftpLinkedService(
        id="test_id",
        name="test_name",
        version="1.0.0",
        settings=make_settings(),
    )
    svc.connect()
    assert svc._connection is mock_client


@patch("ds_protocol_sftp_py_lib.linked_service.sftp.Sftp")
def test_connection_property_raises_when_none(mock_sftp):
    """Verify that accessing the connection property raises ConnectionError when _connection is None."""
    svc = SftpLinkedService(
        id="test_id",
        name="test_name",
        version="1.0.0",
        settings=make_settings(),
    )
    svc._connection = None
    with pytest.raises(ConnectionError):
        _ = svc.connection


@patch("ds_protocol_sftp_py_lib.linked_service.sftp.Sftp")
def test_test_connection_success(mock_sftp):
    """Verify test_connection returns success when getcwd works."""
    mock_instance = MagicMock()
    mock_instance._client = MagicMock()
    mock_instance._client.getcwd.return_value = "/home"
    mock_sftp.return_value = mock_instance
    svc = SftpLinkedService(
        id="test_id",
        name="test_name",
        version="1.0.0",
        settings=make_settings(),
    )
    svc._sftp = mock_instance
    svc._connection = mock_instance._client
    result = svc.test_connection()
    assert result == (True, "Connection successfully tested")


@patch("ds_protocol_sftp_py_lib.linked_service.sftp.Sftp")
def test_test_connection_failure(mock_sftp):
    """Verify test_connection returns failure when getcwd returns None."""
    mock_instance = MagicMock()
    mock_instance._client = MagicMock()
    mock_instance._client.getcwd.return_value = None
    mock_sftp.return_value = mock_instance
    svc = SftpLinkedService(
        id="test_id",
        name="test_name",
        version="1.0.0",
        settings=make_settings(),
    )
    svc._sftp = mock_instance
    svc._connection = mock_instance._client
    result = svc.test_connection()
    assert result == (False, "Could not get current working directory")


@patch("ds_protocol_sftp_py_lib.linked_service.sftp.Sftp")
def test_test_connection_exception(mock_sftp):
    """Verify test_connection returns failure when getcwd raises an exception."""
    mock_instance = MagicMock()
    mock_instance._client = MagicMock()
    mock_instance._client.getcwd.side_effect = Exception("fail")
    mock_sftp.return_value = mock_instance
    svc = SftpLinkedService(
        id="test_id",
        name="test_name",
        version="1.0.0",
        settings=make_settings(),
    )
    svc._sftp = mock_instance
    svc._connection = mock_instance._client
    result = svc.test_connection()
    assert result[0] is False
    assert "fail" in result[1]


@patch("ds_protocol_sftp_py_lib.linked_service.sftp.Sftp")
def test_close_idempotent(mock_sftp):
    """Verify that close method is idempotent and does not raise when called multiple times."""
    mock_instance = MagicMock()
    mock_instance._client = MagicMock()
    mock_instance.close = MagicMock()
    mock_instance._client.close = MagicMock()
    svc = SftpLinkedService(
        id="test_id",
        name="test_name",
        version="1.0.0",
        settings=make_settings(),
    )
    svc._sftp = mock_instance
    svc._connection = mock_instance._client
    svc.close()
    svc.close()  # Should not raise
    mock_instance.close.assert_called()
    mock_instance._client.close.assert_called()


@patch("ds_protocol_sftp_py_lib.linked_service.sftp.Sftp")
def test_close_raises_connection_error_on_exception(mock_sftp):
    """Verify that close method raises ConnectionError when an exception occurs."""
    mock_instance = MagicMock()
    mock_instance.close.side_effect = Exception("fail")
    svc = SftpLinkedService(
        id="test_id",
        name="test_name",
        version="1.0.0",
        settings=make_settings(),
    )
    svc._sftp = mock_instance
    svc._connection = MagicMock()
    with pytest.raises(ConnectionError):
        svc.close()


def test_close_sftp_exception_sets_none():
    mock_instance = MagicMock()
    mock_instance.close.side_effect = Exception("fail")
    svc = SftpLinkedService(
        id="test_id",
        name="test_name",
        version="1.0.0",
        settings=make_settings(),
    )
    svc._sftp = mock_instance
    svc._connection = MagicMock()
    # Should raise ConnectionError and set _sftp to None
    with pytest.raises(ConnectionError):
        svc.close()
    assert svc._sftp is None


def test_close_connection_exception_sets_none():
    mock_instance = MagicMock()
    mock_instance.close = MagicMock()
    mock_connection = MagicMock()
    mock_connection.close.side_effect = Exception("fail")
    svc = SftpLinkedService(
        id="test_id",
        name="test_name",
        version="1.0.0",
        settings=make_settings(),
    )
    svc._sftp = mock_instance
    svc._connection = mock_connection
    # Should raise ConnectionError and set _connection to None
    with pytest.raises(ConnectionError):
        svc.close()
    assert svc._connection is None
