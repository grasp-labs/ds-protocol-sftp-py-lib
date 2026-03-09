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
    # Provide a dummy fingerprint to avoid ConnectionError in Sftp.connect
    return SftpLinkedServiceSettings(
        host="localhost",
        username="user",
        password="pass",
        encrypted_credential="enc",
        private_key=None,
        passphrase=None,
        timeout=1.0,
        host_key_fingerprint="dummy-fingerprint",
        host_key_validation=True,
        port=22,
        policy=None,
    )


@patch("ds_protocol_sftp_py_lib.linked_service.sftp.Sftp")
def test_connect_sets_connection(mock_sftp):
    """Verify that connect() opens a connection and sets the connection property."""
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
    assert svc.connection is mock_instance


@patch("ds_protocol_sftp_py_lib.linked_service.sftp.Sftp")
def test_connection_property_raises_when_none(mock_sftp):
    """Verify that accessing the connection property raises ConnectionError."""
    svc = SftpLinkedService(
        id="test_id",
        name="test_name",
        version="1.0.0",
        settings=make_settings(),
    )
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
    assert svc.connection is mock_instance
    result = svc.test_connection()
    assert result == (True, "Connection successfully tested")


@patch("ds_protocol_sftp_py_lib.linked_service.sftp.Sftp")
def test_test_connection_failure(mock_sftp):
    """Verify test_connection returns failure when getcwd returns None."""
    mock_instance = MagicMock()
    mock_instance.client.getcwd.return_value = None
    mock_sftp.return_value = mock_instance
    svc = SftpLinkedService(
        id="test_id",
        name="test_name",
        version="1.0.0",
        settings=make_settings(),
    )
    svc._sftp = mock_instance
    assert svc.connection is mock_instance
    result = svc.test_connection()
    assert result == (False, "Could not get current working directory")


@patch("ds_protocol_sftp_py_lib.linked_service.sftp.Sftp")
def test_test_connection_exception(mock_sftp):
    """Verify test_connection returns failure when getcwd raises an exception."""
    mock_instance = MagicMock()
    mock_instance.client.getcwd.side_effect = Exception("fail")
    mock_sftp.return_value = mock_instance
    svc = SftpLinkedService(
        id="test_id",
        name="test_name",
        version="1.0.0",
        settings=make_settings(),
    )
    svc._sftp = mock_instance
    assert svc.connection is mock_instance
    result = svc.test_connection()
    assert result[0] is False
    assert "fail" in result[1]


@patch("ds_protocol_sftp_py_lib.linked_service.sftp.Sftp")
def test_connect_when_already_connected(mock_sftp):
    """Covers: connect() when already connected (should close and reconnect)."""
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
    svc._sftp = mock_instance
    svc.connect()
    mock_instance.connect.assert_called_once()
    assert svc._sftp is mock_instance


@patch("ds_protocol_sftp_py_lib.linked_service.sftp.Sftp")
def test_connect_inits_sftp_when_none(mock_sftp):
    """Covers: connect() when _sftp is None (should call _init_sftp)."""
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
    svc._sftp = None
    with patch.object(svc, "_init_sftp", return_value=mock_instance) as mock_init:
        svc.connect()
        mock_init.assert_called_once()
    assert svc._sftp is mock_instance


@patch("ds_protocol_sftp_py_lib.linked_service.sftp.Sftp")
def test_test_connection_calls_connect_when_sftp_none(mock_sftp):
    """Covers: test_connection() when _sftp is None (should call connect)."""
    mock_instance = MagicMock()
    mock_client = MagicMock()
    mock_client.getcwd.return_value = "/home"
    mock_instance.connect.return_value = mock_client
    mock_sftp.return_value = mock_instance
    svc = SftpLinkedService(
        id="test_id",
        name="test_name",
        version="1.0.0",
        settings=make_settings(),
    )
    svc._sftp = None
    with patch.object(svc, "connect") as mock_connect:

        def after_connect():
            svc._sftp = mock_instance

        mock_connect.side_effect = after_connect
        result = svc.test_connection()
        mock_connect.assert_called_once()
        assert result == (True, "Connection successfully tested")


@patch("ds_protocol_sftp_py_lib.linked_service.sftp.Sftp")
def test_close_calls_sftp_close_and_sets_none(mock_sftp):
    """Verify that close() calls close on the Sftp instance and sets _sftp to None."""
    mock_instance = MagicMock()
    mock_sftp.return_value = mock_instance
    svc = SftpLinkedService(
        id="test_id",
        name="test_name",
        version="1.0.0",
        settings=make_settings(),
    )
    svc._sftp = mock_instance
    svc.close()
    mock_instance.close.assert_called_once()
    assert svc._sftp is None
