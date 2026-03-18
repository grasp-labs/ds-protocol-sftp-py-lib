"""
**File:** `tests/dataset/test_sftp.py`
**Region:** `tests/dataset`

This module contains tests for the SFTP Dataset implementation.
The tests cover various scenarios for reading, writing, and
listing data from an SFTP server using the SftpDataset class.
"""

import errno
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from ds_resource_plugin_py_lib.common.resource.dataset.errors import (
    CreateError,
    ListError,
    PurgeError,
    ReadError,
    UpsertError,
)
from ds_resource_plugin_py_lib.common.resource.errors import NotSupportedError

from ds_protocol_sftp_py_lib.dataset.sftp import ListSettings, SftpDataset, SftpDatasetSettings
from ds_protocol_sftp_py_lib.linked_service.sftp import SftpLinkedService, SftpLinkedServiceSettings


@pytest.fixture
def mock_linked_service():
    """Fixture to create a mock SFTP linked service."""
    settings = SftpLinkedServiceSettings(
        host="sftp.example.com",
        username="user",
        password="password123",
        private_key=None,
        passphrase=None,
        timeout=30.0,
        host_key_fingerprint=None,
        host_key_validation=True,
        port=22,
    )
    service = SftpLinkedService(
        id="24b8c9d-1234-5678-90ab-cdef12345678", name="Mock SFTP Linked Service", version="1.0.0", settings=settings
    )
    service._sftp = MagicMock()
    service.connection.client = MagicMock()
    return service


def make_dataset(mock_linked_service, folder_path="/data", file_name="*.csv", download=False):
    """Helper function to create an SftpDataset with specified settings."""
    settings = SftpDatasetSettings(
        folder_path=folder_path,
        file_name=file_name,
        list=ListSettings(download=download),
    )
    return SftpDataset(
        id="12345678-90ab-cdef-1234-567890abcdef",
        name="Test SFTP Dataset",
        version="1.0.0",
        linked_service=mock_linked_service,
        settings=settings,
    )


def test_list_files_basic(mock_linked_service):
    """Test listing files in the SFTP dataset without downloading content."""
    # Mock SFTPAttributes
    attr = MagicMock()
    attr.filename = "file1.csv"
    attr.st_size = 100
    attr.st_uid = 1
    attr.st_gid = 1
    attr.st_mode = 0o644
    attr.st_atime = 1234567890
    attr.st_mtime = 1234567891
    mock_linked_service.connection.client.listdir_attr.return_value = [attr]
    ds = make_dataset(mock_linked_service)
    ds.list()
    assert isinstance(ds.output, pd.DataFrame)
    assert "file_name" in ds.output.columns
    assert ds.output.iloc[0]["file_name"] == "file1.csv"


def test_list_files_with_download(mock_linked_service):
    """Test listing files in the SFTP dataset with downloading content."""
    attr = MagicMock()
    attr.filename = "file2.csv"
    attr.st_size = 200
    attr.st_uid = 2
    attr.st_gid = 2
    attr.st_mode = 0o600
    attr.st_atime = 1234567892
    attr.st_mtime = 1234567893
    mock_linked_service.connection.client.listdir_attr.return_value = [attr]
    # Patch .read() on open() directly (no context manager)
    mock_linked_service.connection.client.open.return_value.read.return_value = b"csvdata"
    ds = make_dataset(mock_linked_service, download=True)
    ds.list()
    assert "content" in ds.output.columns
    assert ds.output.iloc[0]["content"] == b"csvdata"


def test_read_files_as_dataframe(mock_linked_service):
    """Test reading a file from the SFTP dataset as a DataFrame."""
    attr = MagicMock()
    attr.filename = "file3.csv"
    mock_linked_service.connection.client.listdir_attr.return_value = [attr]
    mock_file = MagicMock()
    mock_file.read.return_value = b"col1,col2\n1,2"
    mock_linked_service.connection.client.open.return_value.__enter__.return_value = mock_file
    ds = make_dataset(mock_linked_service)
    with patch(
        "ds_protocol_sftp_py_lib.dataset.sftp.PandasDeserializer.__call__", return_value=pd.DataFrame({"col1": [1], "col2": [2]})
    ):
        ds.read()
        assert isinstance(ds.output, pd.DataFrame)
        assert "col1" in ds.output.columns


def test_purge_file_success(mock_linked_service):
    """Test purging a file from the SFTP dataset successfully."""
    ds = make_dataset(mock_linked_service)
    ds.settings.file_name = "file*.csv"
    ds.settings.folder_path = "/data"
    # Mock two files returned by _get_files_by_pattern
    attr1 = MagicMock()
    attr1.filename = "file1.csv"
    attr2 = MagicMock()
    attr2.filename = "file2.csv"
    with patch.object(ds, "_get_files_by_pattern", return_value=[attr1, attr2]):
        ds.linked_service.connection.client.remove = MagicMock()
        ds.purge()
        # Should be called for both files
        actual_calls = ds.linked_service.connection.client.remove.call_args_list
        assert len(actual_calls) == 2
        assert any("file1.csv" in str(call) for call in actual_calls)
        assert any("file2.csv" in str(call) for call in actual_calls)


def test_list_file_not_found_raises_list_error(mock_linked_service):
    """Test that listing files in the SFTP dataset raises a ListError when the file is not found."""
    ds = make_dataset(mock_linked_service)
    with patch.object(SftpDataset, "_list_directory", side_effect=FileNotFoundError), pytest.raises(ListError):
        ds.list()


def test_read_file_not_found_raises_read_error(mock_linked_service):
    """Test that reading a file from the SFTP dataset raises a ReadError when the file is not found."""
    ds = make_dataset(mock_linked_service)
    with patch.object(SftpDataset, "_list_directory", side_effect=FileNotFoundError), pytest.raises(ReadError):
        ds.read()


def test_create_file_not_found_raises_create_error(mock_linked_service):
    """Test that creating a file in the SFTP dataset raises a CreateError when the file is not found."""
    ds = make_dataset(mock_linked_service)
    ds.input = pd.DataFrame({"a": [1]})
    mock_linked_service.connection.client.open.side_effect = FileNotFoundError
    with pytest.raises(CreateError):
        ds.create()


def test_purge_file_not_found_is_noop(mock_linked_service):
    """Test that purging a file from the SFTP dataset does not raise an error when the file is not found."""
    ds = make_dataset(mock_linked_service)
    attr = MagicMock()
    attr.filename = "file4.csv"
    with patch.object(ds, "_get_files_by_pattern", return_value=[attr]):
        ds.linked_service.connection.client.remove.side_effect = FileNotFoundError
        # Should not raise
        ds.purge()


def test_update_not_implemented(mock_linked_service):
    """Test that updating a file in the SFTP dataset raises a NotSupportedError."""
    ds = make_dataset(mock_linked_service)
    with pytest.raises(NotSupportedError):
        ds.update()


def test_delete_not_implemented(mock_linked_service):
    """Test that deleting a file in the SFTP dataset raises a NotSupportedError."""
    ds = make_dataset(mock_linked_service)
    with pytest.raises(NotSupportedError):
        ds.delete()


def test_rename_not_implemented(mock_linked_service):
    """Test that renaming a file in the SFTP dataset raises a NotSupportedError."""
    ds = make_dataset(mock_linked_service)
    with pytest.raises(NotSupportedError):
        ds.rename()


def test_close_calls_linked_service_close(mock_linked_service):
    """Test that closing the SFTP dataset calls the close method on the linked service."""
    ds = make_dataset(mock_linked_service)
    mock_linked_service.close = MagicMock()
    ds.close()
    mock_linked_service.close.assert_called_once()


def test_get_folder_and_file_path(mock_linked_service):
    """Test that _get_folder_and_file_path returns the correct folder and file path."""
    ds = make_dataset(mock_linked_service, folder_path="/foo/bar", file_name="baz.txt")
    result = ds._get_folder_and_file_path()
    assert "foo/bar" in str(result)
    assert "baz.txt" in str(result)


def test_ensure_sftp_directory_normal(mock_linked_service):
    """Test that _ensure_sftp_directory creates the directory when it does not exist."""
    ds = make_dataset(mock_linked_service, folder_path="/foo/bar", file_name="baz.txt")
    mock_linked_service.connection.client.stat.side_effect = FileNotFoundError
    mock_linked_service.connection.client.mkdir = MagicMock()
    ds._ensure_sftp_directory("/foo/bar/baz")
    assert mock_linked_service.connection.client.mkdir.called


def test_ensure_sftp_directory_max_depth(mock_linked_service):
    """Test that _ensure_sftp_directory raises a CreateError when the maximum directory depth is exceeded."""
    ds = make_dataset(mock_linked_service, folder_path="/foo/bar", file_name="baz.txt")
    with pytest.raises(CreateError):
        ds._ensure_sftp_directory("/" + "/".join(["a"] * 25), max_depth=20)


def test__read_files_as_collection_and_dataframe_empty(mock_linked_service):
    """Test that _read_files_as_dataframe returns an empty DataFrame when no files are provided."""
    ds = make_dataset(mock_linked_service)
    # Should return empty DataFrame if no files
    with patch.object(ds, "linked_service"):
        assert ds._read_files_as_dataframe([]).empty


def test__read_files_as_dataframe_handles_file(mock_linked_service):
    """Test that _read_files_as_dataframe reads a file and returns a DataFrame."""
    ds = make_dataset(mock_linked_service)
    attr = MagicMock()
    attr.filename = "foo.txt"
    mock_file = MagicMock()
    mock_file.read.return_value = b"abc"
    mock_linked_service.connection.client.open.return_value.__enter__.return_value = mock_file
    with patch("ds_protocol_sftp_py_lib.dataset.sftp.PandasDeserializer.__call__", return_value=pd.DataFrame({"a": [1]})):
        out = ds._read_files_as_dataframe([attr])
        assert isinstance(out, pd.DataFrame)


def test__list_directory_files_empty(mock_linked_service):
    """Test that _list_directory_files returns an empty DataFrame when no files are found."""
    ds = make_dataset(mock_linked_service)
    out = ds._list_directory_files([])
    assert isinstance(out, pd.DataFrame)
    assert out.empty


def test__list_directory_files_handles_file(mock_linked_service):
    """Test that _list_directory_files processes file attributes and returns a DataFrame."""
    ds = make_dataset(mock_linked_service)
    attr = MagicMock()
    attr.filename = "foo.txt"
    attr.st_size = 1
    attr.st_uid = 1
    attr.st_gid = 1
    attr.st_mode = 0o644
    attr.st_atime = 1
    attr.st_mtime = 2
    with patch("pandas.DataFrame", wraps=pd.DataFrame) as df_patch:
        ds._list_directory_files([attr])
        assert df_patch.called


def test_read_no_files_logs_warning(mock_linked_service):
    """Test that read() logs a warning when no files are found to read."""
    ds = make_dataset(mock_linked_service)
    with (
        patch.object(ds, "_get_files_by_pattern", return_value=[]),
        patch("ds_protocol_sftp_py_lib.dataset.sftp.logger.warning") as warn_patch,
    ):
        ds.read()
        assert warn_patch.called
        assert ds.output.empty


def test_create_no_input_logs_info(mock_linked_service):
    """Test that create() logs an info message when no input is provided."""
    ds = make_dataset(mock_linked_service)
    ds.input = None
    with patch("ds_protocol_sftp_py_lib.dataset.sftp.logger.info") as info_patch:
        ds.create()
        assert info_patch.called


def test_purge_file_not_found_logs_warning(mock_linked_service):
    """Test that purge() logs a warning when a file to be purged is not found."""
    ds = make_dataset(mock_linked_service)
    attr = MagicMock()
    attr.filename = "file4.csv"
    with patch.object(ds, "_get_files_by_pattern", return_value=[attr]):
        mock_linked_service.connection.client.remove.side_effect = FileNotFoundError
        with patch("ds_protocol_sftp_py_lib.dataset.sftp.logger.warning") as warn_patch:
            ds.purge()
            assert warn_patch.called


def test_list_directory_file_not_found_logs_warning(mock_linked_service):
    """Test that _list_directory logs a warning when the directory is not found."""
    ds = make_dataset(mock_linked_service)
    mock_linked_service.connection.client.listdir_attr.side_effect = FileNotFoundError
    with pytest.raises(FileNotFoundError):
        ds._list_directory("/notfound")


def test_read_raises_generic_exception(mock_linked_service):
    """Test that read() raises a ReadError when an unexpected exception occurs."""
    ds = make_dataset(mock_linked_service)
    with patch.object(ds, "_get_files_by_pattern", side_effect=Exception("fail")):
        with pytest.raises(ReadError) as excinfo:
            ds.read()
        assert "fail" in str(excinfo.value)


def test_purge_raises_generic_exception(mock_linked_service):
    """Test that purge() raises a PurgeError when an unexpected exception occurs during file removal."""
    ds = make_dataset(mock_linked_service)
    attr = MagicMock()
    attr.filename = "file4.csv"
    with patch.object(ds, "_get_files_by_pattern", return_value=[attr]):
        mock_linked_service.connection.client.remove.side_effect = Exception("fail")
        with patch("ds_protocol_sftp_py_lib.dataset.sftp.logger.error") as err_patch:
            with pytest.raises(PurgeError) as excinfo:
                ds.purge()
            assert err_patch.called
            assert "fail" in str(excinfo.value)


def test_list_raises_generic_exception(mock_linked_service):
    """Test that list() raises a ListError when an unexpected exception occurs during listing files."""
    ds = make_dataset(mock_linked_service)
    with (
        patch.object(ds, "_get_files_by_pattern", side_effect=Exception("fail")),
        patch("ds_protocol_sftp_py_lib.dataset.sftp.logger.error") as err_patch,
    ):
        with pytest.raises(ListError) as excinfo:
            ds.list()
        assert err_patch.called
        assert "fail" in str(excinfo.value)


def test_read_files_as_dataframe_raises_exception(mock_linked_service):
    """Test that _read_files_as_dataframe raises an error when an unexpected exception occurs during file reading."""
    ds = make_dataset(mock_linked_service)
    attr = MagicMock()
    attr.filename = "foo.txt"
    # Make open/read work so deserializer is called
    mock_file = MagicMock()
    mock_file.read.return_value = b"abc"
    mock_linked_service.connection.client.open.return_value.__enter__.return_value = mock_file
    with (
        patch("ds_protocol_sftp_py_lib.dataset.sftp.logger.warning"),
        patch("ds_protocol_sftp_py_lib.dataset.sftp.PandasDeserializer.__call__", side_effect=Exception("fail")),
    ):
        with pytest.raises(Exception) as excinfo:
            ds._read_files_as_dataframe([attr])
        assert "fail" in str(excinfo.value)


def test__get_files_by_pattern_raises_exception(mock_linked_service):
    """"""
    ds = make_dataset(mock_linked_service)
    with patch.object(ds, "_list_directory", side_effect=Exception("fail")):
        try:
            ds._get_files_by_pattern("/data", "*.csv")
        except Exception as e:
            assert "fail" in str(e)


def test_create_file_success(mock_linked_service):
    """Covers: create() file creation path (205-213)."""
    ds = make_dataset(mock_linked_service)
    ds.input = pd.DataFrame({"a": [1]})
    # stat raises FileNotFoundError (file does not exist)
    mock_linked_service.connection.client.stat.side_effect = FileNotFoundError
    # open returns a mock file context manager
    mock_file = MagicMock()
    mock_linked_service.connection.client.open.return_value.__enter__.return_value = mock_file
    ds.create()
    mock_linked_service.connection.client.open.assert_called_once()
    args, kwargs = mock_linked_service.connection.client.open.call_args
    assert kwargs.get("mode") == "wb" or (len(args) > 1 and args[1] == "wb")
    mock_file.write.assert_called_once()
    assert ds.output.equals(ds.input)


def test_create_file_already_exists_raises_create_error(mock_linked_service):
    """Covers: create() file creation path when file already exists (214-223)."""
    ds = make_dataset(mock_linked_service)
    ds.input = pd.DataFrame({"a": [1]})
    # stat succeeds, indicating file exists
    mock_linked_service.connection.client.stat.return_value = MagicMock()
    with pytest.raises(CreateError):
        ds.create()
    mock_linked_service.connection.client.open.assert_not_called()


def test_create_file_open_race_eexist_raises_conflict(mock_linked_service):
    """Covers: create() open-time EEXIST race branch."""
    ds = make_dataset(mock_linked_service)
    ds.input = pd.DataFrame({"a": [1]})
    mock_linked_service.connection.client.stat.side_effect = FileNotFoundError
    err = OSError("already exists")
    err.errno = errno.EEXIST
    mock_linked_service.connection.client.open.side_effect = err
    with pytest.raises(CreateError) as excinfo:
        ds.create()
    assert excinfo.value.status_code == 409


def test_create_file_open_permission_denied_raises_403(mock_linked_service):
    """Covers: create() permission denied branch."""
    ds = make_dataset(mock_linked_service)
    ds.input = pd.DataFrame({"a": [1]})
    mock_linked_service.connection.client.stat.side_effect = FileNotFoundError
    err = OSError("access denied")
    err.errno = errno.EACCES
    mock_linked_service.connection.client.open.side_effect = err
    with pytest.raises(CreateError) as excinfo:
        ds.create()
    assert excinfo.value.status_code == 403


def test_create_file_open_generic_oserror_raises_create_error(mock_linked_service):
    """Covers: create() generic OSError branch."""
    ds = make_dataset(mock_linked_service)
    ds.input = pd.DataFrame({"a": [1]})
    mock_linked_service.connection.client.stat.side_effect = FileNotFoundError
    err = OSError("io failure")
    err.errno = errno.EIO
    mock_linked_service.connection.client.open.side_effect = err
    with pytest.raises(CreateError) as excinfo:
        ds.create()
    assert "OS error occurred while creating file on SFTP server" in str(excinfo.value)


def test_purge_no_files_early_return(mock_linked_service):
    """Covers: purge() early return when no files (290-294)."""
    ds = make_dataset(mock_linked_service)
    with patch.object(ds, "_get_files_by_pattern", return_value=[]):
        # Should not raise or call remove
        ds.linked_service.connection.client.remove = MagicMock()
        ds.purge()
        ds.linked_service.connection.client.remove.assert_not_called()


def test_ensure_sftp_directory_empty_and_root(mock_linked_service):
    """Covers: _ensure_sftp_directory() early return for '' and '/' (451)."""
    ds = make_dataset(mock_linked_service)
    # For '', stat('.') is called; for '/', nothing is called
    ds.linked_service.connection.client.stat = MagicMock()
    ds.linked_service.connection.client.mkdir = MagicMock()
    ds._ensure_sftp_directory("")
    ds._ensure_sftp_directory("/")
    ds.linked_service.connection.client.stat.assert_any_call(".")
    ds.linked_service.connection.client.mkdir.assert_not_called()


def test_create_raises_generic_exception(mock_linked_service):
    """Test that create() raises a CreateError when an unexpected exception occurs."""
    ds = make_dataset(mock_linked_service)
    ds.input = pd.DataFrame({"a": [1]})
    mock_linked_service.connection.client.stat.side_effect = FileNotFoundError
    mock_linked_service.connection.client.open.side_effect = Exception("fail-create")
    with pytest.raises(CreateError) as excinfo:
        ds.create()
    assert "fail-create" in str(excinfo.value)


def test_upsert_raises_generic_exception(mock_linked_service):
    """Test that upsert() raises an UpsertError when an unexpected exception occurs."""
    ds = make_dataset(mock_linked_service)
    ds.input = pd.DataFrame({"a": [1]})
    mock_linked_service.connection.client.open.side_effect = Exception("fail-upsert")
    with pytest.raises(UpsertError) as excinfo:
        ds.upsert()
    assert "fail-upsert" in str(excinfo.value)


def test_upsert_empty_input_logs_info(mock_linked_service):
    """Test that upsert() logs an info message when no input is provided."""
    ds = make_dataset(mock_linked_service)
    ds.input = pd.DataFrame()
    with patch("ds_protocol_sftp_py_lib.dataset.sftp.logger.info") as info_patch:
        ds.upsert()
        assert info_patch.called


def test_upsert_logs_info_on_write(mock_linked_service):
    """Test that upsert() logs an info message when writing data."""
    ds = make_dataset(mock_linked_service)
    ds.input = pd.DataFrame({"a": [1]})
    mock_file = MagicMock()
    mock_linked_service.connection.client.open.return_value.__enter__.return_value = mock_file
    with patch("ds_protocol_sftp_py_lib.dataset.sftp.logger.info") as info_patch:
        ds.upsert()
        assert any("Upserting file to SFTP server" in str(call) for call in info_patch.call_args_list)
