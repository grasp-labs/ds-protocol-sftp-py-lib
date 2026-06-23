"""
**File**: `07_dataset_rename.py`
**Region**: `examples/07_dataset_rename`

Example 07: Rename an Sftp dataset.
"""

import logging
from uuid import uuid4

import pandas as pd
from ds_common_logger_py_lib import Logger
from ds_protocol_sftp_py_lib.dataset.sftp import SftpDataset, SftpDatasetSettings, RenameSettings
from ds_protocol_sftp_py_lib.linked_service.sftp import SftpLinkedService, SftpLinkedServiceSettings

Logger.configure(level=logging.DEBUG)
logger = Logger.get_logger(__name__)


def main():
    """Main function to demonstrate renaming an SFTP dataset."""
    data = pd.DataFrame(
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
    )

    dataset = SftpDataset(
        id=uuid4(),
        name="SFTP Dataset",
        version="1.0.0",
        linked_service=SftpLinkedService(
            id=uuid4(),
            name="SFTP Linked Service",
            version="1.0.0",
            settings=SftpLinkedServiceSettings(
                host="",
                port=22,
                username="",
                password="",
                host_key_validation=False,
                host_key_fingerprint="",
            ),
        ),
        settings=SftpDatasetSettings(
            folder_path="integration",
            file_name="test.json",
            rename=RenameSettings(
                new_folder_path="integration/success",
                new_file_name="test.json",
                overwrite=False
            ),
        ),
    )

    dataset.input = data
    dataset.linked_service.connect()
    dataset.rename()
    logger.info("Renamed dataset from %s/%s to %s/%s", dataset.settings.folder_path, dataset.settings.file_name, dataset.settings.rename.new_folder_path, dataset.settings.rename.new_file_name)
    logger.info("Output:\n%s", dataset.output)


if __name__ == "__main__":
    main()
