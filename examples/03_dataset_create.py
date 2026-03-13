"""
**File**: `03_dataset_create.py`
**Region**: `examples/03_dataset_create`

Example 03: Create an Sftp dataset.
"""
import logging
from uuid import uuid4

import pandas as pd
from ds_common_logger_py_lib import Logger
from ds_protocol_sftp_py_lib.dataset.sftp import SftpDataset, SftpDatasetSettings
from ds_protocol_sftp_py_lib.linked_service.sftp import SftpLinkedService, SftpLinkedServiceSettings

Logger.configure(level=logging.DEBUG)
logger = Logger.get_logger(__name__, package=True)

def main():
    """Main function to demonstrate creating an SFTP dataset."""
    data = pd.DataFrame(
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
    )

    dataset = SftpDataset(
        id=str(uuid4()),
        name="SFTP Dataset",
        version="1.0.0",
        linked_service=SftpLinkedService(
            id=str(uuid4()),
            name="SFTP Linked Service",
            version="1.0.0",
            settings=SftpLinkedServiceSettings(
                host="",
                port=22,
                username="",
                password="",
                host_key_validation=True,
                host_key_fingerprint="",
           ),
        ),
        settings=SftpDatasetSettings(
            folder_path="test-folder",
            file_name="test.json",
        ),
    )

    dataset.input = data
    dataset.linked_service.connect()
    dataset.create()
    logger.info("Created dataset at %s/%s", dataset.settings.folder_path, dataset.settings.file_name)
    logger.info("Output:\n%s", dataset.output)

if __name__ == "__main__":
    main()
